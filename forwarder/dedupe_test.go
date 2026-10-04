package forwarder_test

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/forwarder"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// fakeClock 是可手动推进的时钟，让 TTL/淘汰测试完全确定、无需 sleep。
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock               { return &fakeClock{t: time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)} }
func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func okOutcome(id string, status int) forwarder.CachedOutcome {
	return forwarder.CachedOutcome{
		InstanceID: id, StatusCode: status, Header: http.Header{"X": []string{"y"}},
		Attempts: []model.Attempt{{InstanceID: id, StatusCode: status}},
	}
}

func failOutcome(kind model.ErrorKind) forwarder.CachedOutcome {
	return forwarder.CachedOutcome{Err: model.NewError(kind, "ins-x", "注入失败")}
}

// 用例 1：TTL 窗口内复用；过期后当作全新请求；Sweep 只删过期结论。
func TestDedupeTTLExpiryMakesKeyFresh(t *testing.T) {
	clk := newFakeClock()
	st := forwarder.NewDedupeStore(forwarder.DedupeConfig{
		TTL: time.Minute, MaxEntries: 10, WaitBudget: time.Second, Now: clk.now,
	})

	if leader, _ := st.Acquire("k"); !leader {
		t.Fatal("首次必须是 leader")
	}
	st.Publish("k", okOutcome("a", 200))

	// 窗口内：并发/后来的同键调用都是 follower 且命中结论。
	if leader, _ := st.Acquire("k"); leader {
		t.Fatal("TTL 窗口内同键不应再次成为 leader")
	}
	if o, ok := st.Lookup("k"); !ok || o.InstanceID != "a" || o.Failed() {
		t.Fatalf("窗口内应命中成功结论, ok=%v o=%+v", ok, o)
	}

	// 另一个键在 30s 时发布。
	clk.advance(30 * time.Second)
	st.Publish("k2", okOutcome("b", 201))

	// 到 61s：k 过期，k2 仍有效。
	clk.advance(31 * time.Second)
	if swept := st.Sweep(); swept != 1 {
		t.Fatalf("Sweep 应只清理 k 一条, got %d", swept)
	}
	if _, ok := st.Lookup("k2"); !ok {
		t.Fatal("未过期的 k2 必须保留")
	}

	// k 过期后再来：当作全新请求，正常成为 leader，不报错、不卡住。
	if leader, _ := st.Acquire("k"); !leader {
		t.Fatal("过期结论清理后同键请求必须成为全新一次请求的 leader")
	}
	t.Logf("[dedupe-store] TTL：窗口内复用 -> 过期 Sweep 删除 1 条（k2 保留）-> 同键再来成为全新 leader")
}

// 用例 2：容量上限超量时按最旧发布顺序 FIFO 淘汰，新结论保留；
// 被淘汰键再次到来时当作全新请求。
func TestDedupeCapacityFIFOEviction(t *testing.T) {
	clk := newFakeClock()
	st := forwarder.NewDedupeStore(forwarder.DedupeConfig{
		TTL: time.Hour, MaxEntries: 3, WaitBudget: time.Second, Now: clk.now,
	})
	for _, k := range []string{"a", "b", "c"} {
		if leader, _ := st.Acquire(k); !leader {
			t.Fatalf("%s 首次应为 leader", k)
		}
		st.Publish(k, okOutcome("ins-"+k, 200))
		clk.advance(time.Second)
	}
	if st.Len() != 3 {
		t.Fatalf("容量内应保留 3 条, got %d", st.Len())
	}
	// 发布第 4 条 -> 最旧的 a 被淘汰。
	st.Publish("d", okOutcome("ins-d", 200))
	if st.Len() != 3 {
		t.Fatalf("超量后必须仍为容量上限 3, got %d", st.Len())
	}
	if _, ok := st.Lookup("a"); ok {
		t.Fatal("最旧的 a 应被 FIFO 淘汰")
	}
	for _, k := range []string{"b", "c", "d"} {
		if _, ok := st.Lookup(k); !ok {
			t.Fatalf("较新结论 %s 必须保留", k)
		}
	}
	// 被淘汰键再来：全新请求，不卡不错。
	if leader, _ := st.Acquire("a"); !leader {
		t.Fatal("被淘汰的 a 再来时必须成为全新 leader")
	}
	t.Logf("[dedupe-store] 容量=3：发布 a,b,c,d 后 a 被 FIFO 淘汰，b/c/d 保留，条数恒为 3；a 再来为全新请求")
}

// 用例 3：失败结论被一致复用且与成功可区分，绝不会被记成成功。
func TestDedupeFailureOutcomeReusedAsFailure(t *testing.T) {
	clk := newFakeClock()
	st := forwarder.NewDedupeStore(forwarder.DedupeConfig{
		TTL: time.Minute, MaxEntries: 10, WaitBudget: 20 * time.Millisecond, Now: clk.now,
	})

	cases := []struct {
		name string
		key  string
		kind model.ErrorKind
	}{
		{"连接失败", "k-connect", model.ErrConnectFailed},
		{"超时", "k-timeout", model.ErrTimeout},
		{"4xx", "k-4xx", model.ErrUpstream4xx},
		{"5xx", "k-5xx", model.ErrUpstream5xx},
		{"无健康实例", "k-no-inst", model.ErrNoHealthyInstance},
		{"重试预算耗尽", "k-budget", model.ErrNoRetryBudget},
	}
	for _, c := range cases {
		leader, _ := st.Acquire(c.key)
		if !leader {
			t.Fatalf("%s: 首次应为 leader", c.name)
		}
		st.Publish(c.key, failOutcome(c.kind))
	}
	if st.Len() != len(cases) {
		t.Fatalf("失败结论也计入保留条数, want %d got %d", len(cases), st.Len())
	}
	for _, c := range cases {
		o, ok := st.Lookup(c.key)
		if !ok {
			t.Fatalf("%s: 失败结论应在窗口内保留", c.name)
		}
		if !o.Failed() {
			t.Fatalf("%s: 失败结论绝不能表现为成功", c.name)
		}
		if model.ErrKind(o.Err) != c.kind {
			t.Fatalf("%s: 失败类别必须可区分, want %s got %s", c.name, c.kind, model.ErrKind(o.Err))
		}
		// 再次 Acquire 走 follower 路径，也只能拿到同一份失败结论。
		if leader, _ := st.Acquire(c.key); leader {
			t.Fatalf("%s: 窗口内失败结论也必须被复用，不应再执行", c.name)
		}
	}
	t.Logf("[dedupe-store] 失败结论复用：connect/timeout/4xx/5xx/no_healthy/no_retry_budget 六类均保留原类别且 Failed()=true")
}

// 用例 4：inflight 等待者在 Publish 后全部被唤醒并读到同一份结论（成功与失败各一次）。
func TestDedupeWaitersConverge(t *testing.T) {
	for _, failed := range []bool{false, true} {
		label := "成功"
		out := okOutcome("a", 200)
		if failed {
			label = "失败"
			out = failOutcome(model.ErrUpstream5xx)
		}
		st := forwarder.NewDedupeStore(forwarder.DedupeConfig{
			TTL: time.Minute, MaxEntries: 10, WaitBudget: time.Second,
		})
		leader, _ := st.Acquire("key")
		if !leader {
			t.Fatal("应有 1 个 leader")
		}
		const n = 16
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				l, wait := st.Acquire("key")
				if l {
					t.Error("等待者不应成为 leader")
					return
				}
				<-wait
				o, ok := st.Lookup("key")
				if !ok {
					t.Error("唤醒后必须能读到结论")
					return
				}
				if o.Failed() != failed {
					t.Errorf("%s结论的 Failed 标记错误: want %v", label, failed)
				}
			}()
		}
		time.Sleep(20 * time.Millisecond)
		st.Publish("key", out)
		wg.Wait()
		t.Logf("[dedupe-store] %s结论：1 leader + %d 在途等待者全部收敛到同一份结论", label, n)
	}
}
