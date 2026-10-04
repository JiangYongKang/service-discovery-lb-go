package forwarder_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/forwarder"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// 业务：同键并发失败结论收敛 —— 所有等待者拿到同一失败、后端只执行一次、失败不记成成功。
func TestDedupeConcurrentFailureReplayedWithKind(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	srv, ins := newBackend(t, "bad", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release // 慢处理：并发同键调用在此期间到达
		w.WriteHeader(http.StatusNotFound)
	})
	_ = srv

	f := forwarder.New(func(string) (model.Instance, bool) { return ins, true },
		forwarder.WithLogger(func(f string, a ...any) { t.Logf("    "+f, a...) }))
	sel := &staticSelector{instances: []model.Instance{ins}}

	const n = 10
	var wg sync.WaitGroup
	errs := make(chan model.ErrorKind, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.Execute(context.Background(), model.RequestContext{
				Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "fail-key",
			}, sel, forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: 2 * time.Second})
			errs <- model.ErrKind(err)
		}()
	}
	time.Sleep(50 * time.Millisecond) // 等 follower 全部在屏障上等待
	close(release)
	wg.Wait()
	close(errs)

	for k := range errs {
		if k != model.ErrUpstream4xx {
			t.Fatalf("同键并发失败必须一致复用 upstream_4xx, got %q", k)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("同键失败只能真正访问后端 1 次, got %d", calls.Load())
	}
	// 失败结论绝不能被 Lookup 当成成功。
	if _, _, ok := f.DedupeStore().Lookup("fail-key"); ok {
		t.Fatal("失败结论不得被 Lookup 当作成功结果返回")
	}
	// 窗口内后来的重放仍复用同一失败结论，且不再访问后端。
	_, err := f.Execute(context.Background(), model.RequestContext{
		Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "fail-key",
	}, sel, forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: time.Second})
	if model.ErrKind(err) != model.ErrUpstream4xx {
		t.Fatalf("失败重放应仍得到 upstream_4xx, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("失败重放不应再次访问后端, got %d", calls.Load())
	}
	t.Logf("业务=失败结论收敛: 10 并发同键 4xx -> 后端处理 %d 次，全部得到 upstream_4xx；失败未被记成成功；窗口内重放不再访问后端", calls.Load())
}

// 业务：不同失败类别必须可区分地被一致复用（connect/timeout/5xx/4xx）。
func TestDedupeFailureKindsDistinguishable(t *testing.T) {
	cases := []struct {
		name     string
		setup    func(t *testing.T) (model.Instance, func(string) (model.Instance, bool))
		policy   forwarder.RetryPolicy
		wantKind model.ErrorKind
	}{
		{
			name: "connect_failed",
			setup: func(t *testing.T) (model.Instance, func(string) (model.Instance, bool)) {
				ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
				ins := model.Instance{ID: "dead", Service: "svc", Addr: ts.URL}
				ts.Close()
				return ins, func(string) (model.Instance, bool) { return ins, true }
			},
			policy:   forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: time.Second},
			wantKind: model.ErrConnectFailed,
		},
		{
			name: "timeout",
			setup: func(t *testing.T) (model.Instance, func(string) (model.Instance, bool)) {
				_, ins := newBackend(t, "slow", func(http.ResponseWriter, *http.Request) {
					time.Sleep(time.Second)
				})
				return ins, func(string) (model.Instance, bool) { return ins, true }
			},
			policy:   forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: 60 * time.Millisecond},
			wantKind: model.ErrTimeout,
		},
		{
			name: "upstream_5xx",
			setup: func(t *testing.T) (model.Instance, func(string) (model.Instance, bool)) {
				_, ins := newBackend(t, "e500", func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})
				return ins, func(string) (model.Instance, bool) { return ins, true }
			},
			policy:   forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: time.Second},
			wantKind: model.ErrUpstream5xx,
		},
		{
			name: "upstream_4xx",
			setup: func(t *testing.T) (model.Instance, func(string) (model.Instance, bool)) {
				_, ins := newBackend(t, "e404", func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				})
				return ins, func(string) (model.Instance, bool) { return ins, true }
			},
			policy:   forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: time.Second},
			wantKind: model.ErrUpstream4xx,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ins, filter := tc.setup(t)
			f := forwarder.New(filter,
				forwarder.WithLogger(func(f string, a ...any) { t.Logf("    "+f, a...) }))
			sel := &staticSelector{instances: []model.Instance{ins}}
			key := "k-" + tc.name

			// 并发同键：1 leader + 3 follower，结论类别必须一致。
			const n = 4
			var wg sync.WaitGroup
			got := make(chan model.ErrorKind, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := f.Execute(context.Background(), model.RequestContext{
						Service: "svc", Method: "GET", Path: "/", IdempotencyKey: key,
					}, sel, tc.policy)
					got <- rootOrKind(err)
				}()
			}
			wg.Wait()
			close(got)
			for k := range got {
				if k != tc.wantKind {
					t.Fatalf("业务=%s 期望 %s, got %q", tc.name, tc.wantKind, k)
				}
			}
			// 重放一次仍是同一可区分失败：根类别必须仍是原来那一类。
			_, err := f.Execute(context.Background(), model.RequestContext{
				Service: "svc", Method: "GET", Path: "/", IdempotencyKey: key,
			}, sel, tc.policy)
			if rootOrKind(err) != tc.wantKind {
				t.Fatalf("业务=%s 重放根原因应仍为 %s, got %v", tc.name, tc.wantKind, err)
			}
			if tc.wantKind == model.ErrUpstream4xx && model.ErrKind(err) != model.ErrUpstream4xx {
				t.Fatalf("4xx 不可重试，最终类别不得被包装, got %v", err)
			}
			t.Logf("业务=失败类别区分: 根原因=%s 最终类别=%s 并发 4 调用与 1 次重放结论一致；依据=%s",
				tc.wantKind, model.ErrKind(err), model.DescribeKind(tc.wantKind))
		})
	}
}

// rootOrKind 返回错误根类别；无 CallError 时返回空。
func rootOrKind(err error) model.ErrorKind {
	if k := model.RootKind(err); k != "" {
		return k
	}
	return model.ErrKind(err)
}

// noCandidateSelector 起始即无健康候选。
type noCandidateSelector struct{}

func (noCandidateSelector) Pick(map[string]bool) (model.Instance, error) {
	return model.Instance{}, model.NewError(model.ErrNoHealthyInstance, "", "no candidate")
}

// 业务：没有健康实例时，同键并发请求必须在有限时间内拿到 no_healthy_instance，不能挂起。
func TestDedupeNoHealthyInstanceBoundedWait(t *testing.T) {
	f := forwarder.New(nil,
		forwarder.WithLogger(func(f string, a ...any) { t.Logf("    "+f, a...) }))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	const n = 8
	var wg sync.WaitGroup
	start := time.Now()
	done := make(chan model.ErrorKind, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.Execute(ctx, model.RequestContext{
				Service: "svc", Method: "GET", Path: "/",
				IdempotencyKey: "empty-svc",
			}, noCandidateSelector{}, forwarder.RetryPolicy{MaxAttempts: 1})
			done <- model.ErrKind(err)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(done)
	for k := range done {
		if k != model.ErrNoHealthyInstance {
			t.Fatalf("无健康实例必须收敛为 no_healthy_instance, got %q", k)
		}
	}
	if elapsed > time.Second {
		t.Fatalf("无健康实例应立即返回，实际耗时 %v（存在挂起风险）", elapsed)
	}
	// 稍后重放：失败窗口内仍快速得到同一明确失败。
	_, err := f.Execute(context.Background(), model.RequestContext{
		Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "empty-svc",
	}, noCandidateSelector{}, forwarder.RetryPolicy{MaxAttempts: 1})
	if model.ErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("无健康实例重放应仍为 no_healthy_instance, got %v", err)
	}
	t.Logf("业务=无健康实例收敛: 8 并发同键在 %v 内全部得到 no_healthy_instance；重放同样快速失败", elapsed)
}

// 业务：成功结论超过保留窗口后被淘汰，同键再来是一次全新请求。
func TestDedupeSuccessTTLExpiry(t *testing.T) {
	var calls atomic.Int64
	_, ins := newBackend(t, "ok", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	f := forwarder.New(func(string) (model.Instance, bool) { return ins, true },
		forwarder.WithDedupeConfig(forwarder.DedupeConfig{
			SuccessTTL: 80 * time.Millisecond, FailureTTL: 80 * time.Millisecond, Capacity: 16,
		}))
	sel := &staticSelector{instances: []model.Instance{ins}}
	rp := forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: time.Second}

	req := func() {
		t.Helper()
		if _, err := f.Execute(context.Background(), model.RequestContext{
			Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "ttl-ok",
		}, sel, rp); err != nil {
			t.Fatal(err)
		}
	}
	req()
	// 窗口内复用，不重新访问后端。
	_, before, ok := f.DedupeStore().Lookup("ttl-ok")
	if !ok || before != http.StatusOK {
		t.Fatalf("窗口内应能复用成功结论, ok=%v status=%d", ok, before)
	}
	req()
	if calls.Load() != 1 {
		t.Fatalf("保留窗口内应复用, got calls=%d", calls.Load())
	}
	time.Sleep(160 * time.Millisecond)
	if f.DedupeStore().Len() != 0 {
		t.Fatalf("过期后应被惰性清理, len=%d", f.DedupeStore().Len())
	}
	req() // 过期后同键再来：全新请求，不卡不报错。
	if calls.Load() != 2 {
		t.Fatalf("过期后同键再来应重新执行, got calls=%d", calls.Load())
	}
	t.Logf("业务=成功保留窗口: 窗口内复用（后端 1 次）；过期淘汰后同键重放重新执行（后端 %d 次），无卡住/报错", calls.Load())
}

// 业务：失败结论超过更短的失败保留窗口后淘汰，允许重新尝试后端。
func TestDedupeFailureTTLExpiry(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusNotFound)
	var calls atomic.Int64
	_, ins := newBackend(t, "flap", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(int(status.Load()))
	})
	f := forwarder.New(func(string) (model.Instance, bool) { return ins, true },
		forwarder.WithDedupeConfig(forwarder.DedupeConfig{
			SuccessTTL: time.Second, FailureTTL: 80 * time.Millisecond, Capacity: 16,
		}))
	sel := &staticSelector{instances: []model.Instance{ins}}
	rp := forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: time.Second}
	do := func() error {
		_, err := f.Execute(context.Background(), model.RequestContext{
			Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "ttl-fail",
		}, sel, rp)
		return err
	}
	if err := do(); model.ErrKind(err) != model.ErrUpstream4xx {
		t.Fatalf("首次应 4xx, got %v", err)
	}
	if err := do(); model.ErrKind(err) != model.ErrUpstream4xx || calls.Load() != 1 {
		t.Fatalf("失败窗口内应复用不重放, err=%v calls=%d", err, calls.Load())
	}
	time.Sleep(160 * time.Millisecond)
	// 后端恢复：失败结论过期后重新尝试，得到成功，证明失败缓存不会永久阻塞。
	status.Store(http.StatusOK)
	if err := do(); err != nil || calls.Load() != 2 {
		t.Fatalf("失败过期后应重新尝试并成功, err=%v calls=%d", err, calls.Load())
	}
	t.Logf("业务=失败保留窗口: 4xx 窗口内复用不重放（后端 1 次）；过期后后端恢复，重新尝试成功（后端 %d 次）", calls.Load())
}

// 业务：follower 等待受自身截止时间约束，leader 长时间不结束时 follower 不会无限挂起。
func TestDedupeFollowerBoundedByContext(t *testing.T) {
	release := make(chan struct{})
	_, ins := newBackend(t, "hang", func(http.ResponseWriter, *http.Request) { <-release })
	t.Cleanup(func() { close(release) })
	f := forwarder.New(func(string) (model.Instance, bool) { return ins, true })
	sel := &staticSelector{instances: []model.Instance{ins}}

	// leader 占住键。
	go func() {
		_, _ = f.Execute(context.Background(), model.RequestContext{
			Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "hang-key",
		}, sel, forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: 5 * time.Second})
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := f.Execute(ctx, model.RequestContext{
		Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "hang-key",
	}, sel, forwarder.RetryPolicy{MaxAttempts: 1})
	if model.ErrKind(err) != model.ErrTimeout {
		t.Fatalf("follower 等待超时应得到 timeout 类失败, got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("follower 应在自身截止时间附近返回, got %v", d)
	}
	t.Logf("业务=等待有界: follower 在 %v 后按自身截止时间返回 timeout，未无限等待在途 leader", time.Since(start))
}

// countSelector 模拟 least_conn：原子选择（最少在途）+ Release 归还，供端到端计数校验。
type countSelector struct {
	mu        sync.Mutex
	instances []model.Instance
	inflight  map[string]int
}

func (c *countSelector) Pick(exclude map[string]bool) (model.Instance, error) {
	return c.pick(exclude)
}
func (c *countSelector) PickAndBegin(exclude map[string]bool) (model.Instance, error) {
	return c.pick(exclude)
}
func (c *countSelector) pick(exclude map[string]bool) (model.Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var best *model.Instance
	bestN := 0
	for i := range c.instances {
		ins := &c.instances[i]
		if exclude[ins.ID] {
			continue
		}
		n := c.inflight[ins.ID]
		if best == nil || n < bestN {
			best, bestN = ins, n
		}
	}
	if best == nil {
		return model.Instance{}, model.NewError(model.ErrNoHealthyInstance, "", "no candidate")
	}
	c.inflight[best.ID]++ // Pick 与 PickAndBegin 共用：转发路径只在 Begin 路径成对归还
	return *best, nil
}
func (c *countSelector) Release(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight[id] > 0 {
		c.inflight[id]--
	}
}
func (c *countSelector) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	sum := 0
	for _, n := range c.inflight {
		sum += n
	}
	return sum
}

// 业务：长短请求混合、失败/超时/跳过交错后，在途计数必须回到 0。
func TestInflightCounterReturnsZero(t *testing.T) {
	srvA, insA := newBackend(t, "a", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	_ = srvA
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	insB := model.Instance{ID: "b", Service: "svc", Addr: dead.URL}
	dead.Close()
	_, insC := newBackend(t, "c", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	sel := &countSelector{
		instances: []model.Instance{insA, insB, insC},
		inflight:  map[string]int{},
	}
	// 过滤器对 b 报“已摘除/已注销”，覆盖连接前跳过的计数归还路径；a、c 存活。
	f := forwarder.New(func(id string) (model.Instance, bool) {
		switch id {
		case insA.ID:
			return insA, true
		case insC.ID:
			return insC, true
		default:
			return model.Instance{}, false
		}
	}, forwarder.WithLogger(func(f string, a ...any) { t.Logf("    "+f, a...) }))
	rp := forwarder.RetryPolicy{MaxAttempts: 3, PerCallTimeout: 120 * time.Millisecond}

	var wg sync.WaitGroup
	const waves = 12
	for w := 0; w < waves; w++ {
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _ = f.Execute(context.Background(), model.RequestContext{
					Service: "svc", Method: "POST", Path: "/job",
					IdempotencyKey: fmt.Sprintf("mix-%d-%d", w, i),
				}, sel, rp)
			}(i)
		}
	}
	wg.Wait()
	if total := sel.total(); total != 0 {
		t.Fatalf("全部请求结束后在途计数必须归零, got total=%d inflight=%v", total, sel.inflight)
	}
	t.Logf("业务=在途计数闭环: 12 波 × 8 请求（慢/快/连接拒绝/连接前跳过）全部结束，在途总数=%d（归零）", sel.total())
}
