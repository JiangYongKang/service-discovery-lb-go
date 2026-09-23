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

// staticSelector 总是按固定顺序返回给定实例，便于构造确定性的重试序列。
type staticSelector struct {
	instances []model.Instance
}

func (s *staticSelector) Pick(exclude map[string]bool) (model.Instance, error) {
	for _, ins := range s.instances {
		if !exclude[ins.ID] {
			return ins, nil
		}
	}
	return model.Instance{}, model.NewError(model.ErrNoHealthyInstance, "", "no candidate")
}

func newBackend(t *testing.T, id string, handler http.HandlerFunc) (*httptest.Server, model.Instance) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv, model.Instance{ID: id, Service: "svc", Addr: srv.URL}
}

func TestDedupeConcurrentSameKeySingleSideEffect(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	srv, ins := newBackend(t, "only", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release // 模拟慢处理：并发同键请求在此期间到达
		w.WriteHeader(200)
	})
	_ = srv

	f := forwarder.New(func(id string) (model.Instance, bool) { return ins, true },
		forwarder.WithLogger(func(f string, a ...any) { t.Logf("    "+f, a...) }))
	sel := &staticSelector{instances: []model.Instance{ins}}

	var wg sync.WaitGroup
	results := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := f.Execute(context.Background(), model.RequestContext{
				Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "same-key",
			}, sel, forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: 2 * time.Second})
			if err != nil {
				results <- "err:" + err.Error()
				return
			}
			tag := "leader"
			if res.DedupeHit {
				tag = "dedupe"
			}
			results <- tag
		}()
	}
	time.Sleep(50 * time.Millisecond) // 等所有 follower 在去重屏障上等待
	close(release)
	wg.Wait()
	close(results)

	leaders, dedupes, errs := 0, 0, 0
	for r := range results {
		switch r {
		case "leader":
			leaders++
		case "dedupe":
			dedupes++
		default:
			errs++
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("同幂等键 20 并发只能产生 1 次后端副作用, got %d", calls.Load())
	}
	if leaders != 1 || dedupes != 19 || errs != 0 {
		t.Fatalf("应 1 个 leader + 19 个去重复用, got leaders=%d dedupe=%d errs=%d", leaders, dedupes, errs)
	}
	t.Logf("幂等去重：20 并发同键 -> 后端处理 %d 次，1 leader + 19 复用，无重复副作用", calls.Load())
}

func TestFailedInstanceNeverReturnedToHealthySet(t *testing.T) {
	var calls2 atomic.Int64
	// 实例 a 永久连接失败（直接关闭监听器）；实例 b 正常。
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	insA := model.Instance{ID: "a", Service: "svc", Addr: dead.URL}
	dead.Close()

	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls2.Add(1)
		w.WriteHeader(200)
	}))
	t.Cleanup(srvB.Close)
	insB := model.Instance{ID: "b", Service: "svc", Addr: srvB.URL}

	healthView := map[string]model.Instance{"a": insA, "b": insB}
	f := forwarder.New(func(id string) (model.Instance, bool) {
		ins, ok := healthView[id]
		return ins, ok
	}, forwarder.WithLogger(func(f string, a ...any) { t.Logf("    "+f, a...) }))
	sel := &staticSelector{instances: []model.Instance{insA, insB}}

	// 两次独立请求都必须先尝试 a（静态顺序），失败后重试到 b；
	// 证明转发层只在“本请求内”排除 a，从不改写健康集合，也不会把 a 摘除或恢复。
	for i := 0; i < 2; i++ {
		res, err := f.Execute(context.Background(), model.RequestContext{
			Service: "svc", Method: "GET", Path: "/", IdempotencyKey: fmt.Sprintf("k%d", i),
		}, sel, forwarder.RetryPolicy{MaxAttempts: 2, PerCallTimeout: time.Second})
		if err != nil {
			t.Fatalf("第 %d 次请求应重试到 b 成功, got %v", i, err)
		}
		if res.InstanceID != "b" {
			t.Fatalf("应由 b 服务, got %s", res.InstanceID)
		}
	}
	if calls2.Load() != 2 {
		t.Fatalf("b 应处理 2 次, got %d", calls2.Load())
	}
	t.Logf("失败实例 a 在两次请求中都被尝试并在请求内排除，健康集合从未被转发层改动，b 处理 2 次")
}

func TestNonIdempotentTimeoutNotRetried(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	ins := model.Instance{ID: "slow", Service: "svc", Addr: srv.URL}
	f := forwarder.New(func(id string) (model.Instance, bool) { return ins, true })
	sel := &staticSelector{instances: []model.Instance{ins}}

	// 无幂等键 + 超时（可能已产生副作用）：不得重试。
	_, err := f.Execute(context.Background(), model.RequestContext{
		Service: "svc", Method: "POST", Path: "/pay",
	}, sel, forwarder.RetryPolicy{MaxAttempts: 3, PerCallTimeout: 80 * time.Millisecond})
	if model.ErrKind(err) != model.ErrTimeout {
		t.Fatalf("非幂等超时应原样返回 timeout 而非重试耗尽, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("非幂等超时只允许 1 次后端调用, got %d", calls.Load())
	}
	t.Logf("非幂等请求超时不重试（防止重复扣款），后端调用 %d 次, err=%v", calls.Load(), err)
}
