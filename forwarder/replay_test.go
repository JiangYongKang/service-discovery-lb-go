package forwarder_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/forwarder"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// 用例：失败结论在保留窗口内被复用；TTL 过期后同键请求作为全新请求真正再打一次后端，
// 此时后端已恢复 -> 成功，不卡住、不报错。成功结论过期后同理（这里覆盖失败->恢复）。
func TestExpiredFailureRetriedAsFreshRequest(t *testing.T) {
	var status atomic.Int32
	status.Store(500)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := status.Load()
		w.WriteHeader(int(s))
	}))
	t.Cleanup(srv.Close)
	ins := model.Instance{ID: "flap", Service: "svc", Addr: srv.URL}

	f := forwarder.New(func(string) (model.Instance, bool) { return ins, true },
		forwarder.WithDedupeConfig(forwarder.DedupeConfig{
			TTL: 60 * time.Millisecond, MaxEntries: 16, WaitBudget: time.Second,
		}),
		forwarder.WithLogger(func(format string, a ...any) { t.Logf("    "+format, a...) }))
	sel := &staticSelector{instances: []model.Instance{ins}}
	rp := forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: time.Second}
	// 第一次：后端 500 -> no_retry_budget(cause=upstream_5xx) 失败结论。
	_, err := f.Execute(context.Background(), model.RequestContext{
		Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "replay-key",
	}, sel, rp)
	if model.ErrKind(err) != model.ErrNoRetryBudget || !errorHasCauseKind(err, model.ErrUpstream5xx) {
		t.Fatalf("首次应为 5xx 预算耗尽失败, got %v", err)
	}

	// 窗口内重放：复用失败，不打后端（状态切换也无影响）。
	status.Store(200)
	_, err = f.Execute(context.Background(), model.RequestContext{
		Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "replay-key",
	}, sel, rp)
	if model.ErrKind(err) != model.ErrNoRetryBudget || !errorHasCauseKind(err, model.ErrUpstream5xx) {
		t.Fatalf("窗口内必须复用同一份失败结论, got %v", err)
	}

	// 过期后重放：当作全新请求，真正打到已恢复的后端并成功。
	time.Sleep(120 * time.Millisecond)
	res, err := f.Execute(context.Background(), model.RequestContext{
		Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "replay-key",
	}, sel, rp)
	if err != nil || res.DedupeHit || res.StatusCode != 200 {
		t.Fatalf("过期后同键请求必须作为全新请求成功, res=%+v err=%v", res, err)
	}
	t.Logf("[过期重放] 5xx 失败 -> 窗口内复用失败（后端恢复也不打）-> TTL 过后同键全新执行成功")
}

// 用例：leader 在发布结论前消失（模拟异常路径），follower 在等待预算内拿到
// 明确的 dedupe_wait_timeout，而不是无限挂起。
func TestFollowerBoundedWaitWhenLeaderNeverPublishes(t *testing.T) {
	f := forwarder.New(nil,
		forwarder.WithDedupeConfig(forwarder.DedupeConfig{
			TTL: time.Second, MaxEntries: 4, WaitBudget: 40 * time.Millisecond,
		}))
	// 在同一存储内先占住 leader 名额但故意不发布结论。
	if leader, _ := f.Dedupe().Acquire("ghost-key"); !leader {
		t.Fatal("应取得 leader")
	}
	start := time.Now()
	_, err := f.Execute(context.Background(), model.RequestContext{
		Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "ghost-key",
	}, &staticSelector{}, forwarder.RetryPolicy{MaxAttempts: 1})
	elapsed := time.Since(start)
	if model.ErrKind(err) != model.ErrDedupeWaitTimeout {
		t.Fatalf("leader 不发布时 follower 应在预算内得到 dedupe_wait_timeout, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("follower 必须有界返回, 实际耗时 %v", elapsed)
	}
	t.Logf("[有界等待] leader 未发布结论时 follower 在 %v 内返回 dedupe_wait_timeout（依据：%s）",
		elapsed.Round(time.Millisecond), model.DescribeKind(model.ErrDedupeWaitTimeout))
}
