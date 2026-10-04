package forwarder_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/forwarder"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// errorHasCauseKind 沿 Unwrap 链查找原始失败类别是否仍可区分。
func errorHasCauseKind(err error, kind model.ErrorKind) bool {
	for err != nil {
		var ce *model.CallError
		if errors.As(err, &ce) && ce.Kind == kind {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// 用例：同一个键的失败结论被并发与后续调用一致复用，失败类别保持可区分，
// 且绝不以“成功”形式返回；后端只真正处理一次（4xx/5xx 场景）。
func TestFailureOutcomeConcurrentlyReused(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		wantKind model.ErrorKind
		attempts int
	}{
		{"4xx 首次失败即收敛", 422, model.ErrUpstream4xx, 1},
		{"5xx 预算耗尽后收敛", 503, model.ErrNoRetryBudget, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var calls atomic.Int64
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(c.status)
			})
			srv1 := httptest.NewServer(handler)
			srv2 := httptest.NewServer(handler)
			t.Cleanup(srv1.Close)
			t.Cleanup(srv2.Close)
			in1 := model.Instance{ID: "e1", Service: "svc", Addr: srv1.URL}
			in2 := model.Instance{ID: "e2", Service: "svc", Addr: srv2.URL}
			f := forwarder.New(func(id string) (model.Instance, bool) {
				if id == "e1" {
					return in1, true
				}
				return in2, true
			}, forwarder.WithLogger(func(format string, a ...any) { t.Logf("    "+format, a...) }))
			sel := &staticSelector{instances: []model.Instance{in1, in2}}

			const n = 12
			start := make(chan struct{})
			var wg sync.WaitGroup
			kinds := make(chan model.ErrorKind, n)
			statusOK := make(chan struct{}, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					res, err := f.Execute(context.Background(), model.RequestContext{
						Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "fail-key",
					}, sel, forwarder.RetryPolicy{MaxAttempts: c.attempts, PerCallTimeout: time.Second})
					if err == nil {
						t.Errorf("失败结论不能被当成成功返回, res=%+v", res)
						return
					}
					kinds <- model.ErrKind(err)
					if res != nil && res.StatusCode >= 200 && res.StatusCode < 400 {
						statusOK <- struct{}{}
					}
				}()
			}
			close(start)
			wg.Wait()
			close(kinds)
			close(statusOK)

			for k := range kinds {
				if k != c.wantKind {
					t.Fatalf("所有同键调用必须拿到同一失败类别 %s, got %s", c.wantKind, k)
				}
			}
			if len(statusOK) != 0 {
				t.Fatal("任何复用方都不应观察到 2xx/3xx 成功状态")
			}
			// leader 真正尝试 c.attempts 次（同实例重试计数由后端看到），其余 11 个调用零副作用。
			if calls.Load() != int64(c.attempts) {
				t.Fatalf("后端只应被 leader 访问 %d 次, got %d", c.attempts, calls.Load())
			}

			// 过一会儿同键再来（仍在窗口内）：依旧复用失败，不产生新副作用。
			_, err := f.Execute(context.Background(), model.RequestContext{
				Service: "svc", Method: "GET", Path: "/", IdempotencyKey: "fail-key",
			}, sel, forwarder.RetryPolicy{MaxAttempts: c.attempts, PerCallTimeout: time.Second})
			if model.ErrKind(err) != c.wantKind {
				t.Fatalf("后来的同键调用必须复用同一失败类别, got %v", err)
			}
			if calls.Load() != int64(c.attempts) {
				t.Fatalf("复用失败结论不得再打后端, calls=%d", calls.Load())
			}
			t.Logf("[失败复用] %s：%d 并发 + 1 重放全部拿到 kind=%s（依据：%s），后端仅处理 %d 次，无一记为成功",
				c.name, n, c.wantKind, model.DescribeKind(c.wantKind), calls.Load())
		})
	}
}

// 用例：连接失败（无监听）与超时两种传输类失败，结论也按原类别复用。
func TestTransportFailureOutcomeReused(t *testing.T) {
	t.Run("连接失败", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		addr := dead.URL
		dead.Close()
		ins := model.Instance{ID: "dead", Service: "svc", Addr: addr}
		var attempts atomic.Int64
		f := forwarder.New(func(string) (model.Instance, bool) { return ins, true },
			forwarder.WithLogger(func(format string, a ...any) { t.Logf("    "+format, a...) }))
		sel := countingSelector{inner: &staticSelector{instances: []model.Instance{ins}}, n: &attempts}

		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := f.Execute(context.Background(), model.RequestContext{
					Service: "svc", Method: "POST", Path: "/c", IdempotencyKey: "conn-key",
				}, &sel, forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: time.Second})
				// MaxAttempts=1：可重试的连接失败在预算耗尽后降级为 no_retry_budget，
				// cause 必须保留原始 connect_failed，类别仍可区分。
				if model.ErrKind(err) != model.ErrNoRetryBudget {
					t.Errorf("预算耗尽应为 no_retry_budget, got %v", err)
					return
				}
				if !errorHasCauseKind(err, model.ErrConnectFailed) {
					t.Errorf("失败原因必须保留原始 connect_failed, got %v", err)
				}
			}()
		}
		wg.Wait()
		if attempts.Load() != 1 {
			t.Fatalf("只有 leader 选取一次实例, got %d", attempts.Load())
		}
		t.Logf("[失败复用] 连接失败：6 并发同键全部拿到 no_retry_budget(cause=connect_failed)，原始原因可区分；仅 leader 真正尝试")
	})

	t.Run("超时", func(t *testing.T) {
		var calls atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			time.Sleep(300 * time.Millisecond)
			w.WriteHeader(200)
		}))
		t.Cleanup(srv.Close)
		ins := model.Instance{ID: "slow", Service: "svc", Addr: srv.URL}
		f := forwarder.New(func(string) (model.Instance, bool) { return ins, true },
			forwarder.WithLogger(func(format string, a ...any) { t.Logf("    "+format, a...) }))
		sel := &staticSelector{instances: []model.Instance{ins}}

		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := f.Execute(context.Background(), model.RequestContext{
					Service: "svc", Method: "POST", Path: "/t", IdempotencyKey: "timeout-key",
				}, sel, forwarder.RetryPolicy{MaxAttempts: 1, PerCallTimeout: 60 * time.Millisecond})
				// MaxAttempts=1：超时后确定性降级为 no_retry_budget，cause 保留 timeout。
				if model.ErrKind(err) != model.ErrNoRetryBudget {
					t.Errorf("单次预算超时应降级 no_retry_budget, got %v", err)
					return
				}
				if !errorHasCauseKind(err, model.ErrTimeout) {
					t.Errorf("超时失败原因必须仍可区分为 timeout, got %v", err)
				}
			}()
		}
		wg.Wait()
		if calls.Load() != 1 {
			t.Fatalf("慢后端只应被 leader 访问 1 次, got %d", calls.Load())
		}
		t.Logf("[失败复用] 超时：6 并发同键全部拿到 no_retry_budget(cause=timeout)，原始原因可区分；后端仅开始处理 %d 次",
			calls.Load())
	})
}

// countingSelector 包装选择器并统计 Pick 次数，用于证明 follower 不做任何选择。
type countingSelector struct {
	inner forwarder.Selector
	n     *atomic.Int64
}

func (s *countingSelector) Pick(exclude map[string]bool) (model.Instance, error) {
	s.n.Add(1)
	return s.inner.Pick(exclude)
}
