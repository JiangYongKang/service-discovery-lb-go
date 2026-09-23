package forward

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
	"github.com/highcumontoa/service-discovery-lb-go/health"
)

type fakeReporter struct{ calls atomic.Int32 }

func (f *fakeReporter) ReportTransportFailure(id string) { f.calls.Add(1) }

// reportingChain 同时驱动真实健康检查器并记录调用次数。
type reportingChain struct {
	primary *health.Checker
	counter *fakeReporter
}

func (c reportingChain) ReportTransportFailure(id string) {
	c.primary.ReportTransportFailure(id)
	c.counter.ReportTransportFailure(id)
}

func regWithHealthy(ids ...string) *discovery.Registry {
	reg := discovery.NewRegistry()
	for _, id := range ids {
		_ = reg.Register(discovery.NewInstance(id, "svc", "addr-"+id, nil))
		reg.SetState(id, discovery.StateHealthy)
	}
	return reg
}

func TestNoBackendRejectedAndFailOpen(t *testing.T) {
	reg := discovery.NewRegistry()
	f := NewForwarder(reg)
	opts := Options{Service: "svc", Timeout: time.Second, MaxAttempts: 1}

	_, err := f.Do(context.Background(), opts, nil)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}

	opts.FailOpen = true
	res, err := f.Do(context.Background(), opts, nil)
	if err != nil {
		t.Fatalf("fail-open must not return error, got %v", err)
	}
	if res.Outcome != OutcomeFailOpen {
		t.Fatalf("want fail-open outcome, got %s", res.Outcome)
	}
	if d, ok := res.Response.(Degraded); !ok || d.Service != "svc" {
		t.Fatalf("bad degraded response: %#v", res.Response)
	}
	t.Logf("no-backend fail-open: %s | %s", res.Outcome, res.Explain())
}

func TestConnRefusedRetriesWithoutDuplicateSideEffects(t *testing.T) {
	reg := regWithHealthy("a", "b", "c")
	checker := health.NewChecker(reg,
		func(ctx context.Context, _ *discovery.Instance) error { return nil },
		health.Config{FailureThreshold: 3, SuccessThreshold: 2})
	rep := &fakeReporter{}
	f := NewForwarder(reg).WithFailureReporter(reportingChain{checker, rep})
	opts := Options{Service: "svc", Timeout: time.Second, MaxAttempts: 3}

	var sent atomic.Int32
	invoke := func(ctx context.Context, inst *discovery.Instance) (any, error) {
		n := sent.Add(1)
		if inst.ID == "a" {
			// 仅 a 连接失败；请求确定未到达
			return nil, fmt.Errorf("dial %s: %w", inst.Addr, ErrConnectionRefused)
		}
		return fmt.Sprintf("ok-from-%s#%d", inst.ID, n), nil
	}
	res, err := f.Do(context.Background(), opts, invoke)
	if err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if res.Outcome != OutcomeSuccess {
		t.Fatalf("want success, got %s", res.Outcome)
	}
	if sent.Load() != 2 {
		t.Fatalf("exactly one failed attempt + one success = 2 sends, got %d", sent.Load())
	}
	if rep.calls.Load() != 1 {
		t.Fatalf("failure reported once, got %d", rep.calls.Load())
	}
	got, _ := reg.Get("a")
	if got.Routable() {
		t.Fatalf("failed instance a must be removed from healthy set, state=%s", got.State())
	}
	t.Logf("retry result: %s\n%s", res.Response, res.Explain())
	t.Logf("instance a after failure: state=%s routable=false", got.State())
}

func TestTimeoutNotRetried(t *testing.T) {
	reg := regWithHealthy("a", "b")
	f := NewForwarder(reg)
	opts := Options{Service: "svc", Timeout: 30 * time.Millisecond, MaxAttempts: 3}

	var sent atomic.Int32
	invoke := func(ctx context.Context, inst *discovery.Instance) (any, error) {
		sent.Add(1)
		<-ctx.Done() // 拖到超时：结果未知，可能有副作用
		return nil, ctx.Err()
	}
	res, err := f.Do(context.Background(), opts, invoke)
	if err == nil {
		t.Fatal("timeout must be surfaced")
	}
	if sent.Load() != 1 {
		t.Fatalf("timeout must not be retried (duplicate side effect), sends=%d", sent.Load())
	}
	if res.Outcome != OutcomeTimeout {
		t.Fatalf("want timeout outcome, got %s", res.Outcome)
	}
	t.Logf("timeout not retried, sends=1; %s", res.Explain())
}

func TestAllNodesFailThenFailOpen(t *testing.T) {
	reg := regWithHealthy("a", "b")
	checker := health.NewChecker(reg,
		func(ctx context.Context, _ *discovery.Instance) error { return nil },
		health.Config{FailureThreshold: 3, SuccessThreshold: 2})
	f := NewForwarder(reg).WithFailureReporter(checker)
	opts := Options{Service: "svc", Timeout: time.Second, MaxAttempts: 3, FailOpen: true}
	invoke := func(ctx context.Context, inst *discovery.Instance) (any, error) {
		return nil, errors.New("dial: " + ErrConnectionRefused.Error())
	}
	res, err := f.Do(context.Background(), opts, invoke)
	if err != nil {
		t.Fatalf("fail-open path: %v", err)
	}
	if res.Outcome != OutcomeFailOpen {
		t.Fatalf("want fail-open, got %s", res.Outcome)
	}
	// 两个节点各尝试一次（换节点重试），总共 2 次副作用
	if n := countRealAttempts(res); n != 2 {
		t.Fatalf("want 2 real attempts across 2 nodes, got %d", n)
	}
	healthy := reg.HealthyInstances("svc")
	if len(healthy) != 0 {
		t.Fatalf("both failed nodes must leave healthy set, remain=%d", len(healthy))
	}
	t.Logf("all-nodes-fail fail-open: %s\n%s", res.Outcome, res.Explain())
}

func TestConcurrentDeregisterNeverRouted(t *testing.T) {
	// 多轮：每轮注册 4 个全新 ID（墓碑不可复用），与转发并发注销。
	// invoke 内部模拟真实拨号：进入时若实例已注销，则如同端口已关，
	// 返回 ErrConnectionRefused——转发器必须安全跳过并换节点。
	const rounds = 100
	var badHits atomic.Int32 // 成功响应对应的实例在拨号瞬间已注销的次数（必须恒为 0）
	var successes atomic.Int32

	for r := 0; r < rounds; r++ {
		var mu sync.Mutex
		liveAtCall := map[string]struct{}{}
		reg := discovery.NewRegistry()
		ids := []string{}
		for k := 0; k < 4; k++ {
			id := fmt.Sprintf("r%d-k%d", r, k)
			ids = append(ids, id)
			_ = reg.Register(discovery.NewInstance(id, "svc", "addr", nil))
			reg.SetState(id, discovery.StateHealthy)
		}
		f := NewForwarder(reg)
		opts := Options{Service: "svc", Timeout: time.Second, MaxAttempts: 4}

		invoke := func(ctx context.Context, inst *discovery.Instance) (any, error) {
			// 拨号瞬间复核：模拟 TCP connect 对已关闭端口的感知
			cur, ok := reg.Get(inst.ID)
			if !ok || cur.State() == discovery.StateDeregistered {
				return nil, ErrConnectionRefused
			}
			mu.Lock()
			liveAtCall[inst.ID] = struct{}{}
			mu.Unlock()
			successes.Add(1)
			return "ok-" + inst.ID, nil
		}

		doneDereg := make(chan struct{})
		go func() {
			for _, id := range ids {
				_ = reg.Deregister(id)
			}
			close(doneDereg)
		}()
		res, _ := f.Do(context.Background(), opts, invoke)
		<-doneDereg

		if res.Outcome == OutcomeSuccess {
			id := res.Response.(string)
			id = id[len("ok-"):]
			// 成功结论只与“拨号瞬间”的状态有关；之后下线不影响结论正确性。
			mu.Lock()
			_, live := liveAtCall[id]
			mu.Unlock()
			if !live {
				badHits.Add(1)
			}
		}
	}
	if badHits.Load() != 0 {
		t.Fatalf("%d requests succeeded on deregistered instances", badHits.Load())
	}
	t.Logf("over %d rounds with concurrent deregister: successes=%d, bad-hits=0",
		rounds, successes.Load())
}

func TestAttemptBudgetCapsSideEffects(t *testing.T) {
	reg := regWithHealthy("a", "b", "c")
	f := NewForwarder(reg)
	opts := Options{Service: "svc", Timeout: time.Second, MaxAttempts: 2}
	var sent atomic.Int32
	invoke := func(ctx context.Context, inst *discovery.Instance) (any, error) {
		sent.Add(1)
		return nil, ErrConnectionRefused
	}
	res, _ := f.Do(context.Background(), opts, invoke)
	if sent.Load() != 2 {
		t.Fatalf("MaxAttempts=2 must cap real sends at 2, got %d", sent.Load())
	}
	t.Logf("attempt budget honored: sends=%d outcome=%s", sent.Load(), res.Outcome)
}

func countRealAttempts(res *Result) int {
	n := 0
	for _, a := range res.Attempts {
		if !a.Skipped {
			n++
		}
	}
	return n
}
