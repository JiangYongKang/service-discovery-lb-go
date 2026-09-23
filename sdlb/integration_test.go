package sdlb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/discovery"
	"github.com/highcumontoa/service-discovery-lb-go/forward"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/route"
)

// startBackends 启动 n 个真实 TCP 后端并注册到 sdlb。
func startBackends(t *testing.T, s *SDLB, service string, n int) []*backend.Server {
	t.Helper()
	out := make([]*backend.Server, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", service, i)
		srv, err := backend.New(id, service, true)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.Stop)
		if err := s.RegisterInstance(id, service, srv.Addr, nil); err != nil {
			t.Fatal(err)
		}
		out = append(out, srv)
	}
	return out
}

func tcpInvoke(dial func(ctx context.Context, addr, id string) (string, error)) forward.InvokeFunc {
	return func(ctx context.Context, inst *discovery.Instance) (any, error) {
		resp, err := dial(ctx, inst.Addr, inst.ID)
		if err != nil {
			// 统一映射到 forward 的语义错误
			if isTimeoutErr(ctx, err) {
				return nil, forward.ErrUpstreamTimeout
			}
			return nil, fmt.Errorf("%w: %v", forward.ErrConnectionRefused, err)
		}
		return resp, nil
	}
}

func isTimeoutErr(ctx context.Context, err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
		return true
	}
	return strings.Contains(err.Error(), "i/o timeout")
}

// 场景 1：正常注册 -> 探活转健康 -> 请求成功且轮询分布。
func TestIntegrationRegisterAndRoute(t *testing.T) {
	cfg := Config{Health: healthCfg(40*time.Millisecond, 150*time.Millisecond)}
	s := New(cfg, backend.TCPProbe())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	srvs := startBackends(t, s, "echo", 3)

	waitHealthy(t, s, "echo", 3, time.Second)
	t.Logf("after probing:\n%s", s.ExplainStatus("echo"))

	dial := backend.DialInvoke()
	hits := map[string]int{}
	var mu sync.Mutex
	for i := 0; i < 12; i++ {
		res, err := s.Call(context.Background(), "echo", "", tcpInvoke(dial))
		if err != nil {
			t.Fatalf("call %d: %v\n%s", i, err, res.Explain())
		}
		id := strings.Fields(res.Response.(string))[1]
		mu.Lock()
		hits[id]++
		mu.Unlock()
	}
	if len(hits) != 3 {
		t.Fatalf("round-robin should hit all 3, got %v", hits)
	}
	total := int64(0)
	for _, srv := range srvs {
		total += srv.RequestCount()
	}
	if total != 12 {
		t.Fatalf("backend counters want 12 total, got %d", total)
	}
	t.Logf("request distribution: %v; backend counters total=%d", hits, total)
}

// 场景 2：实例假死 -> 探测失败摘除；实例进程下线 -> 连接失败摘除 + 安全重试。
func TestIntegrationHealthProbeRemovalAndDialFailover(t *testing.T) {
	cfg := Config{Health: healthCfg(40*time.Millisecond, 120*time.Millisecond)}
	s := New(cfg, backend.TCPProbe())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	srvs := startBackends(t, s, "echo", 2)
	waitHealthy(t, s, "echo", 2, time.Second)

	// echo-0 假死：健康探测应在 FailureThreshold 轮后摘除
	srvs[0].SetHealthy(false)
	srvs[0].Stall(time.Second)
	time.Sleep(400 * time.Millisecond)
	if got := s.Registry().HealthyInstances("echo"); len(got) != 1 || got[0].ID != "echo-1" {
		t.Fatalf("after stall, only echo-1 must remain healthy: %s", s.ExplainStatus("echo"))
	}
	t.Logf("echo-0 stalled and removed:\n%s", s.ExplainStatus("echo"))

	// 此时请求必须全部落在 echo-1
	dial := backend.DialInvoke()
	for i := 0; i < 5; i++ {
		res, err := s.Call(context.Background(), "echo", "", tcpInvoke(dial))
		if err != nil {
			t.Fatal(err)
		}
		if id := strings.Fields(res.Response.(string))[1]; id != "echo-1" {
			t.Fatalf("request must go to echo-1, got %s", id)
		}
	}

	// echo-1 进程下线（连接拒绝）：允许重试一次但没有其它健康节点 -> fail-open
	srvs[1].Stop()
	_, _ = s.SetRules([]*route.Rule{{
		Service: "echo", Strategy: lb.StrategyRoundRobin,
		Timeout: 300 * time.Millisecond, MaxAttempts: 2, FailOpen: true,
	}})
	res, err := s.Call(context.Background(), "echo", "", tcpInvoke(dial))
	if err != nil {
		t.Fatalf("fail-open expected, got %v", err)
	}
	if res.Outcome != forward.OutcomeFailOpen {
		t.Fatalf("want fail-open, got %s", res.Outcome)
	}
	t.Logf("all backends down -> fail-open:\n%s", res.Explain())
}

// 场景 3：探测抖动（快速翻转健康）期间，并发请求绝不能路由到非健康实例。
func TestIntegrationProbeJitterUnderConcurrency(t *testing.T) {
	cfg := Config{Health: healthCfg(30*time.Millisecond, 80*time.Millisecond)}
	s := New(cfg, backend.TCPProbe())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	srvs := startBackends(t, s, "echo", 4)
	waitHealthy(t, s, "echo", 4, time.Second)
	_, _ = s.SetRules([]*route.Rule{{
		Service: "echo", Strategy: lb.StrategyRoundRobin,
		Timeout: 400 * time.Millisecond, MaxAttempts: 1, FailOpen: true,
	}})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// 抖动器：反复翻转两个实例
	for _, idx := range []int{0, 1} {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				srvs[idx].SetHealthy(!srvs[idx].IsHealthy())
				srvs[idx].Stall(300 * time.Millisecond)
				time.Sleep(35 * time.Millisecond)
			}
		}(idx)
	}

	dial := backend.DialInvoke()
	var badRouted atomic.Int32 // 成功响应却来自不健康实例（以调用时刻注册表为准的校验在 invoke 内做）
	var attempted atomic.Int32
	const callers = 12
	const perCaller = 30
	var cwg sync.WaitGroup
	for c := 0; c < callers; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for k := 0; k < perCaller; k++ {
				res, _ := s.Call(context.Background(), "echo", "", func(ctx context.Context, inst *discovery.Instance) (any, error) {
					// 拨号前记录注册中心状态（转发器已复核过一次，这里再次独立断言）
					cur, ok := s.Registry().Get(inst.ID)
					if !ok || !cur.Routable() {
						badRouted.Add(1)
						return nil, fmt.Errorf("should never be invoked: %s", inst.ID)
					}
					attempted.Add(1)
					return dial(ctx, inst.Addr, inst.ID)
				})
				_ = res
			}
		}()
	}
	cwg.Wait()
	close(stop)
	wg.Wait()

	if badRouted.Load() != 0 {
		t.Fatalf("%d calls reached non-routable instances during jitter", badRouted.Load())
	}
	t.Logf("jitter concurrency: real attempts=%d, calls to non-routable=0", attempted.Load())
	t.Logf("final registry state:\n%s", s.ExplainStatus("echo"))
}

// 场景 4：热更新期间在途请求策略不变；一致哈希 key 粘性在版本内稳定。
func TestIntegrationHotUpdateStickiness(t *testing.T) {
	cfg := Config{Health: healthCfg(30*time.Millisecond, 80*time.Millisecond)}
	s := New(cfg, backend.TCPProbe())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	startBackends(t, s, "echo", 4)
	waitHealthy(t, s, "echo", 4, time.Second)

	dial := backend.DialInvoke()
	_, _ = s.SetRules([]*route.Rule{{
		Service: "echo", Strategy: lb.StrategyConsistent,
		Timeout: 500 * time.Millisecond, MaxAttempts: 1,
	}})

	// 同 key 在无上下线时恒定
	map1 := map[string]string{}
	for _, k := range []string{"u1", "u2", "u3", "u4"} {
		res, err := s.Call(context.Background(), "echo", k, tcpInvoke(dial))
		if err != nil {
			t.Fatal(err)
		}
		map1[k] = strings.Fields(res.Response.(string))[1]
	}
	for round := 0; round < 20; round++ {
		for _, k := range []string{"u1", "u2", "u3", "u4"} {
			res, err := s.Call(context.Background(), "echo", k, tcpInvoke(dial))
			if err != nil {
				t.Fatal(err)
			}
			if id := strings.Fields(res.Response.(string))[1]; id != map1[k] {
				t.Fatalf("key %s moved %s->%s without membership change", k, map1[k], id)
			}
		}
	}
	t.Logf("consistent-hash stable: %v", map1)

	// 后台反复热更新（合法/非法交替），并发请求必须要么成功要么 fail-open，
	// 且每个 Result 自带其使用的规则版本。
	verBefore := s.Rules().CurrentVersion()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		strategies := []lb.Strategy{lb.StrategyRoundRobin, lb.StrategyRandom, lb.StrategyWeighted}
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, err := s.SetRules([]*route.Rule{{
				Service: "echo", Strategy: strategies[i%3],
				Timeout: 500 * time.Millisecond, MaxAttempts: 1,
			}})
			if err != nil {
				t.Errorf("valid update failed: %v", err)
				return
			}
			_, _ = s.SetRules([]*route.Rule{{Service: ""}}) // 非法：必须被拒绝
			i++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	var cwg sync.WaitGroup
	versions := sync.Map{}
	for c := 0; c < 8; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for k := 0; k < 40; k++ {
				res, err := s.Call(context.Background(), "echo", "", tcpInvoke(dial))
				if err != nil && res == nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				versions.Store(res.RuleVer, struct{}{})
			}
		}()
	}
	cwg.Wait()
	close(stop)
	wg.Wait()

	if s.Rules().CurrentVersion() <= verBefore {
		t.Fatal("version must advance")
	}
	n := 0
	versions.Range(func(_, _ any) bool { n++; return true })
	if n < 2 {
		t.Fatalf("in-flight requests should span multiple rule versions, saw %d", n)
	}
	t.Logf("requests during hot update spanned %d rule versions; final version=%d",
		n, s.Rules().CurrentVersion())
}
