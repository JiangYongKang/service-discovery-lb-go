package mesh_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/health"
	"github.com/highcumontoa/service-discovery-lb-go/mesh"
	"github.com/highcumontoa/service-discovery-lb-go/model"
	"github.com/highcumontoa/service-discovery-lb-go/router"
)

// testLogf 输出带前缀的解释性日志（单测 -v 可见实例状态与判定依据）。
func testLogf(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		t.Logf("    "+format, args...)
	}
}

func rulesFor(svc string, attempts int, timeout time.Duration) router.Rules {
	return router.Rules{Version: 1, ByService: map[string]router.Rule{
		svc: {Service: svc, MaxAttempts: attempts, PerCallTimeout: timeout},
	}}
}

// newMeshWithBackends 启动 n 个本地后端并注册，但尚未标记健康。
func newMeshWithBackends(t *testing.T, n int, attempts int) (*mesh.Mesh, []*backend.Instance) {
	t.Helper()
	probe := health.NewHTTPProbe(300 * time.Millisecond).Func()
	m, err := mesh.New(mesh.Config{
		FailThreshold: 2, RecoverThreshold: 2,
		Probe: probe, ProbeInterval: 50 * time.Millisecond,
		InitialRules: rulesFor("echo", attempts, 400*time.Millisecond),
		Logf:         testLogf(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	var bes []*backend.Instance
	for i := 0; i < n; i++ {
		be, err := backend.New(fmt.Sprintf("inst-%d", i+1), "echo", backend.Behavior{})
		if err != nil {
			t.Fatal(err)
		}
		bes = append(bes, be)
		if err := m.Register(be.ToModel()); err != nil {
			t.Fatal(err)
		}
	}
	return m, bes
}

// markHealthy 通过注入的探测函数驱动状态机，直到全部实例 HEALTHY。
func markHealthy(t *testing.T, m *mesh.Mesh, bes []*backend.Instance) {
	t.Helper()
	instByID := map[string]model.Instance{}
	for _, be := range bes {
		instByID[be.ID] = be.ToModel()
	}
	for round := 0; round < 10; round++ {
		reports := m.Monitor.RunOnceWith(instByID)
		healthy := 0
		for _, r := range reports {
			if r.State == model.StateHealthy {
				healthy++
			}
		}
		if healthy == len(bes) {
			return
		}
	}
	t.Fatal("实例未能在阈值内全部变为 HEALTHY")
}

// ---- 场景 1：无健康实例时请求必须被明确拒绝 ----

func TestNoHealthyInstanceRejected(t *testing.T) {
	m, bes := newMeshWithBackends(t, 2, 1)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	// 全部处于 UNKNOWN，未跑过探测。
	_, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/echo", IdempotencyKey: "k1",
	})
	if model.ErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("无健康实例应拒绝 no_healthy_instance, got %v", err)
	}
	t.Logf("全部 UNKNOWN 时请求被拒绝: %v | 依据: %s", err, model.DescribeKind(model.ErrKind(err)))

	if _, err := m.Call(context.Background(), model.RequestContext{
		Service: "missing", Method: "GET", Path: "/",
	}); model.ErrKind(err) != model.ErrServiceNotFound {
		t.Fatalf("未知服务应返回 service_not_found, got %v", err)
	}
}

// ---- 场景 2：正常路由 + 连接失败重试到其他健康实例（无重复副作用）----

func TestRetryOnConnectFailureNoDuplicateSideEffect(t *testing.T) {
	m, bes := newMeshWithBackends(t, 3, 3)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)

	// 关闭 inst-1 制造连接拒绝；注册表仍标记健康，转发层必须重试到其余实例。
	target := bes[0]
	targetID := target.ID
	target.Close()
	t.Logf("注入故障：%s 进程关闭（连接拒绝），但健康集合尚未摘除它", targetID)

	res, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "POST", Path: "/create", IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("存在其他健康实例时应重试成功, got %v", err)
	}
	if res.InstanceID == targetID {
		t.Fatal("不应由已关闭实例服务")
	}
	t.Logf("重试成功，最终实例=%s, 尝试链=%+v", res.InstanceID, res.Attempts)

	var totalCalls int64
	for _, be := range bes {
		totalCalls += be.Calls()
	}
	// 连接失败的实例没有业务调用；成功实例恰好 1 次 -> 无重复副作用。
	if totalCalls != 1 {
		t.Fatalf("全部后端业务处理总次数应为 1（无重复副作用）, got %d", totalCalls)
	}
	t.Logf("后端实际业务处理总次数=%d：连接失败不计副作用，成功实例恰好处理 1 次", totalCalls)
}

// ---- 场景 3：超时降级与重试预算 ----

func TestTimeoutExhaustsBudget(t *testing.T) {
	m, bes := newMeshWithBackends(t, 2, 2)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)
	// 全部实例高延迟，单次尝试超时 -> 两次尝试后 no_retry_budget。
	for _, be := range bes {
		be.SetBehavior(backend.Behavior{Latency: 2 * time.Second})
	}
	_, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/slow", IdempotencyKey: "t1",
	})
	if model.ErrKind(err) != model.ErrNoRetryBudget {
		t.Fatalf("应耗尽重试预算 no_retry_budget, got %v", err)
	}
	t.Logf("两次尝试均超时后确定性降级: %v", err)
}

// ---- 场景 4：4xx 不重试，原因可区分 ----

func Test4xxNotRetried(t *testing.T) {
	m, bes := newMeshWithBackends(t, 1, 3)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)
	bes[0].SetBehavior(backend.Behavior{CallStatus: 404})
	_, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/missing", IdempotencyKey: "e4",
	})
	if model.ErrKind(err) != model.ErrUpstream4xx {
		t.Fatalf("4xx 应原样返回 upstream_4xx, got %v", err)
	}
	if bes[0].Calls() != 1 {
		t.Fatalf("4xx 不应重试，后端调用次数应为 1, got %d", bes[0].Calls())
	}
	t.Logf("4xx 首次即返回且不重试: %v | 依据: %s", err, model.DescribeKind(model.ErrUpstream4xx))
}

var _ = errors.New
var _ = health.New
var _ = sync.Mutex{}
