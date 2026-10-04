package mesh_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/forwarder"
	"github.com/highcumontoa/service-discovery-lb-go/health"
	"github.com/highcumontoa/service-discovery-lb-go/mesh"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// newMeshWithShortFailureTTL 装配一个失败保留窗口很短（80ms）的 mesh，用于验证淘汰语义。
func newMeshWithShortFailureTTL(t *testing.T) (*mesh.Mesh, []*backend.Instance) {
	t.Helper()
	probe := health.NewHTTPProbe(300 * time.Millisecond).Func()
	m, err := mesh.New(mesh.Config{
		FailThreshold: 2, RecoverThreshold: 2,
		Probe: probe, ProbeInterval: 50 * time.Millisecond,
		InitialRules: rulesFor("echo", 2, time.Second),
		Dedupe: forwarder.DedupeConfig{
			SuccessTTL: time.Minute, FailureTTL: 80 * time.Millisecond, Capacity: 64,
		},
		Logf: testLogf(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	var bes []*backend.Instance
	for i := 0; i < 2; i++ {
		be, err := backend.New("inst-"+strconv.Itoa(i+1), "echo", backend.Behavior{})
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

// 业务：端到端——失败结论过保留窗口后，同键再来被当作全新请求；后端恢复后能成功，不被旧失败卡住。
func TestMeshFailureOutcomeEvictedThenFreshSuccess(t *testing.T) {
	m, bes := newMeshWithShortFailureTTL(t)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)
	bes[0].SetBehavior(backend.Behavior{CallStatus: 503})
	bes[1].SetBehavior(backend.Behavior{CallStatus: 503})

	// 首次：两实例都 5xx，MaxAttempts=2 耗尽预算 -> 最终失败（根因 upstream_5xx）。
	_, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "POST", Path: "/create", IdempotencyKey: "recover-key",
	})
	if model.RootKind(err) != model.ErrUpstream5xx {
		t.Fatalf("首次失败根因应为 upstream_5xx, got kind=%s root=%s err=%v",
			model.ErrKind(err), model.RootKind(err), err)
	}
	// 窗口内同键重放：复用失败，不访问后端，且不是成功。
	before := bes[0].Calls() + bes[1].Calls()
	_, err2 := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "POST", Path: "/create", IdempotencyKey: "recover-key",
	})
	if err2 == nil || model.ErrKind(err2) == "" {
		t.Fatal("失败窗口内重放必须仍为失败，不能被当成功")
	}
	if d := bes[0].Calls() + bes[1].Calls(); d != before {
		t.Fatalf("失败窗口内重放不得再次访问后端, before=%d after=%d", before, d)
	}
	// 等失败保留窗口过期。
	time.Sleep(200 * time.Millisecond)
	// 后端恢复健康响应。
	bes[0].SetBehavior(backend.Behavior{})
	bes[1].SetBehavior(backend.Behavior{})
	res, err3 := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "POST", Path: "/create", IdempotencyKey: "recover-key",
	})
	if err3 != nil {
		t.Fatalf("失败结论过期且后端恢复后，同键再来应作为全新请求成功, got %v", err3)
	}
	if res.DedupeHit {
		t.Fatal("过期淘汰后不应命中旧结论 DedupeHit")
	}
	t.Logf("业务=失败淘汰后恢复: 首次 5xx 失败（根因 upstream_5xx）；窗口内重放复用失败不访问后端；过期后同键重新执行成功 instance=%s，未被旧失败卡住",
		res.InstanceID)
}
