package discovery

import (
	"sync"
	"testing"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

func ins(id string) model.Instance {
	return model.Instance{ID: id, Service: "svc-a", Addr: "http://127.0.0.1:0"}
}

func TestRegisterDuplicateAndDeregister(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(ins("i-1")); err != nil {
		t.Fatalf("首次注册失败: %v", err)
	}
	err := r.Register(ins("i-1"))
	if err == nil || model.ErrKind(err) != model.ErrDuplicateRegistration {
		t.Fatalf("重复注册应被拒绝为 duplicate_registration, got %v", err)
	}
	t.Logf("重复注册被拒绝: %v | 依据: %s", err, model.DescribeKind(model.ErrKind(err)))

	if err := r.Deregister("i-1"); err != nil {
		t.Fatalf("注销失败: %v", err)
	}
	if !r.IsDead("i-1") {
		t.Fatal("注销后 IsDead 应为 true")
	}
	err = r.Deregister("i-1")
	if err == nil || model.ErrKind(err) != model.ErrInstanceDead {
		t.Fatalf("重复注销应返回 instance_dead, got %v", err)
	}
	t.Logf("重复注销被拒绝: %v", err)

	err = r.Deregister("never-existed")
	if err == nil || model.ErrKind(err) != model.ErrInstanceNotRegistered {
		t.Fatalf("注销不存在实例应返回 instance_not_registered, got %v", err)
	}

	// 重新上线：允许同 ID 再注册，且不再是 dead。
	if err := r.Register(ins("i-1")); err != nil {
		t.Fatalf("重新上线应允许: %v", err)
	}
	if r.IsDead("i-1") {
		t.Fatal("重新注册后不应再是 dead")
	}
	if _, state, ok := r.Get("i-1"); !ok || state != model.StateUnknown {
		t.Fatalf("重新注册应为 UNKNOWN 初始态, got %v %v", state, ok)
	}
}

func TestHealthySetRoutingGate(t *testing.T) {
	r := NewRegistry()
	for _, id := range []string{"i-1", "i-2", "i-3"} {
		if err := r.Register(ins(id)); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.HealthyInstances("svc-a"); len(got) != 0 {
		t.Fatalf("UNKNOWN 初始态不可路由，期望 0，got %v", got)
	}
	r.SetHealth("i-1", model.StateHealthy)
	r.SetHealth("i-2", model.StateRecovering)
	r.SetHealth("i-3", model.StateUnhealthy)
	got := r.HealthyInstances("svc-a")
	if len(got) != 1 || got[0] != "i-1" {
		t.Fatalf("只有 HEALTHY 可路由, got %v", got)
	}
	t.Logf("健康集合=%v；i-2=RECOVERING、i-3=UNHEALTHY 均被排除，依据：仅 HEALTHY 状态 Routeable()=true", got)
}

func TestConcurrentDeregisterOnce(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(ins("i-x")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var okCount, deadCount, otherCount int
	var mu sync.Mutex
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.Deregister("i-x")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				okCount++
			case model.ErrKind(err) == model.ErrInstanceDead:
				deadCount++
			default:
				otherCount++
			}
		}()
	}
	wg.Wait()
	if okCount != 1 || deadCount != 31 || otherCount != 0 {
		t.Fatalf("并发注销应恰好 1 次成功 31 次 instance_dead, got ok=%d dead=%d other=%d",
			okCount, deadCount, otherCount)
	}
	t.Logf("32 并发注销：1 次成功移除，31 次得到 instance_dead（结论稳定可区分）")
}

func TestSnapshotImmutability(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(ins("i-1"))
	s1 := r.Snapshot()
	r.SetHealth("i-1", model.StateHealthy)
	s2 := r.Snapshot()
	if s1.Version >= s2.Version {
		t.Fatalf("健康变化应产生新版本, %d -> %d", s1.Version, s2.Version)
	}
	if s1.Healthy["i-1"] != model.StateUnknown {
		t.Fatal("旧快照必须保持 UNKNOWN，不应被后续修改污染")
	}
}
