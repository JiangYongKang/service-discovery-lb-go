package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestRegisterDuplicateAndDeregisterReasons(t *testing.T) {
	reg := NewRegistry()
	inst := NewInstance("i-1", "svc", "127.0.0.1:9000", nil)
	if err := reg.Register(inst); err != nil {
		t.Fatalf("first register: %v", err)
	}

	err := reg.Register(NewInstance("i-1", "svc", "127.0.0.1:9001", nil))
	if err == nil {
		t.Fatal("duplicate register must be rejected")
	}
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != RejectDuplicateRegister {
		t.Fatalf("want RejectDuplicateRegister, got %v", err)
	}
	if !errors.Is(err, ErrDuplicateInstance) {
		t.Fatal("error must wrap ErrDuplicateInstance")
	}
	t.Logf("duplicate register rejected: reason=%s msg=%q", re.Reason, re.Message)

	if err := reg.Deregister("i-1"); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	if err := reg.Deregister("i-1"); err == nil {
		t.Fatal("double deregister must be rejected")
	} else if errors.As(err, &re) && re.Reason != RejectInstanceDeregistered {
		t.Fatalf("want RejectInstanceDeregistered, got reason=%d", re.Reason)
	} else {
		t.Logf("double deregister rejected: reason=%s msg=%q", re.Reason, re.Message)
	}

	// 失效实例不允许重新注册，且原因与重复注册不同。
	err = reg.Register(NewInstance("i-1", "svc", "127.0.0.1:9002", nil))
	if err == nil {
		t.Fatal("re-register of tombstoned instance must be rejected")
	}
	if !errors.As(err, &re) || re.Reason != RejectInstanceDeregistered {
		t.Fatalf("want RejectInstanceDeregistered, got %v", err)
	}
	t.Logf("tombstone re-register rejected: reason=%s msg=%q", re.Reason, re.Message)

	if err := reg.Deregister("ghost"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("want ErrInstanceNotFound, got %v", err)
	}
}

func TestRoutableOnlyWhenHealthy(t *testing.T) {
	reg := NewRegistry()
	for _, s := range []InstanceState{StateRegistered, StateProbing, StateUnhealthy, StateDeregistered} {
		id := fmt.Sprintf("inst-%s", s)
		if err := reg.Register(NewInstance(id, "svc", "a", nil)); err != nil {
			t.Fatal(err)
		}
		if s == StateDeregistered {
			_ = reg.Deregister(id)
		} else {
			reg.SetState(id, s)
		}
		got, _ := reg.Get(id)
		if got.Routable() {
			t.Fatalf("state %s must not be routable", s)
		}
		t.Logf("instance %s state=%s routable=false", id, got.State())
	}

	_ = reg.Register(NewInstance("ok", "svc", "b", nil))
	if !reg.SetState("ok", StateHealthy) {
		t.Fatal("set healthy")
	}
	got, _ := reg.Get("ok")
	if !got.Routable() {
		t.Fatal("healthy must be routable")
	}
	if len(reg.HealthyInstances("svc")) != 1 {
		t.Fatal("exactly one healthy instance")
	}
	t.Logf("instance ok state=%s routable=true", got.State())
}

func TestConcurrentDeregisterAndSnapshot(t *testing.T) {
	reg := NewRegistry()
	const n = 50
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("i-%02d", i)
		if err := reg.Register(NewInstance(id, "svc", "x", nil)); err != nil {
			t.Fatal(err)
		}
		reg.SetState(id, StateHealthy)
	}

	var wg sync.WaitGroup
	// 一半 goroutine 反复读快照，一半并发注销。
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 200; k++ {
				snap := reg.Snapshot()
				for _, in := range snap.Instances {
					// 快照不可变：任何实例要么健康可路由，要么不可路由，不允许“半状态”
					if in.State() == StateHealthy && !in.Routable() {
						t.Errorf("inconsistent snapshot for %s", in.ID)
					}
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = reg.Deregister(fmt.Sprintf("i-%02d", i))
		}(i)
	}
	wg.Wait()

	if got := reg.HealthyInstances("svc"); len(got) != 0 {
		t.Fatalf("all deregistered, but %d healthy remain", len(got))
	}
	// 注销后状态迁移必须被拒绝
	if reg.SetState("i-00", StateHealthy) {
		t.Fatal("must not transition a deregistered instance")
	}
	if _, ok := reg.Transition("i-00", StateHealthy, 0); ok {
		t.Fatal("transition must report failure for deregistered instance")
	}
	t.Log("concurrent deregister converged: 0 healthy, transitions rejected")
}

func TestWatchVersion(t *testing.T) {
	reg := NewRegistry()
	ctx := context.Background()
	go func() {
		_ = reg.Register(NewInstance("a", "s", "x", nil))
	}()
	v, err := reg.WatchVersion(ctx, 0)
	if err != nil || v != 1 {
		t.Fatalf("watch version: v=%d err=%v", v, err)
	}
}
