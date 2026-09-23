package health

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

func fastConfig() Config {
	return Config{
		Interval:         50 * time.Millisecond,
		Timeout:          200 * time.Millisecond,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		ProbeInterval:    25 * time.Millisecond,
	}
}

// drainDecisions 消费判定通道并返回摘要日志。
func drainDecisions(ch <-chan Decision) []Decision {
	out := []Decision{}
	for {
		select {
		case d := <-ch:
			out = append(out, d)
		default:
			return out
		}
	}
}

func TestHealthyThenFailureRemovalAndRecovery(t *testing.T) {
	reg := discovery.NewRegistry()
	_ = reg.Register(discovery.NewInstance("a", "svc", "addr", nil))
	reg.SetState("a", discovery.StateHealthy)

	up := make(chan struct{})
	close(up)
	down := make(chan struct{}) // 永远阻塞 -> 探测超时

	var failFlag atomicBool
	probe := func(ctx context.Context, inst *discovery.Instance) error {
		if failFlag.get() {
			select {
			case <-down:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		<-up
		return nil
	}
	c := NewChecker(reg, probe, fastConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 第1次失败：healthy -> probing，立即不可路由
	failFlag.set(true)
	d, err := c.CheckOnce(ctx, "a")
	if err == nil {
		t.Fatal("probe should fail")
	}
	if d.NewState != discovery.StateProbing {
		t.Fatalf("want probing, got %s (%s)", d.NewState, d.Reason)
	}
	got, _ := reg.Get("a")
	if got.Routable() {
		t.Fatal("probing instance must not be routable")
	}
	t.Logf("decision: %s | routable=false", d.String())

	// 再失败两次 -> unhealthy 摘除
	for i := 0; i < 2; i++ {
		d, _ = c.CheckOnce(ctx, "a")
	}
	if d.NewState != discovery.StateUnhealthy {
		t.Fatalf("want unhealthy, got %s (%s)", d.NewState, d.Reason)
	}
	t.Logf("decision: %s", d.String())

	// 恢复：成功 1 次 -> probing，仍不可路由；第 2 次 -> healthy
	failFlag.set(false)
	d, _ = c.CheckOnce(ctx, "a")
	if d.NewState != discovery.StateProbing {
		t.Fatalf("recover step1 want probing, got %s (%s)", d.NewState, d.Reason)
	}
	got, _ = reg.Get("a")
	if got.Routable() {
		t.Fatal("recovering instance must not be routable yet")
	}
	d, _ = c.CheckOnce(ctx, "a")
	if d.NewState != discovery.StateHealthy {
		t.Fatalf("recover step2 want healthy, got %s (%s)", d.NewState, d.Reason)
	}
	t.Logf("decision: %s", d.String())
}

func TestProbeJitterStaysRemovedUntilStable(t *testing.T) {
	reg := discovery.NewRegistry()
	_ = reg.Register(discovery.NewInstance("j", "svc", "addr", nil))
	reg.SetState("j", discovery.StateHealthy)

	var n int
	probe := func(ctx context.Context, inst *discovery.Instance) error {
		n++
		// 抖动：失败，成功，失败
		if n == 1 || n == 3 {
			return errors.New("simulated jitter failure")
		}
		return nil
	}
	c := NewChecker(reg, probe, fastConfig())
	ctx := context.Background()

	c.CheckOnce(ctx, "j") // 1 fail -> probing
	c.CheckOnce(ctx, "j") // 2 ok -> probing (SuccessThreshold=2，恢复中)
	if got, _ := reg.Get("j"); got.Routable() {
		t.Fatal("single success after failure must not restore routing")
	}
	d, _ := c.CheckOnce(ctx, "j") // 3 fail -> 仍 probing/unhealthy，计数重置
	if got, _ := reg.Get("j"); got.Routable() {
		t.Fatalf("jitter must keep instance out of routing, state=%s", got.State())
	}
	t.Logf("jitter decision: %s", d.String())
}

func TestConcurrentDeregisterDuringProbe(t *testing.T) {
	reg := discovery.NewRegistry()
	_ = reg.Register(discovery.NewInstance("x", "svc", "addr", nil))
	reg.SetState("x", discovery.StateHealthy)

	release := make(chan struct{})
	probe := func(ctx context.Context, inst *discovery.Instance) error {
		select {
		case <-release:
			return nil // 探测在注销完成后才“成功”返回
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c := NewChecker(reg, probe, Config{
		Timeout: 2 * time.Second, FailureThreshold: 3, SuccessThreshold: 2,
	})

	done := make(chan Decision, 1)
	go func() {
		d, _ := c.CheckOnce(context.Background(), "x")
		done <- d
	}()

	time.Sleep(20 * time.Millisecond)
	if err := reg.Deregister("x"); err != nil {
		t.Fatal(err)
	}
	close(release) // 探测随后返回成功

	d := <-done
	got, _ := reg.Get("x")
	if got.State() != discovery.StateDeregistered {
		t.Fatalf("late probe success must not resurrect instance, state=%s", got.State())
	}
	t.Logf("concurrent deregister: %s final-state=%s", d.String(), got.State())
}

func TestTransportFailureMovesOutOfHealthy(t *testing.T) {
	reg := discovery.NewRegistry()
	_ = reg.Register(discovery.NewInstance("tf", "svc", "addr", nil))
	reg.SetState("tf", discovery.StateHealthy)
	c := NewChecker(reg, func(ctx context.Context, inst *discovery.Instance) error { return nil }, fastConfig())

	c.ReportTransportFailure("tf")
	got, _ := reg.Get("tf")
	if got.Routable() {
		t.Fatalf("transport failure must remove from routing, state=%s", got.State())
	}
	// 反复失败直至阈值 -> unhealthy
	c.ReportTransportFailure("tf")
	c.ReportTransportFailure("tf")
	got, _ = reg.Get("tf")
	if got.State() != discovery.StateUnhealthy {
		t.Fatalf("want unhealthy, got %s", got.State())
	}
	// 对已注销实例的反馈不得改变其状态
	reg.Register(discovery.NewInstance("gone", "svc", "a", nil))
	reg.SetState("gone", discovery.StateHealthy)
	reg.Deregister("gone")
	c.ReportTransportFailure("gone")
	g, _ := reg.Get("gone")
	if g.State() != discovery.StateDeregistered {
		t.Fatalf("deregistered must stay deregistered, got %s", g.State())
	}
	t.Logf("transport-failure converged: tf=%s failCount=%d; gone stays deregistered",
		got.State(), got.FailCount())
}

// ---- 极小原子布尔（避免引入 sync/atomic 别名噪音）----
type atomicBool struct{ v int32 }

func (a *atomicBool) set(b bool) {
	if b {
		storeInt32(&a.v, 1)
	} else {
		storeInt32(&a.v, 0)
	}
}
func (a *atomicBool) get() bool { return loadInt32(&a.v) == 1 }
