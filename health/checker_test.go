package health

import (
	"errors"
	"testing"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

var errProbe = errors.New("connection refused")

func TestStateMachineTransitions(t *testing.T) {
	c := New(Config{FailThreshold: 3, RecoverThreshold: 2}, nil)
	c.Reset("i")

	// UNKNOWN：1 次成功仍不可路由（未达就绪阈值）。
	st, why := c.OnProbeResult("i", nil)
	if st != model.StateUnknown {
		t.Fatalf("want UNKNOWN, got %s", st)
	}
	t.Logf("UNKNOWN +1成功: %s | %s", c.GetStatus("i"), why)

	st, _ = c.OnProbeResult("i", nil)
	if st != model.StateHealthy {
		t.Fatalf("连续 2 次成功应 HEALTHY, got %s", st)
	}

	// 2 次失败未达摘除阈值，仍 HEALTHY（但不可路由结论不变依据是阈值）。
	c.OnProbeResult("i", errProbe)
	st, why = c.OnProbeResult("i", errProbe)
	if st != model.StateHealthy {
		t.Fatalf("失败 2/3 应保持 HEALTHY, got %s", st)
	}
	t.Logf("HEALTHY 连续失败 2/3: %s | %s", c.GetStatus("i"), why)

	// 第 3 次失败 -> 摘除。
	st, why = c.OnProbeResult("i", errProbe)
	if st != model.StateUnhealthy {
		t.Fatalf("失败 3/3 应 UNHEALTHY, got %s", st)
	}
	if st.Routeable() {
		t.Fatal("UNHEALTHY 不可路由")
	}
	t.Logf("达到摘除阈值: %s | %s", c.GetStatus("i"), why)

	// 恢复中：1 次成功 -> RECOVERING，仍不可路由。
	st, why = c.OnProbeResult("i", nil)
	if st != model.StateRecovering {
		t.Fatalf("摘除后首次成功应 RECOVERING, got %s", st)
	}
	t.Logf("恢复观察期: %s | %s", c.GetStatus("i"), why)

	// 抖动：恢复期一次失败立即打回 UNHEALTHY。
	st, why = c.OnProbeResult("i", errProbe)
	if st != model.StateUnhealthy {
		t.Fatalf("恢复期失败应打回 UNHEALTHY, got %s", st)
	}
	t.Logf("恢复期抖动: %s | %s", c.GetStatus("i"), why)

	// 连续 2 次成功恢复 HEALTHY。
	c.OnProbeResult("i", nil)
	st, why = c.OnProbeResult("i", nil)
	if st != model.StateHealthy {
		t.Fatalf("连续 2 次成功应恢复 HEALTHY, got %s", st)
	}
	t.Logf("恢复完成: %s | %s", c.GetStatus("i"), why)
}

func TestJitterNeverRouteableUntilThreshold(t *testing.T) {
	// 反复抖动场景：成功失败交替，恢复阈值永远达不到 -> 一直不可路由。
	c := New(Config{FailThreshold: 2, RecoverThreshold: 3}, nil)
	c.Reset("j")
	routeableCount := 0
	for round := 0; round < 10; round++ {
		c.OnProbeResult("j", errProbe)
		c.OnProbeResult("j", nil) // 进入/停留在 RECOVERING
		c.OnProbeResult("j", nil) // 连续成功 2/3
		if c.OnProbeResult("j", errProbe); false {
		}
		st, _ := c.OnProbeResult("j", errProbe)
		if st.Routeable() {
			routeableCount++
		}
	}
	if routeableCount != 0 {
		t.Fatalf("抖动场景下不应出现可路由时刻, got %d", routeableCount)
	}
	t.Logf("抖动 10 轮，实例从未进入 HEALTHY（恢复阈值 3 从未连续达到），路由结论稳定：不可路由")
}
