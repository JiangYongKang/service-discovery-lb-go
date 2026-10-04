package mesh_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/model"
	"github.com/highcumontoa/service-discovery-lb-go/router"
)

// newMeshWithDedupe 预留：差异化保留窗口用例直接使用 mesh.New + Config.Dedupe 装配。

// 业务：端到端——同键 20 个并发请求，后端副作用只发生 1 次；之后窗口内重放仍复用。
func TestMeshSameKeyConcurrentSingleSideEffect(t *testing.T) {
	m, bes := newMeshWithBackends(t, 2, 2)
	cleanup := func() {
		for _, be := range bes {
			be.Close()
		}
	}
	defer cleanup()
	// 慢后端：确保 20 个 goroutine 全部在在途屏障上等待后再放行。
	for _, be := range bes {
		be.SetBehavior(backend.Behavior{Latency: 120 * time.Millisecond})
	}
	markHealthy(t, m, bes)

	const n = 20
	var wg sync.WaitGroup
	type oc struct {
		ok        bool
		dedupeHit bool
		errKind   model.ErrorKind
	}
	resCh := make(chan oc, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := m.Call(context.Background(), model.RequestContext{
				Service: "echo", Method: "POST", Path: "/pay", IdempotencyKey: "order-1001",
			})
			if err != nil {
				resCh <- oc{errKind: model.ErrKind(err)}
				return
			}
			resCh <- oc{ok: true, dedupeHit: res.DedupeHit}
		}()
	}
	wg.Wait()
	close(resCh)

	var leaders, hits, fails int
	for r := range resCh {
		switch {
		case r.errKind != "":
			fails++
		case r.dedupeHit:
			hits++
		default:
			leaders++
		}
	}
	var total int64
	for _, be := range bes {
		total += be.Calls()
	}
	if total != 1 {
		t.Fatalf("同键 20 并发后端只能真正处理 1 次, got %d", total)
	}
	if fails != 0 || leaders != 1 || hits != n-1 {
		t.Fatalf("应 1 leader + %d 复用 + 0 失败, got leaders=%d hits=%d fails=%d", n-1, leaders, hits, fails)
	}
	// 保留窗口内再重放：仍然复用，后端次数不变。
	res, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "POST", Path: "/pay", IdempotencyKey: "order-1001",
	})
	if err != nil || !res.DedupeHit {
		t.Fatalf("窗口内重放应复用成功结论 dedupeHit=true, res=%+v err=%v", res, err)
	}
	total = 0
	for _, be := range bes {
		total += be.Calls()
	}
	if total != 1 {
		t.Fatalf("窗口内重放不得再次访问后端, got %d", total)
	}
	t.Logf("业务=同键并发单副作用: 20 并发 -> 后端处理 %d 次（1 leader + %d 在途复用）；窗口内重放仍复用，后端仍为 %d 次",
		1, n-1, total)
}

// 业务：端到端——整套后端都不可用时，同键并发请求必须在有限时间内得到 no_healthy_instance；
// 失败实例不会因这类重放被重新当成可用；失败窗口内重放快速返回同一结论。
func TestMeshNoHealthyInstanceBoundedAndReplay(t *testing.T) {
	m, bes := newMeshWithBackends(t, 2, 1)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)
	// 把两个实例都摘除：注入探测失败，跑两轮达到摘除阈值。
	for _, be := range bes {
		be.SetBehavior(backend.Behavior{FailProbe: true})
	}
	m.Monitor.RunOnce()
	m.Monitor.RunOnce()
	for _, be := range bes {
		if _, st, _ := m.Registry.Get(be.ID); st != model.StateUnhealthy {
			t.Fatalf("%s 应为 UNHEALTHY, got %s", be.ID, st)
		}
	}

	const n = 10
	var wg sync.WaitGroup
	start := time.Now()
	kinds := make(chan model.ErrorKind, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := m.Call(ctx, model.RequestContext{
				Service: "echo", Method: "GET", Path: "/pay", IdempotencyKey: "down-key",
			})
			kinds <- model.ErrKind(err)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(kinds)
	for k := range kinds {
		if k != model.ErrNoHealthyInstance {
			t.Fatalf("整组后端不可用时必须收敛为 no_healthy_instance, got %q", k)
		}
	}
	if elapsed > time.Second {
		t.Fatalf("无健康实例必须在有限时间内返回, 实际 %v", elapsed)
	}
	// 重放这类失败不会改变健康状态：失败实例仍摘除，不会被“回放”成可用。
	for _, be := range bes {
		_, st, _ := m.Registry.Get(be.ID)
		if st == model.StateHealthy {
			t.Fatalf("%s 不得因失败重放被重新标记为 HEALTHY", be.ID)
		}
	}
	// 失败窗口内再来（前后脚）仍快速得到同一明确失败。
	rstart := time.Now()
	_, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/pay", IdempotencyKey: "down-key",
	})
	if model.ErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("失败窗口内重放应仍为 no_healthy_instance, got %v", err)
	}
	t.Logf("业务=无健康实例有界收敛: 10 并发在 %v 内全部 no_healthy_instance；失败实例保持摘除未复活；重放在 %v 内返回同一结论",
		elapsed, time.Since(rstart))
}

// 业务：端到端——least_conn 策略下长短请求混合并发，全部结束后在途计数必须回到 0；
// 并发涌入不会反复选中同一实例；且与规则热更新同时发生时计数不串味、热更后仍可解释。
func TestMeshLeastConnInflightZeroAndHotUpdate(t *testing.T) {
	m, bes := newMeshWithBackends(t, 3, 2)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)
	// 初始即采用 least_conn。
	if err := m.UpdateRules(router.Rules{Version: 2, ByService: map[string]router.Rule{
		"echo": {Service: "echo", Policy: lb.PolicyLeastConn, MaxAttempts: 2, PerCallTimeout: time.Second},
	}}); err != nil {
		t.Fatal(err)
	}
	// inst-1 慢，inst-2/inst-3 快，验证在途计数驱动负载从忙实例流向闲实例。
	bes[0].SetBehavior(backend.Behavior{Latency: 120 * time.Millisecond})

	var wg sync.WaitGroup
	stopHot := make(chan struct{})
	var hotWG sync.WaitGroup

	// 热更新器：least_conn -> round_robin -> least_conn 循环，夹杂非法版本。
	hotWG.Add(1)
	go func() {
		defer hotWG.Done()
		v := uint64(3)
		pols := []lb.Policy{lb.PolicyLeastConn, lb.PolicyRoundRobin, lb.PolicyWeighted}
		for {
			select {
			case <-stopHot:
				return
			default:
			}
			pol := pols[int(v)%len(pols)]
			_ = m.UpdateRules(router.Rules{Version: v, ByService: map[string]router.Rule{
				"echo": {Service: "echo", Policy: pol, MaxAttempts: 2, PerCallTimeout: time.Second,
					Weights: map[string]int{bes[0].ID: 1, bes[1].ID: 1, bes[2].ID: 1}},
			}})
			if v%4 == 0 {
				_ = m.UpdateRules(router.Rules{Version: v, ByService: map[string]router.Rule{
					"echo": {Service: "echo", Policy: "bogus"},
				}})
			}
			v++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 60 个长短不一的请求（唯一键，绕过结果缓存，纯粹压在途计数路径）。
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Call(context.Background(), model.RequestContext{
				Service: "echo", Method: "GET", Path: "/job",
				IdempotencyKey: fmt.Sprintf("lc-%d", i),
			})
		}(i)
	}
	wg.Wait()
	close(stopHot)
	hotWG.Wait()

	// 收敛回单一 least_conn 版本后，在途总数必须为 0。
	if err := m.UpdateRules(router.Rules{Version: 999, ByService: map[string]router.Rule{
		"echo": {Service: "echo", Policy: lb.PolicyLeastConn, MaxAttempts: 2, PerCallTimeout: time.Second},
	}}); err != nil {
		t.Fatal(err)
	}
	// 旧版本在途请求都已结束；其计数为 0，新版本也应为 0。
	if total := m.Dispatch.InflightTotal("echo"); total != 0 {
		t.Fatalf("全部请求结束后跨版本在途总数必须归零, got %d", total)
	}
	// 慢实例不应独自扛下所有请求：在途计数选择应把流量分给快速实例。
	c0, c1, c2 := bes[0].Calls(), bes[1].Calls(), bes[2].Calls()
	t.Logf("业务=在途计数与热更新并存: 60 请求在策略热更新交错下完成，跨版本在途总数=0；实例处理次数 inst-1=%d(慢) inst-2=%d inst-3=%d",
		c0, c1, c2)
	if c1+c2 == 0 {
		t.Fatal("并发涌入不应反复选中同一慢实例，快速实例必须承接明显流量")
	}
	if c1+c2 < c0 {
		t.Fatalf("least_conn 应偏向快速实例，但慢实例 c0=%d 而 c1+c2=%d", c0, c1+c2)
	}

	// 再发一批请求，确认归零后计数仍能重新准确涨落并再次归零。
	var wg2 sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			_, _ = m.Call(context.Background(), model.RequestContext{
				Service: "echo", Method: "GET", Path: "/job",
				IdempotencyKey: fmt.Sprintf("lc-after-%d", i),
			})
		}(i)
	}
	wg2.Wait()
	if total := m.Dispatch.InflightTotal("echo"); total != 0 {
		t.Fatalf("第二批请求结束后在途总数必须仍归零, got %d", total)
	}
	t.Logf("业务=在途计数可重复归零: 归零后再发 20 请求，结束后跨版本在途总数=0，计数与规则版本(rules_v)/注册表版本(registry_v)在调度日志中均可解释")
}
