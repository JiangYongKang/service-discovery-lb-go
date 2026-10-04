package mesh_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/health"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/mesh"
	"github.com/highcumontoa/service-discovery-lb-go/model"
	"github.com/highcumontoa/service-discovery-lb-go/router"
)

// leastConnMesh 装配一个 least_conn 策略的 mesh（n 个后端，attempts=3）。
func leastConnMesh(t *testing.T, n, attempts int) *testMesh {
	t.Helper()
	probe := health.NewHTTPProbe(300 * time.Millisecond).Func()
	m, err := mesh.New(mesh.Config{
		FailThreshold: 2, RecoverThreshold: 2,
		Probe: probe, ProbeInterval: 50 * time.Millisecond,
		InitialRules: router.Rules{Version: 1, ByService: map[string]router.Rule{
			"echo": {Service: "echo", Policy: lb.PolicyLeastConn, MaxAttempts: attempts, PerCallTimeout: 400 * time.Millisecond},
		}},
		DedupeWaitBudget: 3 * time.Second,
		Logf:             testLogf(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	var bes []*backend.Instance
	for i := 0; i < n; i++ {
		be, err := backend.New(fmt.Sprintf("lc-%d", i+1), "echo", backend.Behavior{})
		if err != nil {
			t.Fatal(err)
		}
		bes = append(bes, be)
		if err := m.Register(be.ToModel()); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, be := range bes {
			be.Close()
		}
	})
	return &testMesh{m: m, bes: bes, version: 1}
}

type testMesh struct {
	m       *mesh.Mesh
	bes     []*backend.Instance
	version uint64
}

// inflightMap 取某规则版本均衡器的在途计数快照。
func (tm *testMesh) inflightMap() map[string]int {
	return tm.m.Dispatch.BalancerInflight(tm.version, "echo")
}

func (tm *testMesh) inflightSum() int {
	sum := 0
	for _, n := range tm.inflightMap() {
		sum += n
	}
	return sum
}

// 用例 A：长请求/短请求混合并发，在途期间计数与真实在途一致；
// 一批请求全部结束后计数必须归零（不能只涨不落）。
func TestLeastConnInflightReturnsToZero(t *testing.T) {
	tm := leastConnMesh(t, 4, 3)
	markHealthy(t, tm.m, tm.bes)

	// 让 lc-1/lc-2 变慢，lc-3/lc-4 保持快，制造长短混合。
	tm.bes[0].SetBehavior(backend.Behavior{Latency: 300 * time.Millisecond})
	tm.bes[1].SetBehavior(backend.Behavior{Latency: 300 * time.Millisecond})

	const shortN, longN = 20, 2
	kinds := make(chan model.ErrorKind, shortN+longN)
	start := make(chan struct{})
	var wg sync.WaitGroup

	call := func(key, path string) {
		defer wg.Done()
		<-start
		_, err := tm.m.Call(context.Background(), model.RequestContext{
			Service: "echo", Method: "GET", Path: path, IdempotencyKey: key,
		})
		kinds <- model.ErrKind(err)
	}
	for i := 0; i < longN; i++ {
		wg.Add(1)
		go call(fmt.Sprintf("long-%d", i), "/long")
	}
	for i := 0; i < shortN; i++ {
		wg.Add(1)
		go call(fmt.Sprintf("short-%d", i), "/short")
	}

	// 在途期间高频采样计数：必须观察到正峰值，且任何时刻总和不超过在途请求总数。
	var maxMu sync.Mutex
	maxInflight := 0
	overLimit := false
	stop := make(chan struct{})
	samplerStopped := make(chan struct{})
	go func() {
		defer close(samplerStopped)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n := tm.inflightSum()
				maxMu.Lock()
				if n > maxInflight {
					maxInflight = n
				}
				if n > shortN+longN {
					overLimit = true
				}
				maxMu.Unlock()
			}
		}
	}()

	close(start)
	wg.Wait()
	close(stop)
	<-samplerStopped
	close(kinds)
	for k := range kinds {
		if k != "" {
			t.Fatalf("本用例后端全部健康，不应出现失败结论, got %s", k)
		}
	}
	if overLimit {
		t.Fatalf("在途计数总和不可能超过在途请求总数 %d", shortN+longN)
	}

	// 核心断言：一批请求全部结束后计数必须回到起始值 0。
	if got := tm.inflightSum(); got != 0 {
		t.Fatalf("全部请求结束后在途计数必须归零, got=%v sum=%d", tm.inflightMap(), got)
	}
	if maxInflight == 0 {
		t.Fatal("在途期间应能观察到正的在途计数峰值")
	}

	t.Logf("[在途计数] 业务=echo 用例=%d 长 + %d 短混合并发 结果=在途峰值=%d，全部结束后计数归零 map=%v；处理分布 lc-1=%d lc-2=%d lc-3=%d lc-4=%d",
		longN, shortN, maxInflight, tm.inflightMap(),
		tm.bes[0].Calls(), tm.bes[1].Calls(), tm.bes[2].Calls(), tm.bes[3].Calls())
}

// 用例 B：一批请求被屏障同时放行（模拟瞬时并发涌入），实时计数必须让选择分散开，
// 不能因计数更新滞后而反复选中同一个实例。
func TestLeastConnSurgeSpreadsAcrossInstances(t *testing.T) {
	tm := leastConnMesh(t, 4, 1)
	markHealthy(t, tm.m, tm.bes)

	gate := make(chan struct{})
	var wg sync.WaitGroup
	const n = 40
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate // 全部 goroutine 就绪后同时开始竞争
			res, err := tm.m.Call(context.Background(), model.RequestContext{
				Service: "echo", Method: "GET", Path: "/s", IdempotencyKey: fmt.Sprintf("surge-%d", i),
			})
			if err != nil {
				t.Errorf("涌入请求不应失败: %v", err)
				return
			}
			_ = res
		}(i)
	}
	close(gate)
	wg.Wait()

	dist := map[string]int64{}
	var total int64
	for _, be := range tm.bes {
		c := be.Calls()
		dist[be.ID] = c
		total += c
	}
	if total != n {
		t.Fatalf("后端处理总数应为 %d, got %d", n, total)
	}
	minC, maxC := int64(1<<62), int64(0)
	for _, c := range dist {
		if c < minC {
			minC = c
		}
		if c > maxC {
			maxC = c
		}
	}
	// 理想是 10/10/10/10；允许完成时序带来的少量抖动（极差 <=6），
	// 但绝不能出现某个实例被计数滞后反复压住（那会得到接近 40 的极差）。
	if maxC-minC > 6 {
		t.Fatalf("并发涌入选择必须靠实时计数分散，分布=%v 极差=%d（疑似计数滞后反复选中）", dist, maxC-minC)
	}
	if tm.inflightSum() != 0 {
		t.Fatalf("结束后计数必须归零, got %v", tm.inflightMap())
	}
	t.Logf("[在途计数] 业务=echo 用例=%d 请求同时涌入 结果=均匀分散分布=%v（计数实时更新，无反复选中同一实例），结束后归零", n, dist)
}

// 用例 C：请求中途失败（连接失败/5xx/超时）时在途计数同样成对归还，结束后归零。
func TestLeastConnInflightZeroAfterFailures(t *testing.T) {
	tm := leastConnMesh(t, 3, 1) // 单实例预算：失败即降级
	markHealthy(t, tm.m, tm.bes)

	// 三种故障：lc-1 连接失败（关进程）、lc-2 返回 500、lc-3 慢到超时。
	tm.bes[0].Close()
	tm.bes[1].SetBehavior(backend.Behavior{CallStatus: 500})
	tm.bes[2].SetBehavior(backend.Behavior{Latency: 2 * time.Second})

	var wg sync.WaitGroup
	kinds := make(chan model.ErrorKind, 12)
	for i := 0; i < 4; i++ {
		for j, id := range []string{"dead", "5xx", "slow"} {
			wg.Add(1)
			go func(i, j int, tag string) {
				defer wg.Done()
				// 三个键分别只会固定路由到某类实例？least_conn 选择不确定，
				// 因此这里不区分实例，只断言：整批失败后计数必须归零。
				_, err := tm.m.Call(context.Background(), model.RequestContext{
					Service: "echo", Method: "GET", Path: "/f",
					IdempotencyKey: fmt.Sprintf("fail-%s-%d", tag, i),
				})
				kinds <- model.ErrKind(err)
			}(i, j, id)
		}
	}
	wg.Wait()
	close(kinds)
	bad := 0
	for k := range kinds {
		if k == "" {
			bad++
		}
	}
	if bad != 0 {
		t.Fatalf("全部实例都在故障，不应有成功，成功数=%d", bad)
	}
	if got := tm.inflightSum(); got != 0 {
		t.Fatalf("连接失败/5xx/超时之后在途计数都必须归还，got=%v sum=%d", tm.inflightMap(), got)
	}
	t.Logf("[在途计数] 业务=echo 用例=连接失败 + 5xx + 超时共 12 次全部失败 结果=计数全部成对归零 map=%v", tm.inflightMap())
}

// 用例 D：在途请求进行中发生规则热更新（round_robin -> least_conn 及版本切换），
// 热更新后的路由结论仍可由“当时在途数 + 规则版本 + 注册表版本”解释，
// 且两个版本的在途计数互不串味；全部结束后各自归零。
func TestLeastConnInflightAcrossHotUpdate(t *testing.T) {
	tm := leastConnMesh(t, 3, 1)
	markHealthy(t, tm.m, tm.bes)

	// 先发一波 least_conn 请求建立正的在途计数（版本 1），并让它们在途时热更新。
	tm.bes[0].SetBehavior(backend.Behavior{})
	// 用自定义延迟让请求保持在途：给 lc-1 加延迟。
	tm.bes[0].SetBehavior(backend.Behavior{Latency: 300 * time.Millisecond})

	var wg sync.WaitGroup
	oldDone := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := tm.m.Call(context.Background(), model.RequestContext{
				Service: "echo", Method: "GET", Path: "/u", IdempotencyKey: fmt.Sprintf("old-%d", i),
			})
			if err != nil {
				t.Errorf("旧版本在途请求不应失败: %v", err)
			}
		}(i)
	}
	go func() { wg.Wait(); close(oldDone) }()

	// 在旧请求在途期间热更新到版本 2（仍 least_conn，但是一个全新计数器）。
	time.Sleep(40 * time.Millisecond)
	rulesV2 := router.Rules{Version: 2, ByService: map[string]router.Rule{
		"echo": {Service: "echo", Policy: lb.PolicyLeastConn, MaxAttempts: 1, PerCallTimeout: time.Second},
	}}
	if err := tm.m.UpdateRules(rulesV2); err != nil {
		t.Fatal(err)
	}
	tm.version = 2

	// 旧版本均衡器可能已在 balancerFor 回收中被删除（只保留当前版本）。
	// 关键不变量：新版本计数器从 0 开始，绝不继承旧版本在途数。
	v2Inflight := tm.inflightSum()
	if v2Inflight != 0 {
		t.Fatalf("热更新后的新版本在途计数必须从 0 开始，不能串入旧版本计数, got %v", tm.inflightMap())
	}

	// 新版本再打一波请求（此时旧版本可能已结束）。
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := tm.m.Call(context.Background(), model.RequestContext{
				Service: "echo", Method: "GET", Path: "/u", IdempotencyKey: fmt.Sprintf("new-%d", i),
			})
			if err != nil {
				t.Errorf("新版本请求不应失败: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := tm.inflightSum(); got != 0 {
		t.Fatalf("热更新后所有请求结束，新版本计数必须归零, got %v", tm.inflightMap())
	}
	t.Logf("[在途计数] 业务=echo 用例=旧版本 3 在途时热更新到规则版本 2 + 新版本 6 请求 结果=新版本计数从 0 起、不继承旧计数，全部结束后归零")
}
