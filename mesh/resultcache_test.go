package mesh_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/health"
	"github.com/highcumontoa/service-discovery-lb-go/mesh"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// 业务：结果复用与失败收敛
// 用例 A：整套后端都不可用（全部 UNKNOWN）时，同一个业务键的一批并发请求
// 必须在有限时间内全部拿到同一份 no_healthy_instance 失败结论，不挂起、不报错成别的类别。
func TestNoHealthySameKeyConvergesBounded(t *testing.T) {
	m, bes := newMeshWithBackends(t, 2, 1)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	// 不跑 markHealthy：实例全部 UNKNOWN，整套后端不可路由。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	kinds := make(chan model.ErrorKind, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := m.Call(ctx, model.RequestContext{
				Service: "echo", Method: "POST", Path: "/job", IdempotencyKey: "no-inst-key",
			})
			kinds <- model.ErrKind(err)
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("无健康实例时同键请求必须在有限时间内全部返回，出现长时间挂起")
	}
	close(kinds)
	for k := range kinds {
		if k != model.ErrNoHealthyInstance {
			t.Fatalf("所有同键调用必须收敛到 no_healthy_instance, got %s", k)
		}
	}
	t.Logf("[结果复用/无健康实例] 业务=echo 用例=同键 %d 并发（整套后端 UNKNOWN）结果=全部在有限时间内拿到 no_healthy_instance（依据：%s）",
		n, model.DescribeKind(model.ErrNoHealthyInstance))

	// 过一会儿同键重放：仍无健康实例，依旧快速失败且类别不变。
	start2 := time.Now()
	_, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "POST", Path: "/job", IdempotencyKey: "no-inst-key",
	})
	if model.ErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("重放必须复用同一份失败结论, got %v", err)
	}
	if d := time.Since(start2); d > time.Second {
		t.Fatalf("重放复用结论应立即返回, 耗时 %v", d)
	}
	t.Logf("[结果复用/无健康实例] 用例=稍后同键重放 结果=%s 耗时=%v（复用失败结论，不等待、不重试）",
		model.ErrKind(err), time.Since(start2).Round(time.Millisecond))

	// 等默认保留窗口过去后，实例仍不健康 -> 当作全新请求，依旧快速明确失败，不卡住。
	// （默认 TTL 30s，这里不等待；改为直接用全新键证明“全新请求”路径同样有界。）
	ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
	defer cancel3()
	_, err = m.Call(ctx3, model.RequestContext{
		Service: "echo", Method: "POST", Path: "/job", IdempotencyKey: fmt.Sprintf("fresh-%d", time.Now().UnixNano()),
	})
	if model.ErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("全新键在无健康实例时也必须明确失败, got %v", err)
	}
	t.Logf("[结果复用/无健康实例] 用例=全新键请求 结果=%s（全新请求路径同样有界明确失败）", model.ErrKind(err))
}

// 用例 B：短保留窗口下，失败结论过期后同键再来时作为全新请求执行；
// 后端（健康集合）在此期间恢复 -> 请求成功。证明淘汰后不会卡住或报错。
func TestCachedFailureExpiresThenFreshSuccess(t *testing.T) {
	// 短保留窗口 mesh：先无健康实例失败，再标记健康，等 TTL 后同键成功。
	short, bes2 := newMeshWithBackendsTTL(t, 1, 1, 80*time.Millisecond, 128)
	defer func() {
		for _, be := range bes2 {
			be.Close()
		}
	}()

	// 全部 UNKNOWN：同键第一次拿到失败结论。
	_, err := short.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/x", IdempotencyKey: "ttl-key",
	})
	if model.ErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("起始应无健康实例, got %v", err)
	}
	// 等过保留窗口，并让实例变健康。
	markHealthy(t, short, bes2)
	time.Sleep(120 * time.Millisecond)
	res, err := short.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/x", IdempotencyKey: "ttl-key",
	})
	if err != nil || res.DedupeHit || res.InstanceID != bes2[0].ID {
		t.Fatalf("TTL 过期 + 后端恢复后，同键请求必须作为全新请求成功, res=%+v err=%v", res, err)
	}
	if bes2[0].Calls() != 1 {
		t.Fatalf("恢复后后端应恰好处理 1 次, got %d", bes2[0].Calls())
	}
	t.Logf("[结果复用/淘汰] 业务=echo 用例=失败缓存 80ms 过期后实例恢复 结果=同键全新请求成功，后端处理 1 次，不卡不错")
}

// newMeshWithBackendsTTL 与 newMeshWithBackends 相同，但可指定结论保留窗口与容量。
func newMeshWithBackendsTTL(t *testing.T, n, attempts int, ttl time.Duration, maxEntries int) (*mesh.Mesh, []*backend.Instance) {
	t.Helper()
	probe := health.NewHTTPProbe(300 * time.Millisecond).Func()
	m, err := mesh.New(mesh.Config{
		FailThreshold: 2, RecoverThreshold: 2,
		Probe: probe, ProbeInterval: 50 * time.Millisecond,
		InitialRules:     rulesFor("echo", attempts, 400*time.Millisecond),
		DedupeTTL:        ttl,
		DedupeMaxEntries: maxEntries,
		DedupeWaitBudget: 2 * time.Second,
		Logf:             testLogf(t),
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

// 用例 C：结论总条数超过容量上限时，最旧结论被淘汰；被淘汰键再来时作为全新请求执行。
func TestResultCacheCapacityEvictionAtMesh(t *testing.T) {
	m, bes := newMeshWithBackendsTTL(t, 1, 1, time.Hour, 3)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)

	keys := []string{"cap-a", "cap-b", "cap-c", "cap-d", "cap-e"}
	for i, k := range keys {
		res, err := m.Call(context.Background(), model.RequestContext{
			Service: "echo", Method: "GET", Path: "/c", IdempotencyKey: k,
		})
		if err != nil {
			t.Fatalf("第 %d 个键应成功, got %v", i, err)
		}
		if res.DedupeHit {
			t.Fatalf("全新键 %s 不应命中缓存", k)
		}
	}
	// 容量 3：cap-a、cap-b 应已被淘汰；cap-c/d/e 仍可复用。
	for _, k := range []string{"cap-c", "cap-d", "cap-e"} {
		res, err := m.Call(context.Background(), model.RequestContext{
			Service: "echo", Method: "GET", Path: "/c", IdempotencyKey: k,
		})
		if err != nil || !res.DedupeHit {
			t.Fatalf("未淘汰的 %s 必须复用结论, res=%+v err=%v", k, res, err)
		}
	}
	callsBefore := bes[0].Calls()
	// 被淘汰的 cap-a 再来：作为全新请求真正打一次后端。
	res, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/c", IdempotencyKey: "cap-a",
	})
	if err != nil || res.DedupeHit {
		t.Fatalf("被淘汰键再来必须作为全新请求, res=%+v err=%v", res, err)
	}
	if bes[0].Calls() != callsBefore+1 {
		t.Fatalf("淘汰后重放必须真正再打一次后端")
	}
	t.Logf("[结果复用/容量] 业务=echo 用例=容量 3 发布 5 个结论 结果=最旧 a/b 被 FIFO 淘汰，c/d/e 复用，a 重放全新执行（后端计数 +1）")
}
