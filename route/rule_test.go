package route

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/lb"
)

func TestPublishAtomicAndRollback(t *testing.T) {
	tbl := NewTable()
	v1, err := tbl.Publish([]*Rule{{
		Service: "svc", Strategy: lb.StrategyRandom,
		Timeout: 100 * time.Millisecond, MaxAttempts: 2,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if v1.Version != 1 {
		t.Fatalf("want version 1, got %d", v1.Version)
	}
	r := v1.Lookup("svc")
	if r.Strategy != lb.StrategyRandom || r.MaxAttempts != 2 {
		t.Fatalf("v1 rule mismatch: %+v", r)
	}
	t.Logf("v1 published: strategy=%s maxAttempts=%d", r.Strategy, r.MaxAttempts)

	// 非法整批更新：必须失败且保留 v1
	_, err = tbl.Publish([]*Rule{
		{Service: "ok", Strategy: lb.StrategyRoundRobin},
		{Service: "", Strategy: lb.StrategyRoundRobin}, // 非法
	})
	if !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("want ErrInvalidRule, got %v", err)
	}
	cur := tbl.Current()
	if cur.Version != 1 {
		t.Fatalf("failed publish must not bump version, got %d", cur.Version)
	}
	if cur.Lookup("svc").Strategy != lb.StrategyRandom {
		t.Fatal("previous rules must remain effective after failed publish")
	}
	t.Logf("invalid publish rejected: current stays v%d, svc strategy=%s",
		cur.Version, cur.Lookup("svc").Strategy)

	// 空规则、重复服务、未知策略、负权重分别被拒绝
	for i, bad := range [][]*Rule{
		nil,
		{{Service: "svc"}, {Service: "svc"}},
		{{Service: "svc", Strategy: "nope"}},
		{{Service: "svc", Weights: map[string]int{"a": -1}}},
	} {
		if _, err := tbl.Publish(bad); err == nil {
			t.Fatalf("case %d must be rejected", i)
		}
	}
	if tbl.Current().Version != 1 {
		t.Fatalf("version must remain 1 after all bad publishes, got %d", tbl.Current().Version)
	}

	// 合法更新原子生效
	v2, err := tbl.Publish([]*Rule{{
		Service: "svc", Strategy: lb.StrategyConsistent, MaxAttempts: 3,
		Timeout: 80 * time.Millisecond,
	}})
	if err != nil || v2.Version != 2 {
		t.Fatalf("v2 publish: v=%d err=%v", v2.Version, err)
	}
	t.Logf("v2 published: strategy=%s maxAttempts=%d", v2.Lookup("svc").Strategy, v2.Lookup("svc").MaxAttempts)
}

func TestInFlightRequestKeepsOldRuleDuringHotUpdate(t *testing.T) {
	tbl := NewTable()
	mgr := NewManager(tbl)
	_, _ = mgr.Update([]*Rule{{
		Service: "svc", Strategy: lb.StrategyRoundRobin,
		Timeout: time.Second, MaxAttempts: 1,
	}})

	var wg sync.WaitGroup
	const workers = 16
	const rounds = 100
	strategiesSeen := map[lb.Strategy]int{}
	var mu sync.Mutex

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for k := 0; k < rounds; k++ {
				// 在途请求：先拿不可变版本指针，之后“处理期间”即使热更新也不变
				snap := tbl.Current()
				rule := snap.Lookup("svc")
				st := rule.Strategy
				ver := snap.Version
				// 模拟在途处理的一小段时间
				time.Sleep(time.Microsecond)
				if snap.Lookup("svc").Strategy != st {
					t.Errorf("in-flight rule changed mid-request!")
					return
				}
				if snap.Version != ver {
					t.Errorf("snapshot version changed!")
					return
				}
				mu.Lock()
				strategiesSeen[st]++
				mu.Unlock()
			}
		}(w)
	}

	// 热更新循环：交替发布两个合法版本
	updates := []lb.Strategy{lb.StrategyRandom, lb.StrategyWeighted, lb.StrategyConsistent}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			_, err := mgr.Update([]*Rule{{
				Service: "svc", Strategy: updates[i%len(updates)],
				Timeout: time.Second, MaxAttempts: 1,
			}})
			if err != nil {
				t.Errorf("update: %v", err)
				return
			}
			// 也插入非法更新，确认永远不会部分生效
			_, _ = mgr.Update([]*Rule{{Service: ""}})
			time.Sleep(50 * time.Microsecond)
		}
	}()
	wg.Wait()

	t.Logf("strategies observed by in-flight requests across %d requests: %v",
		workers*rounds, strategiesSeen)
	if len(strategiesSeen) == 0 {
		t.Fatal("no requests observed")
	}
	// 最终版本规则必须是一个完整合法版本
	cur := tbl.Current()
	if cur.Lookup("svc") == nil || cur.Lookup("svc").Service != "svc" {
		t.Fatal("final rule table corrupt")
	}
}

func TestResolveFallbackRule(t *testing.T) {
	tbl := NewTable()
	r, ver := tbl.Resolve("unknown")
	if r == nil || r.Strategy != lb.StrategyRoundRobin || ver != 0 {
		t.Fatalf("fallback rule mismatch: %+v ver=%d", r, ver)
	}
}
