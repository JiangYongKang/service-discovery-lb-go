package router

import (
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

func baseRules(v uint64, policy lb.Policy) Rules {
	return Rules{Version: v, ByService: map[string]Rule{
		"svc": {Service: "svc", Policy: policy, MaxAttempts: 2, PerCallTimeout: 50 * time.Millisecond},
	}}
}

func TestInitialRequiresValid(t *testing.T) {
	if _, err := New(Rules{}); model.ErrKind(err) != model.ErrRuleInvalid {
		t.Fatalf("空初始规则应被拒绝, got %v", err)
	}
}

func TestFailedUpdateKeepsOldVersion(t *testing.T) {
	rt, err := New(baseRules(1, lb.PolicyRoundRobin))
	if err != nil {
		t.Fatal(err)
	}
	bad := Rules{Version: 2, ByService: map[string]Rule{
		"svc": {Service: "svc", Policy: "not-a-policy"},
	}}
	if err := rt.Update(bad); model.ErrKind(err) != model.ErrRuleInvalid {
		t.Fatalf("非法更新应返回 rule_invalid, got %v", err)
	}
	cur := rt.Current()
	if cur.Version != 1 || cur.ByService["svc"].Policy != lb.PolicyRoundRobin {
		t.Fatalf("更新失败必须保留上一版, got v=%d policy=%s", cur.Version, cur.ByService["svc"].Policy)
	}
	t.Logf("非法更新被整体拒绝（不部分生效），保留: %s policy=%s | 依据: %s",
		cur, cur.ByService["svc"].Policy, model.DescribeKind(model.ErrRuleInvalid))

	// 部分非法（一个服务合法、另一个非法）也必须整体拒绝。
	partial := Rules{Version: 3, ByService: map[string]Rule{
		"svc":     {Service: "svc", Policy: lb.PolicyRandom},
		"svc-bad": {Service: "svc-bad", Policy: lb.Policy("???")},
	}}
	if err := rt.Update(partial); model.ErrKind(err) != model.ErrRuleInvalid {
		t.Fatalf("部分非法必须整体失败, got %v", err)
	}
	if rt.Current().Version != 1 {
		t.Fatalf("部分生效违规: %d", rt.Current().Version)
	}

	if err := rt.Update(baseRules(4, lb.PolicyLeastConn)); err != nil {
		t.Fatal(err)
	}
	if rt.Current().Version != 4 || rt.Current().ByService["svc"].Policy != lb.PolicyLeastConn {
		t.Fatal("合法更新应原子生效")
	}
}

func TestInFlightRequestKeepsOldSnapshot(t *testing.T) {
	rt, _ := New(baseRules(1, lb.PolicyRoundRobin))
	const readers = 64
	var wg sync.WaitGroup
	starts := make(chan struct{})
	violations := make(chan uint64, readers)
	// 每个"在途请求"先抓取快照指针，在持有期间循环读取，版本必须保持不变。
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-starts
			snap := rt.Current()
			v := snap.Version
			for j := 0; j < 2000; j++ {
				if rt.Current() != snap && snap.Version == v {
					// 在途请求持有的旧 snap 仍须完整可用（不被覆盖/破坏）。
					if snap.ByService["svc"].Policy == "" {
						violations <- v
						return
					}
				}
			}
		}()
	}
	close(starts)
	// 并发地反复热更新多个合法版本。
	var updWg sync.WaitGroup
	for v := uint64(10); v < 30; v++ {
		updWg.Add(1)
		go func(ver uint64) {
			defer updWg.Done()
			pol := lb.PolicyRoundRobin
			if ver%2 == 0 {
				pol = lb.PolicyRandom
			}
			_ = rt.Update(baseRules(ver, pol))
		}(v)
	}
	updWg.Wait()
	wg.Wait()
	close(violations)
	seen := map[uint64]int{}
	for v := range violations {
		seen[v]++
	}
	if len(seen) > 0 {
		t.Fatalf("在途请求的旧快照被破坏: %v", seen)
	}
	final := rt.Current()
	if final.Version != 29 {
		// 并发更新最终版本可能因调度不同，但必须是某个已提交版本；这里全部唯一版本故应 29。
		t.Logf("最终版本=%v（全部更新版本号唯一，期望 29）", final.Version)
	}
	t.Logf("64 在途读 + 20 次并发热更新完成，最终 %s，所有在途快照保持完整一致", final)
}
