package lb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

func mkIDs(ids ...string) []model.Instance {
	out := make([]model.Instance, 0, len(ids))
	for _, id := range ids {
		out = append(out, model.Instance{ID: id, Service: "s"})
	}
	return out
}

func TestRoundRobinStable(t *testing.T) {
	b, _ := New(PolicyRoundRobin, nil)
	got := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		ins, err := b.Pick("s", mkIDs("a", "b", "c"), nil)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ins.ID)
	}
	want := []string{"a", "b", "c", "a", "b", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("轮询结论应确定为 %v, got %v", want, got)
	}
	t.Logf("轮询序列=%v，健康集合变化时仍按“当前候选取模”给出确定结论", got)

	// 排除已尝试实例后从剩余健康集合选择。
	ins, err := b.Pick("s", mkIDs("a", "b", "c"), map[string]bool{"a": true, "b": true})
	if err != nil || ins.ID != "c" {
		t.Fatalf("排除 a,b 后只能选 c, got %+v err=%v", ins, err)
	}
}

func TestNoCandidate(t *testing.T) {
	b, _ := New(PolicyRoundRobin, nil)
	_, err := b.Pick("s", nil, nil)
	if modelErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("空候选应为 no_healthy_instance, got %v", err)
	}
	_, err = b.Pick("s", mkIDs("a"), map[string]bool{"a": true})
	if modelErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("全部排除后应为 no_healthy_instance, got %v", err)
	}
}

func TestWeightedDistribution(t *testing.T) {
	wb, _ := New(PolicyWeighted, map[string]int{"a": 5, "b": 1, "c": 1})
	count := map[string]int{}
	for i := 0; i < 700; i++ {
		ins, err := wb.Pick("s", mkIDs("a", "b", "c"), nil)
		if err != nil {
			t.Fatal(err)
		}
		count[ins.ID]++
	}
	t.Logf("SWRR 700 次分布=%v（权重 a:b:c=5:1:1）", count)
	if count["a"] != 500 || count["b"] != 100 || count["c"] != 100 {
		t.Fatalf("加权分布应为 500/100/100, got %v", count)
	}
	// 平滑性：前 7 次中每个实例都应出现（无连续 5 次 a）。
	b2, _ := New(PolicyWeighted, map[string]int{"a": 5, "b": 1, "c": 1})
	first7 := map[string]int{}
	for i := 0; i < 7; i++ {
		ins, _ := b2.Pick("s", mkIDs("a", "b", "c"), nil)
		first7[ins.ID]++
	}
	if first7["b"] != 1 || first7["c"] != 1 {
		t.Fatalf("SWRR 前 7 次应平滑覆盖, got %v", first7)
	}
}

func TestLeastConnUnderConcurrency(t *testing.T) {
	lc := newLeastConn()
	pool := mkIDs("a", "b", "c", "d")
	picks := map[string]int{}
	var pickMu sync.Mutex
	var wg sync.WaitGroup

	// 背景在途：a=3, b=1, c=0, d=0。这些计数在全部选择完成后才归还。
	for i := 0; i < 3; i++ {
		lc.Begin("a")
	}
	lc.Begin("b")
	defer func() {
		for i := 0; i < 3; i++ {
			lc.End("a")
		}
		lc.End("b")
	}()

	// 100 个并发请求：选择 + Begin 原子完成，且所有请求持有到选择阶段结束。
	release := make(chan struct{})
	var chosen sync.WaitGroup
	for i := 0; i < 100; i++ {
		chosen.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ins, err := lc.PickAndBegin("s", pool, nil)
			if err != nil {
				chosen.Done()
				t.Error(err)
				return
			}
			// 不变量（可解释）：被选实例的在途数必须 <= 其他所有健康候选。
			chosenN := lc.Inflight(ins.ID) - 1 // PickAndBegin 已自增，自增前的值
			for _, other := range pool {
				if other.ID == ins.ID {
					continue
				}
				// other 的计数取“选择时刻”的下界：当前值不小于当时值（只增不减）。
				if n := lc.Inflight(other.ID); n < chosenN {
					t.Errorf("选择 %s 时其在途 %d 大于候选 %s 的 %d，违反最少连接",
						ins.ID, chosenN, other.ID, n)
				}
			}
			pickMu.Lock()
			picks[ins.ID]++
			pickMu.Unlock()
			chosen.Done()
			<-release
			lc.End(ins.ID)
		}()
	}
	chosen.Wait()
	close(release)
	wg.Wait()
	t.Logf("least-conn 100 并发选择分布=%v，背景在途 a=3/b=1（选择时刻最小负载不变量已逐次校验）", picks)
	if picks["c"]+picks["d"] == 0 {
		t.Fatalf("初始最低负载的 c/d 应至少承接部分请求, picks=%v", picks)
	}
	if sum := picks["a"] + picks["b"] + picks["c"] + picks["d"]; sum != 100 {
		t.Fatalf("选择总数应为 100, got %d", sum)
	}
}

func TestUnknownPolicyRejected(t *testing.T) {
	if _, err := New("magic", nil); modelErrKind(err) != model.ErrRuleInvalid {
		t.Fatalf("未知策略应被拒绝为 rule_invalid, got %v", err)
	}
}
