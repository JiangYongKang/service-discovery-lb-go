package lb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

func mkInstances(ids ...string) []*discovery.Instance {
	out := make([]*discovery.Instance, 0, len(ids))
	for _, id := range ids {
		in := discovery.NewInstance(id, "svc", "addr", nil)
		_ = in
		// 测试内直接构造可路由实例
		out = append(out, newHealthy(id))
	}
	return out
}

func newHealthy(id string) *discovery.Instance {
	in := discovery.NewInstance(id, "svc", "addr-"+id, nil)
	return in
}

func TestNoHealthyInstance(t *testing.T) {
	for _, st := range []Strategy{StrategyRoundRobin, StrategyRandom, StrategyWeighted, StrategyConsistent} {
		b, err := New(st, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = b.Pick(Request{Service: "s"}, nil)
		if !errors.Is(err, ErrNoHealthyInstance) {
			t.Fatalf("%s: want ErrNoHealthyInstance, got %v", st, err)
		}
	}
}

func TestRoundRobinStableOrder(t *testing.T) {
	b, _ := New(StrategyRoundRobin, nil)
	insts := mkInstances("c", "a", "b") // 乱序输入
	want := []string{"a", "b", "c", "a", "b", "c"}
	for i, w := range want {
		got, err := b.Pick(Request{Service: "svc"}, insts)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != w {
			t.Fatalf("step %d want %s got %s", i, w, got.ID)
		}
	}
	t.Log("round-robin over unsorted input produces stable sorted order: a,b,c,a,b,c")
}

func TestWeightedDistribution(t *testing.T) {
	b, _ := New(StrategyWeighted, map[string]int{"a": 3, "b": 1})
	insts := mkInstances("a", "b")
	counts := map[string]int{}
	for i := 0; i < 400; i++ {
		got, err := b.Pick(Request{Service: "svc"}, insts)
		if err != nil {
			t.Fatal(err)
		}
		counts[got.ID]++
	}
	ratio := float64(counts["a"]) / float64(counts["b"])
	if ratio < 2.5 || ratio > 3.5 {
		t.Fatalf("weighted ratio want ~3, got %.2f (%v)", ratio, counts)
	}
	t.Logf("weighted picks over 400 calls: %v ratio=%.2f", counts, ratio)
}

func TestConsistentHashSticky(t *testing.T) {
	b, _ := New(StrategyConsistent, nil)
	insts := mkInstances("n1", "n2", "n3", "n4")
	keys := []string{"user:1", "user:2", "user:3", "order:9", "cart:77"}
	first := map[string]string{}
	// 重复 50 轮，同 key 必须恒定
	for round := 0; round < 50; round++ {
		for _, k := range keys {
			got, err := b.Pick(Request{Service: "svc", HashKey: k}, insts)
			if err != nil {
				t.Fatal(err)
			}
			if round == 0 {
				first[k] = got.ID
			} else if got.ID != first[k] {
				t.Fatalf("key %s moved %s -> %s", k, first[k], got.ID)
			}
		}
	}
	t.Logf("consistent-hash mapping stable over 50 rounds: %v", first)

	// 摘除不承载当前任何 key 的节点 n3：这些 key 的映射必须全部保持不变
	reduced := mkInstances("n1", "n2", "n4")
	moved := 0
	for _, k := range keys {
		got, _ := b.Pick(Request{Service: "svc", HashKey: k}, reduced)
		if got.ID != first[k] {
			moved++
		}
	}
	if moved != 0 {
		t.Fatalf("removing non-owner node n3 moved %d/5 keys, expected 0", moved)
	}
	t.Logf("after removing non-owner n3, %d/5 keys remapped (minimal migration)", moved)
}

func TestUnknownStrategy(t *testing.T) {
	if _, err := New(Strategy("magikarp"), nil); !errors.Is(err, ErrUnknownStrategy) {
		t.Fatalf("want ErrUnknownStrategy, got %v", err)
	}
}

var _ = fmt.Sprint
