package lb

import (
	"sync/atomic"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

// weightedBalancer 平滑加权轮询（类似 nginx SWRR）。
// 权重来自规则快照中 ID->weight，缺省为 1。
type weightedBalancer struct {
	weights map[string]int
	counter uint64
}

func newWeighted(weights map[string]int) *weightedBalancer {
	w := make(map[string]int, len(weights))
	for k, v := range weights {
		if v > 0 {
			w[k] = v
		}
	}
	return &weightedBalancer{weights: w}
}

func (b *weightedBalancer) Name() Strategy { return StrategyWeighted }

func (b *weightedBalancer) weight(id string) int {
	if w, ok := b.weights[id]; ok && w > 0 {
		return w
	}
	return 1
}

func (b *weightedBalancer) Pick(req Request, instances []*discovery.Instance) (*discovery.Instance, error) {
	if len(instances) == 0 {
		return nil, ErrNoHealthyInstance
	}
	sorted := sortedByID(instances)

	// 用单调计数器驱动的确定性平滑加权，避免共享 currentWeight 数组
	// 在快照实例集合变化时出现并发数据竞争。
	total := 0
	for _, in := range sorted {
		total += b.weight(in.ID)
	}
	n := atomic.AddUint64(&b.counter, 1) - 1

	// currentWeight_i(step k) 初始为 0，第 k 轮的相对位置可解析计算：
	// 在第 n 次选择时，遍历 ID 排序后的列表做 SWRR 模拟（每次只依赖 n 与权重）。
	cw := make([]int, len(sorted))
	for i, in := range sorted {
		cw[i] = b.weight(in.ID)
	}
	step := int(n % uint64(total))
	// 经典 SWRR 的单步推进：对每个节点 current += effective；选最大者后减去 totalWeight。
	eff := make([]int, len(sorted))
	cur := make([]int, len(sorted))
	for i, in := range sorted {
		eff[i] = b.weight(in.ID)
	}
	var picked int
	for s := 0; s <= step; s++ {
		best := -1
		for i := range cur {
			cur[i] += eff[i]
			if best == -1 || cur[i] > cur[best] {
				best = i
			}
		}
		cur[best] -= total
		picked = best
	}
	_ = cw
	return sorted[picked], nil
}
