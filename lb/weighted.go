package lb

import (
	"sync"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// wrrNode 保存每个实例的静态权重与动态当前权重。
type wrrNode struct {
	id        string
	weight    int
	curWeight int
}

// weightedRR 平滑加权轮询（类似 nginx SWRR）：
// 每轮每个节点 current += effectiveWeight；选出 current 最大者；
// 被选节点 current -= 总有效权重。长期分布与权重成比例，且单轮内分布平滑、确定。
type weightedRR struct {
	mu      sync.Mutex
	weights map[string]int
	nodes   map[string]*wrrNode // 按 service 分组保序
	order   map[string][]string
}

func newWeightedRR(weights map[string]int) *weightedRR {
	return &weightedRR{
		weights: weights,
		nodes:   make(map[string]*wrrNode),
		order:   make(map[string][]string),
	}
}

func (b *weightedRR) Name() Policy { return PolicyWeighted }

func (b *weightedRR) effectiveWeight(id string) int {
	if w, ok := b.weights[id]; ok && w > 0 {
		return w
	}
	return 1
}

func (b *weightedRR) Pick(service string, candidates []model.Instance, exclude map[string]bool) (model.Instance, error) {
	pool := available(candidates, exclude)
	if len(pool) == 0 {
		return model.Instance{}, ErrNoCandidate
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	order, ok := b.order[service]
	currentIDs := make(map[string]bool, len(pool))
	for _, ins := range pool {
		currentIDs[ins.ID] = true
		if _, exist := b.nodes[ins.ID]; !exist {
			b.nodes[ins.ID] = &wrrNode{id: ins.ID, weight: b.effectiveWeight(ins.ID)}
		}
	}
	// 重建顺序：沿用上一版仍在候选中的实例顺序，再追加新实例，保证确定性。
	if !ok {
		order = make([]string, 0, len(pool))
	}
	kept := order[:0]
	for _, id := range order {
		if currentIDs[id] {
			kept = append(kept, id)
		} else {
			delete(b.nodes, id)
		}
	}
	for _, ins := range pool {
		found := false
		for _, id := range kept {
			if id == ins.ID {
				found = true
				break
			}
		}
		if !found {
			kept = append(kept, ins.ID)
		}
	}
	b.order[service] = kept

	total := 0
	for _, id := range kept {
		n := b.nodes[id]
		n.curWeight += n.weight
		total += n.weight
	}
	best := kept[0]
	bestNode := b.nodes[best]
	for _, id := range kept[1:] {
		n := b.nodes[id]
		if n.curWeight > bestNode.curWeight {
			best, bestNode = id, n
		}
	}
	bestNode.curWeight -= total

	for _, ins := range pool {
		if ins.ID == best {
			return ins, nil
		}
	}
	// 理论不可达：best 一定来自 pool
	return model.Instance{}, ErrNoCandidate
}
