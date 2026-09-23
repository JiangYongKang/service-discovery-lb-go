package lb

import (
	"sync/atomic"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// roundRobin 轮询均衡器。游标在全局单调递增，对“当前候选集合”取模，
// 因此即使健康集合变化，选择结论仍然确定（可由版本与游标解释）。
type roundRobin struct {
	next uint64
}

func (b *roundRobin) Name() Policy { return PolicyRoundRobin }

func (b *roundRobin) Pick(service string, candidates []model.Instance, exclude map[string]bool) (model.Instance, error) {
	pool := available(candidates, exclude)
	if len(pool) == 0 {
		return model.Instance{}, ErrNoCandidate
	}
	idx := atomic.AddUint64(&b.next, 1) - 1
	return pool[idx%uint64(len(pool))], nil
}
