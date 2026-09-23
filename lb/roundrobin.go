package lb

import (
	"sync/atomic"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

// roundRobin 在“同一组实例”上按稳定顺序轮转。
// 实例集合来自注册表快照，每次 Pick 按 ID 排序后取模，
// 因此并发上下线时，只要快照一致，选择结论就一致。
type roundRobin struct {
	counter uint64
}

func (rr *roundRobin) Name() Strategy { return StrategyRoundRobin }

func (rr *roundRobin) Pick(req Request, instances []*discovery.Instance) (*discovery.Instance, error) {
	if len(instances) == 0 {
		return nil, ErrNoHealthyInstance
	}
	sorted := sortedByID(instances)
	n := atomic.AddUint64(&rr.counter, 1) - 1
	return sorted[int(n%uint64(len(sorted)))], nil
}
