package lb

import (
	"math/rand/v2"
	"sync"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

// randomBalancer 在健康实例上等概率随机选择。
type randomBalancer struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (b *randomBalancer) Name() Strategy { return StrategyRandom }

func (b *randomBalancer) Pick(req Request, instances []*discovery.Instance) (*discovery.Instance, error) {
	if len(instances) == 0 {
		return nil, ErrNoHealthyInstance
	}
	b.mu.Lock()
	n := b.intN(len(instances))
	b.mu.Unlock()
	// 仍基于排序后的稳定索引选取，便于日志复现。
	return sortedByID(instances)[n], nil
}

func (b *randomBalancer) intN(n int) int {
	if b.r == nil {
		b.r = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return b.r.IntN(n)
}
