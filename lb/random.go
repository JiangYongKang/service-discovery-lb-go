package lb

import (
	"math/rand"
	"sync"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// randSource 是可替换的随机源，测试中可注入固定序列保证确定性。
var (
	rndMu sync.Mutex
	rnd   = rand.New(rand.NewSource(1))
)

// SeedRand 替换随机源种子（主要供测试使用）。
func SeedRand(seed int64) {
	rndMu.Lock()
	defer rndMu.Unlock()
	rnd = rand.New(rand.NewSource(seed))
}

// randomBalancer 随机均衡器。
type randomBalancer struct{}

func newRandom() *randomBalancer { return &randomBalancer{} }

func (b *randomBalancer) Name() Policy { return PolicyRandom }

func (b *randomBalancer) Pick(service string, candidates []model.Instance, exclude map[string]bool) (model.Instance, error) {
	pool := available(candidates, exclude)
	if len(pool) == 0 {
		return model.Instance{}, ErrNoCandidate
	}
	rndMu.Lock()
	idx := rnd.Intn(len(pool))
	rndMu.Unlock()
	return pool[idx], nil
}
