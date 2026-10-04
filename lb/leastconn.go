package lb

import (
	"sync"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// leastConn 最少在途连接均衡器。
// 选择当前在途请求数最少的健康候选；并列时按候选顺序取第一个，保证确定性。
// Begin/End 由转发层在每次尝试前后调用（包括并发请求），计数器的更新与选择互斥，
// 因此并发下路由结论仍可由“当时的在途计数”解释。
type leastConn struct {
	mu       sync.Mutex
	inflight map[string]int
}

func newLeastConn() *leastConn {
	return &leastConn{inflight: make(map[string]int)}
}

func (b *leastConn) Name() Policy { return PolicyLeastConn }

func (b *leastConn) Pick(service string, candidates []model.Instance, exclude map[string]bool) (model.Instance, error) {
	pool := available(candidates, exclude)
	if len(pool) == 0 {
		return model.Instance{}, ErrNoCandidate
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	best := pool[0]
	bestN := b.inflight[best.ID]
	for _, ins := range pool[1:] {
		if n := b.inflight[ins.ID]; n < bestN {
			best, bestN = ins, n
		}
	}
	return best, nil
}

// PickAndBegin 在同一临界区内完成“选择 + 在途计数 +1”。
// 这是转发路径应使用的原子操作：并发请求不会在计数更新前相互重复选择。
func (b *leastConn) PickAndBegin(service string, candidates []model.Instance, exclude map[string]bool) (model.Instance, error) {
	pool := available(candidates, exclude)
	if len(pool) == 0 {
		return model.Instance{}, ErrNoCandidate
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	best := pool[0]
	bestN := b.inflight[best.ID]
	for _, ins := range pool[1:] {
		if n := b.inflight[ins.ID]; n < bestN {
			best, bestN = ins, n
		}
	}
	b.inflight[best.ID]++
	return best, nil
}

// Begin 登记一个在途请求。
func (b *leastConn) Begin(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.inflight[id]++
}

// End 结束一个在途请求。
func (b *leastConn) End(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inflight[id] > 0 {
		b.inflight[id]--
	}
}

// Inflight 查询某实例在途数（供日志解释）。
func (b *leastConn) Inflight(id string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inflight[id]
}

// TotalInflight 查询该均衡器（规则版本 + 服务）下所有实例的在途总数。
// 一批请求全部结束后必须回到 0，用于端到端校验“计数只涨必落”。
func (b *leastConn) TotalInflight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	for _, n := range b.inflight {
		total += n
	}
	return total
}
