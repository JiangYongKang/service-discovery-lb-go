package lb

import (
	"errors"
	"fmt"
	"sort"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

// Strategy 负载均衡策略名称。
type Strategy string

const (
	StrategyRoundRobin Strategy = "round-robin"
	StrategyRandom     Strategy = "random"
	StrategyWeighted   Strategy = "weighted"
	StrategyConsistent Strategy = "consistent-hash"
)

// ErrNoHealthyInstance 后端无可用实例。
var ErrNoHealthyInstance = errors.New("lb: no healthy instance available")

// ErrUnknownStrategy 未知策略。
var ErrUnknownStrategy = errors.New("lb: unknown strategy")

// Request 参与选路的请求抽象（一致哈希等需要 Key）。
type Request struct {
	Service string
	HashKey string
	Tags    map[string]string
}

// Balancer 从一批健康实例中选出一个。
// 实现必须不依赖实例顺序的外部假设：对快照排序后再选择，保证并发下结论稳定。
type Balancer interface {
	Pick(req Request, instances []*discovery.Instance) (*discovery.Instance, error)
	Name() Strategy
}

// New 按策略名构造负载均衡器。
func New(strategy Strategy, weights map[string]int) (Balancer, error) {
	switch strategy {
	case "", StrategyRoundRobin:
		return &roundRobin{}, nil
	case StrategyRandom:
		return &randomBalancer{}, nil
	case StrategyWeighted:
		return newWeighted(weights), nil
	case StrategyConsistent:
		return newConsistentHash(weights), nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownStrategy, strategy)
	}
}

// sortedByID 返回按 ID 排序的实例副本，保证选择结果可复现。
func sortedByID(in []*discovery.Instance) []*discovery.Instance {
	out := append([]*discovery.Instance(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
