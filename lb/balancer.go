package lb

import (
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// Policy 负载均衡策略名。
type Policy string

const (
	PolicyRoundRobin Policy = "round_robin"
	PolicyRandom     Policy = "random"
	PolicyWeighted   Policy = "weighted"
	PolicyLeastConn  Policy = "least_conn"
)

// Balancer 依据某一时刻的候选集合选择实例。
// candidates 必须全部为当前健康实例；exclude 为本请求已尝试过的实例（重试时排除）。
// 实现必须在“过滤后的可用候选”中做选择；可用候选为空时返回 ErrNoCandidate。
type Balancer interface {
	Pick(service string, candidates []model.Instance, exclude map[string]bool) (model.Instance, error)
	Name() Policy
}

// ErrNoCandidate 表示健康候选为空（全部摘除或已尝试过）。
var ErrNoCandidate = model.NewError(model.ErrNoHealthyInstance, "", "过滤后没有可选择的健康候选")

// ConnBeginner 由需要原子“选择并占用连接名额”的均衡器实现。
type ConnBeginner interface {
	PickAndBegin(service string, candidates []model.Instance, exclude map[string]bool) (model.Instance, error)
}

// New 按策略名构造均衡器；weights 用于加权策略（缺省权重为 1）。
func New(policy Policy, weights map[string]int) (Balancer, error) {
	switch policy {
	case "", PolicyRoundRobin:
		return &roundRobin{}, nil
	case PolicyRandom:
		return newRandom(), nil
	case PolicyWeighted:
		return newWeightedRR(weights), nil
	case PolicyLeastConn:
		return newLeastConn(), nil
	default:
		return nil, model.Errf(model.ErrRuleInvalid, "", "未知负载均衡策略: %s", policy)
	}
}

// available 返回过滤掉 exclude 后的候选；保序以便结论确定、可解释。
func available(candidates []model.Instance, exclude map[string]bool) []model.Instance {
	if len(exclude) == 0 {
		return candidates
	}
	out := make([]model.Instance, 0, len(candidates))
	for _, ins := range candidates {
		if !exclude[ins.ID] {
			out = append(out, ins)
		}
	}
	return out
}
