package router

import (
	"sync/atomic"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// Router 持有当前生效规则版本（不可变快照）。
//
// 一致性语义：
//   - Current() 返回的 *Rules 永不为部分更新的中间态；
//   - 热更新采用“先整体校验+规范化，再一次原子指针替换”：
//     新版本任一规则非法时，直接返回 ErrRuleInvalid，旧版本保持完全可用；
//   - 在途请求在开始时取一次 Current()，整个请求（含重试）都依据该版本执行，
//     即使更新期间规则变化也不会中途切换策略或权重。
type Router struct {
	current atomic.Pointer[Rules]
}

// New 创建路由器，需先有一版可用初始规则（同样执行校验）。
func New(initial Rules) (*Router, error) {
	rules, err := prepare(initial)
	if err != nil {
		return nil, err
	}
	r := &Router{}
	r.current.Store(&rules)
	return r, nil
}

// Current 返回当前规则快照（不可变，调用方可长期持有）。
func (r *Router) Current() *Rules { return r.current.Load() }

// Update 原子热更新：先整体校验+规范化，失败保留上一版，绝不部分生效。
func (r *Router) Update(next Rules) error {
	rules, err := prepare(next)
	if err != nil {
		return model.WrapError(model.ErrRuleInvalid, "",
			"新版本规则校验失败，保留上一版 "+r.current.Load().String(), err)
	}
	old := r.current.Swap(&rules)
	_ = old
	return nil
}

// prepare 对整版规则做校验与规范化，返回可发布的独立副本。
func prepare(in Rules) (Rules, error) {
	if err := in.Validate(); err != nil {
		return Rules{}, err
	}
	out := in.Clone()
	if out.Version == 0 {
		out.Version = 1
	}
	out.Normalize()
	return out, nil
}
