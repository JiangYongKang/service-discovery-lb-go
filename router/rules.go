package router

import (
	"fmt"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/forwarder"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// 合法策略集合，校验规则热更新时使用。
var validPolicies = map[lb.Policy]bool{
	lb.PolicyRoundRobin: true,
	lb.PolicyRandom:     true,
	lb.PolicyWeighted:   true,
	lb.PolicyLeastConn:  true,
}

// Rule 是单个服务的路由规则。
type Rule struct {
	Service        string
	Policy         lb.Policy
	Weights        map[string]int
	MaxAttempts    int
	RetryableKinds map[model.ErrorKind]bool
	PerCallTimeout time.Duration
}

// Clone 返回深拷贝，保证整版规则在发布后不可变。
func (r Rule) Clone() Rule {
	cp := r
	if r.Weights != nil {
		cp.Weights = make(map[string]int, len(r.Weights))
		for k, v := range r.Weights {
			cp.Weights[k] = v
		}
	}
	if r.RetryableKinds != nil {
		cp.RetryableKinds = make(map[model.ErrorKind]bool, len(r.RetryableKinds))
		for k, v := range r.RetryableKinds {
			cp.RetryableKinds[k] = v
		}
	}
	return cp
}

// ToForwardRetry 导出转发层使用的重试策略视图。
func (r Rule) ToForwardRetry() forwarder.RetryPolicy {
	return forwarder.RetryPolicy{
		MaxAttempts:    r.MaxAttempts,
		RetryableKinds: r.RetryableKinds,
		PerCallTimeout: r.PerCallTimeout,
	}
}

// Rules 是一整版不可变规则。
type Rules struct {
	Version   uint64
	ByService map[string]Rule
}

// Clone 深拷贝整版规则。
func (r Rules) Clone() Rules {
	cp := Rules{Version: r.Version, ByService: make(map[string]Rule, len(r.ByService))}
	for k, v := range r.ByService {
		cp.ByService[k] = v.Clone()
	}
	return cp
}

// Lookup 查询某服务的规则。
func (r Rules) Lookup(service string) (Rule, bool) {
	rule, ok := r.ByService[service]
	return rule, ok
}

// Validate 校验整版规则；任一服务规则非法即整体失败，热更新必须整体拒绝。
func (r Rules) Validate() error {
	if len(r.ByService) == 0 {
		return model.NewError(model.ErrRuleInvalid, "", "规则版本不允许为空")
	}
	for name, rule := range r.ByService {
		if rule.Service == "" {
			rule.Service = name
		}
		if rule.Service != name {
			return model.Errf(model.ErrRuleInvalid, "",
				"规则键 %s 与 Service 字段 %s 不一致", name, rule.Service)
		}
		if rule.Policy == "" {
			rule.Policy = lb.PolicyRoundRobin
		}
		if !validPolicies[rule.Policy] {
			return model.Errf(model.ErrRuleInvalid, "", "服务 %s 使用未知策略 %q", name, rule.Policy)
		}
		if rule.MaxAttempts < 0 {
			return model.Errf(model.ErrRuleInvalid, "", "服务 %s 的 MaxAttempts 不能为负", name)
		}
		for id, w := range rule.Weights {
			if w < 0 {
				return model.Errf(model.ErrRuleInvalid, "", "服务 %s 实例 %s 权重为负", name, id)
			}
		}
		for k := range rule.RetryableKinds {
			switch k {
			case model.ErrConnectFailed, model.ErrTimeout, model.ErrUpstream5xx,
				model.ErrUpstream4xx, model.ErrNoHealthyInstance:
			default:
				return model.Errf(model.ErrRuleInvalid, "",
					"服务 %s 配置了不允许用于重试决策的错误类别 %s", name, k)
			}
		}
	}
	return nil
}

// Normalize 补默认值（MaxAttempts、策略、单次超时），在校验通过后使用。
func (r *Rules) Normalize() {
	for name, rule := range r.ByService {
		if rule.Policy == "" {
			rule.Policy = lb.PolicyRoundRobin
		}
		if rule.MaxAttempts == 0 {
			rule.MaxAttempts = 1
		}
		if rule.PerCallTimeout == 0 {
			rule.PerCallTimeout = time.Second
		}
		r.ByService[name] = rule
	}
}

// String 输出版本摘要，供日志解释。
func (r Rules) String() string {
	return fmt.Sprintf("Rules(version=%d, services=%d)", r.Version, len(r.ByService))
}
