package route

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/lb"
)

// Rule 一条路由规则：匹配某服务，指定负载均衡策略与转发参数。
type Rule struct {
	Service       string
	Strategy      lb.Strategy
	Timeout       time.Duration // 单次尝试超时
	MaxAttempts   int           // 含首次在内的最大尝试次数（<=1 表示不重试）
	RetrySameNode bool          // 是否允许重试同一节点（默认 false，避免重复副作用）
	FailOpen      bool          // 后端不可用/全部失败时是否降级放行
	Weights       map[string]int
}

// VersionedRules 某一版完整规则集合，一经发布即不可变。
type VersionedRules struct {
	Version uint64
	Rules   map[string]*Rule
	BuiltAt time.Time
}

// Lookup 查找服务对应的规则；未配置返回 nil。
func (v *VersionedRules) Lookup(service string) *Rule {
	if v == nil {
		return nil
	}
	return v.Rules[service]
}

// ErrNoRules / ErrInvalidRule 热更新失败原因。
var (
	ErrNoRules     = errors.New("route: rules must not be empty")
	ErrInvalidRule = errors.New("route: invalid rule")
)

// defaultRule 内置兜底规则：任一未显式配置的服务使用它。
func defaultRule(service string) *Rule {
	return &Rule{
		Service:     service,
		Strategy:    lb.StrategyRoundRobin,
		Timeout:     500 * time.Millisecond,
		MaxAttempts: 1,
	}
}

// validate 全量校验一批规则；任何一条非法即整体拒绝（不允许部分生效）。
func validate(rules []*Rule) (map[string]*Rule, error) {
	if len(rules) == 0 {
		return nil, ErrNoRules
	}
	out := make(map[string]*Rule, len(rules))
	for i, r := range rules {
		if r == nil {
			return nil, fmt.Errorf("%w: rule[%d] is nil", ErrInvalidRule, i)
		}
		if r.Service == "" {
			return nil, fmt.Errorf("%w: rule[%d] has empty service", ErrInvalidRule, i)
		}
		if _, ok := out[r.Service]; ok {
			return nil, fmt.Errorf("%w: duplicate service %q", ErrInvalidRule, r.Service)
		}
		switch r.Strategy {
		case "", lb.StrategyRoundRobin, lb.StrategyRandom,
			lb.StrategyWeighted, lb.StrategyConsistent:
		default:
			return nil, fmt.Errorf("%w: service %q unknown strategy %q",
				ErrInvalidRule, r.Service, r.Strategy)
		}
		if r.Timeout < 0 {
			return nil, fmt.Errorf("%w: service %q negative timeout", ErrInvalidRule, r.Service)
		}
		if r.MaxAttempts < 0 {
			return nil, fmt.Errorf("%w: service %q negative max-attempts", ErrInvalidRule, r.Service)
		}
		for id, w := range r.Weights {
			if w <= 0 {
				return nil, fmt.Errorf("%w: service %q non-positive weight for %q",
					ErrInvalidRule, r.Service, id)
			}
		}
		// 复制成不可变规则
		cp := *r
		if r.Strategy == "" {
			cp.Strategy = lb.StrategyRoundRobin
		}
		if cp.Timeout == 0 {
			cp.Timeout = defaultRule(r.Service).Timeout
		}
		if cp.MaxAttempts == 0 {
			cp.MaxAttempts = 1
		}
		if r.Weights != nil {
			cp.Weights = make(map[string]int, len(r.Weights))
			for k, v := range r.Weights {
				cp.Weights[k] = v
			}
		}
		out[r.Service] = &cp
	}
	return out, nil
}

// Table 持有当前生效规则版本，支持原子热更新。
// 并发读取通过 RWMutex + 不可变快照保证：在途请求拿到 *VersionedRules
// 指针后，即使后台发布新版本，本次请求的策略也不会中途改变。
type Table struct {
	mu      sync.RWMutex
	current *VersionedRules
}

// NewTable 创建规则表，初始版本 0 使用内置兜底规则（空 map，按服务动态兜底）。
func NewTable() *Table {
	return &Table{current: &VersionedRules{Version: 0, Rules: map[string]*Rule{}, BuiltAt: time.Now()}}
}

// Publish 校验并原子发布新版规则；校验失败时保留上一版，返回错误。
func (t *Table) Publish(rules []*Rule) (*VersionedRules, error) {
	valid, err := validate(rules)
	if err != nil {
		// 不触碰 current，保证上一版继续可用。
		return t.Current(), err
	}
	t.mu.Lock()
	next := &VersionedRules{
		Version: t.current.Version + 1,
		Rules:   valid,
		BuiltAt: time.Now(),
	}
	t.current = next
	t.mu.Unlock()
	return next, nil
}

// Current 返回当前生效版本（永不为 nil）。
// 若服务未配置规则，返回的版本中 Lookup 为 nil，调用方按兜底规则处理。
func (t *Table) Current() *VersionedRules {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.current
}

// Resolve 返回某服务的生效规则：显式配置优先，否则兜底。
func (t *Table) Resolve(service string) (*Rule, uint64) {
	cur := t.Current()
	if r := cur.Lookup(service); r != nil {
		return r, cur.Version
	}
	return defaultRule(service), cur.Version
}
