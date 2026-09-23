package router

import (
	"context"
	"sync"

	"github.com/highcumontoa/service-discovery-lb-go/forwarder"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// Discovery 是编排器需要的注册表能力视图。
type Discovery interface {
	HealthyInstances(service string) []string
	Get(id string) (model.Instance, model.HealthState, bool)
	IsDead(id string) bool
	Snapshot() model.Snapshot
}

// Dispatcher 编排一次请求的完整决策链：
// 取规则快照 -> 取注册表快照 -> 选健康候选 -> 均衡器挑选 -> 连接前复核 -> 转发/重试。
type Dispatcher struct {
	reg    Discovery
	fwd    *forwarder.Forwarder
	router *Router

	bcMu      sync.Mutex
	balancers map[balancerKey]lb.Balancer
	logf      func(format string, args ...any)
}

type balancerKey struct {
	version uint64
	service string
}

// DispatcherOption 配置编排器。
type DispatcherOption func(*Dispatcher)

// WithDispatcherLogger 注入决策日志。
func WithDispatcherLogger(logf func(format string, args ...any)) DispatcherOption {
	return func(d *Dispatcher) { d.logf = logf }
}

// NewDispatcher 创建编排器。
func NewDispatcher(reg Discovery, rt *Router, fwd *forwarder.Forwarder, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		reg:       reg,
		fwd:       fwd,
		router:    rt,
		balancers: make(map[balancerKey]lb.Balancer),
		logf:      func(string, ...any) {},
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// balancerFor 按“规则版本 + 服务”获取或构造均衡器。
// 每个版本拥有独立的均衡器游标，因此在途请求与旧版本策略严格一致。
// 旧版本在被新版本取代后回收（在途请求持有的是 Balancer 对象引用，不受影响）。
func (d *Dispatcher) balancerFor(rule Rule, version uint64) (lb.Balancer, error) {
	d.bcMu.Lock()
	defer d.bcMu.Unlock()
	key := balancerKey{version: version, service: rule.Service}
	if b, ok := d.balancers[key]; ok {
		return b, nil
	}
	// 回收同服务的更早版本条目。
	for k := range d.balancers {
		if k.service == rule.Service && k.version != version {
			delete(d.balancers, k)
		}
	}
	b, err := lb.New(rule.Policy, rule.Weights)
	if err != nil {
		return nil, err
	}
	d.balancers[key] = b
	return b, nil
}

// snapshotSelector 是绑定单次请求的选择器：持有不可变候选集与一个规则版本的均衡器。
type snapshotSelector struct {
	service    string
	snap       model.Snapshot
	bal        lb.Balancer
	healthyIDs map[string]bool
}

func (s *snapshotSelector) Pick(exclude map[string]bool) (model.Instance, error) {
	return s.pick(exclude, false)
}

// PickAndBegin 在均衡器支持时原子完成选择与在途计数 +1（least_conn 路径）。
func (s *snapshotSelector) PickAndBegin(exclude map[string]bool) (model.Instance, error) {
	return s.pick(exclude, true)
}

func (s *snapshotSelector) pick(exclude map[string]bool, begin bool) (model.Instance, error) {
	candidates := make([]model.Instance, 0, len(s.snap.Instances))
	for _, ins := range s.snap.Instances {
		if ins.Service != s.service {
			continue
		}
		if !s.healthyIDs[ins.ID] {
			continue
		}
		candidates = append(candidates, ins)
	}
	if begin {
		if cb, ok := s.bal.(lb.ConnBeginner); ok {
			return cb.PickAndBegin(s.service, candidates, exclude)
		}
	}
	return s.bal.Pick(s.service, candidates, exclude)
}

// Dispatch 执行一次完整请求；规则版本与实例视图在请求开始时冻结。
func (d *Dispatcher) Dispatch(ctx context.Context, req model.RequestContext) (*forwarder.Result, error) {
	rules := d.router.Current()
	rule, ok := rules.Lookup(req.Service)
	if !ok {
		return nil, model.Errf(model.ErrServiceNotFound, "",
			"规则版本 %d 中不存在服务 %s", rules.Version, req.Service)
	}
	bal, err := d.balancerFor(rule, rules.Version)
	if err != nil {
		return nil, err
	}
	snap := d.reg.Snapshot()
	healthyIDs := make(map[string]bool)
	for _, ins := range snap.Instances {
		if ins.Service == req.Service && snap.Healthy[ins.ID] == model.StateHealthy {
			healthyIDs[ins.ID] = true
		}
	}
	d.logf("[dispatch] service=%s rules_v=%d registry_v=%d policy=%s healthy=%d",
		req.Service, rules.Version, snap.Version, rule.Policy, len(healthyIDs))

	sel := &snapshotSelector{service: req.Service, snap: snap, bal: bal, healthyIDs: healthyIDs}
	return d.fwd.Execute(ctx, req, sel, rule.ToForwardRetry())
}

// Filter 是转发器使用的连接前复核函数。
func (d *Dispatcher) Filter(id string) (model.Instance, bool) {
	if d.reg.IsDead(id) {
		return model.Instance{}, false
	}
	ins, state, ok := d.reg.Get(id)
	if !ok || !state.Routeable() {
		return model.Instance{}, false
	}
	return ins, true
}
