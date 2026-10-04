package mesh

import (
	"context"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
	"github.com/highcumontoa/service-discovery-lb-go/forwarder"
	"github.com/highcumontoa/service-discovery-lb-go/health"
	"github.com/highcumontoa/service-discovery-lb-go/model"
	"github.com/highcumontoa/service-discovery-lb-go/router"
)

// Mesh 是本地服务发现 + 流量调度能力的门面。
type Mesh struct {
	Registry *discovery.Registry
	Checker  *health.Checker
	Monitor  *health.Monitor
	Router   *router.Router
	Fwd      *forwarder.Forwarder
	Dispatch *router.Dispatcher
}

// Config 装配参数。
type Config struct {
	FailThreshold    int
	RecoverThreshold int
	Probe            health.ProbeFunc
	ProbeInterval    time.Duration
	InitialRules     router.Rules
	// DedupeTTL 同键结论保留时长（成功/失败一致）；<=0 使用转发器默认值。
	DedupeTTL time.Duration
	// DedupeMaxEntries 结论总条数上限，超限按最旧发布顺序淘汰；<=0 使用默认值。
	DedupeMaxEntries int
	// DedupeWaitBudget 同键 follower 等待在途结论的最长时间；<=0 使用默认值。
	DedupeWaitBudget time.Duration
	Logf             func(format string, args ...any)
}

// New 装配整套组件。
func New(cfg Config) (*Mesh, error) {
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	dedupeCfg := forwarder.DedupeConfig{
		TTL:        cfg.DedupeTTL,
		MaxEntries: cfg.DedupeMaxEntries,
		WaitBudget: cfg.DedupeWaitBudget,
	}
	reg := discovery.NewRegistry()
	checker := health.New(health.Config{
		FailThreshold:    cfg.FailThreshold,
		RecoverThreshold: cfg.RecoverThreshold,
	}, cfg.Probe)
	rt, err := router.New(cfg.InitialRules)
	if err != nil {
		return nil, err
	}
	fwd := forwarder.New(nil, forwarder.WithDedupeConfig(dedupeCfg), forwarder.WithLogger(logf))
	dsp := router.NewDispatcher(reg, rt, fwd, router.WithDispatcherLogger(logf))
	// 转发器必须使用编排器的连接前复核，堵截并发注销/摘除。
	fwd = forwarder.New(dsp.Filter, forwarder.WithDedupeConfig(dedupeCfg), forwarder.WithLogger(logf))
	dsp = router.NewDispatcher(reg, rt, fwd, router.WithDispatcherLogger(logf))
	mon := health.NewMonitor(checker, reg, reg, cfg.ProbeInterval, logf)
	return &Mesh{
		Registry: reg,
		Checker:  checker,
		Monitor:  mon,
		Router:   rt,
		Fwd:      fwd,
		Dispatch: dsp,
	}, nil
}

// Register 注册实例并将健康状态机重置为 UNKNOWN（等待探测就绪）。
func (m *Mesh) Register(ins model.Instance) error {
	if err := m.Registry.Register(ins); err != nil {
		return err
	}
	m.Checker.Reset(ins.ID)
	return nil
}

// Deregister 注销实例、清理状态机记录；在途请求在连接前复核时会被拦截。
func (m *Mesh) Deregister(id string) error {
	if err := m.Registry.Deregister(id); err != nil {
		return err
	}
	m.Checker.Remove(id)
	return nil
}

// ReportProbe 供测试/外部探针直接上报一次探测结果。
func (m *Mesh) ReportProbe(id string, probeErr error) (model.HealthState, string) {
	state, reason := m.Checker.OnProbeResult(id, probeErr)
	m.Registry.SetHealth(id, state)
	return state, reason
}

// Call 发起一次端到端请求。
func (m *Mesh) Call(ctx context.Context, req model.RequestContext) (*forwarder.Result, error) {
	return m.Dispatch.Dispatch(ctx, req)
}

// UpdateRules 原子热更新路由规则；失败保留旧版本。
func (m *Mesh) UpdateRules(next router.Rules) error {
	return m.Router.Update(next)
}

// SweepResultCache 主动清理所有已过期的复用结论，返回删除条数。
// 不调用也安全：结论在下次命中时会被惰性清理；此入口供周期性维护与可观测性使用。
func (m *Mesh) SweepResultCache() int {
	return m.Fwd.Dedupe().Sweep()
}

// CachedResultCount 返回当前保留的结论条数（成功与失败均计入）。
func (m *Mesh) CachedResultCount() int {
	return m.Fwd.Dedupe().Len()
}
