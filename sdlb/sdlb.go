// Package sdlb 把注册中心、健康检查、规则热更新与转发重试整合为一个门面，
// 面向调用方提供注册/注销、规则更新与带解释结论的调用入口。
package sdlb

import (
	"context"
	"fmt"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
	"github.com/highcumontoa/service-discovery-lb-go/forward"
	"github.com/highcumontoa/service-discovery-lb-go/health"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/route"
)

// Config 门面配置。
type Config struct {
	Health health.Config
}

// DefaultConfig 默认配置。
func DefaultConfig() Config { return Config{Health: health.DefaultConfig()} }

// SDLB 服务发现 + 负载均衡门面。
type SDLB struct {
	reg       *discovery.Registry
	table     *route.Table
	manager   *route.Manager
	checker   *health.Checker
	fwd       *forward.Forwarder
	probeFunc health.ProbeFunc
}

// New 创建门面。probe 为 nil 时不启用后台健康检查（注册实例需手动置健康）。
func New(cfg Config, probe health.ProbeFunc) *SDLB {
	reg := discovery.NewRegistry()
	table := route.NewTable()
	manager := route.NewManager(table)
	fwd := forward.NewForwarder(reg)
	s := &SDLB{
		reg:       reg,
		table:     table,
		manager:   manager,
		fwd:       fwd,
		probeFunc: probe,
	}
	if probe != nil {
		s.checker = health.NewChecker(reg, probe, cfg.Health)
		fwd.WithFailureReporter(s.checker)
	}
	return s
}

// Registry 暴露注册中心。
func (s *SDLB) Registry() *discovery.Registry { return s.reg }

// Rules 暴露规则管理器。
func (s *SDLB) Rules() *route.Manager { return s.manager }

// Forwarder 暴露转发器。
func (s *SDLB) Forwarder() *forward.Forwarder { return s.fwd }

// Checker 暴露健康检查器（可能为 nil）。
func (s *SDLB) Checker() *health.Checker { return s.checker }

// Start 启动后台健康检查循环。
func (s *SDLB) Start(ctx context.Context) {
	if s.checker != nil {
		s.checker.Start(ctx)
	}
}

// RegisterInstance 注册一个实例（初始 registered，待健康探测转 healthy）。
func (s *SDLB) RegisterInstance(id, service, addr string, tags map[string]string) error {
	return s.reg.Register(discovery.NewInstance(id, service, addr, tags))
}

// MarkHealthy 直接把实例标记为健康（无健康检查器或测试时使用）。
func (s *SDLB) MarkHealthy(id string) error {
	if !s.reg.SetState(id, discovery.StateHealthy) {
		return fmt.Errorf("sdlb: cannot mark %q healthy (not found or deregistered)", id)
	}
	return nil
}

// DeregisterInstance 注销实例；重复注销/失效实例返回可区分错误。
func (s *SDLB) DeregisterInstance(id string) error { return s.reg.Deregister(id) }

// Call 按当前规则版本快照转发一次请求。
// 在途请求固定使用进入时的规则版本，热更新不会中途改变其策略。
func (s *SDLB) Call(ctx context.Context, service, hashKey string,
	invoke forward.InvokeFunc) (*forward.Result, error) {
	r, ver := s.table.Resolve(service)
	opts := forward.Options{
		Service:       service,
		HashKey:       hashKey,
		Strategy:      r.Strategy,
		Timeout:       r.Timeout,
		MaxAttempts:   r.MaxAttempts,
		RetrySameNode: r.RetrySameNode,
		FailOpen:      r.FailOpen,
		Weights:       r.Weights,
		RuleVersion:   ver,
	}
	return s.fwd.Do(ctx, opts, invoke)
}

// SetRules 热更新全部规则；校验失败保留上一版。
func (s *SDLB) SetRules(rules []*route.Rule) (uint64, error) {
	return s.manager.Update(rules)
}

// ExplainStatus 打印某服务实例状态快照（供测试日志）。
func (s *SDLB) ExplainStatus(service string) string {
	snap := s.reg.Snapshot()
	out := fmt.Sprintf("registry version=%d service=%s:", snap.Version, service)
	for _, inst := range snap.Instances {
		if inst.Service != service {
			continue
		}
		routable := "NO-ROUTE"
		if inst.Routable() {
			routable = "ROUTABLE"
		}
		out += fmt.Sprintf("\n  [%s] %s addr=%s state=%s failCount=%d %s",
			routable, inst.ID, inst.Addr, inst.State(), inst.FailCount(),
			"updated="+inst.UpdatedAt().Format("15:04:05.000"))
	}
	return out
}

var _ lb.Strategy
