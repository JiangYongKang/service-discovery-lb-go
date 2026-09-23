package health

import (
	"context"
	"sync"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// HealthSink 接收状态迁移结果（通常是注册表）。
type HealthSink interface {
	SetHealth(id string, state model.HealthState)
}

// DiscoveryView 提供需要探测的实例清单。
type DiscoveryView interface {
	Snapshot() model.Snapshot
}

// Monitor 周期性地对在册实例执行探测，并把状态机结论同步到注册表。
// 探测间隔内的状态变化只可能来自完整的状态机迁移，因此健康集合的变化确定且可解释。
type Monitor struct {
	checker  *Checker
	view     DiscoveryView
	sink     HealthSink
	interval time.Duration
	logf     func(format string, args ...any)

	mu      sync.Mutex
	running bool
	stop    chan struct{}
	wg      sync.WaitGroup
}

// NewMonitor 创建监控器。
func NewMonitor(checker *Checker, view DiscoveryView, sink HealthSink, interval time.Duration,
	logf func(format string, args ...any)) *Monitor {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Monitor{checker: checker, view: view, sink: sink, interval: interval, logf: logf}
}

// RunOnce 执行一轮探测（同步，便于测试确定性驱动）。
// 返回每实例的新状态与判定依据。
func (m *Monitor) RunOnce() map[string]StatusReport {
	snap := m.view.Snapshot()
	byID := make(map[string]model.Instance, len(snap.Instances))
	for _, ins := range snap.Instances {
		byID[ins.ID] = ins
	}
	return m.run(byID)
}

// RunOnceWith 对显式给定的实例集合执行一轮探测（同步、确定性，便于测试驱动）。
func (m *Monitor) RunOnceWith(instances map[string]model.Instance) map[string]StatusReport {
	return m.run(instances)
}

func (m *Monitor) run(instances map[string]model.Instance) map[string]StatusReport {
	reports := make(map[string]StatusReport, len(instances))
	for _, ins := range instances {
		state, reason := m.checker.Tick(ins)
		m.sink.SetHealth(ins.ID, state)
		st := m.checker.GetStatus(ins.ID)
		reports[ins.ID] = StatusReport{
			Instance: ins, State: state, Status: st, Reason: reason,
		}
		m.logf("[health] service=%s instance=%s %s | 判定依据: %s",
			ins.Service, ins.ID, st, reason)
	}
	return reports
}

// StatusReport 描述一次探测后的状态。
type StatusReport struct {
	Instance model.Instance
	State    model.HealthState
	Status   Status
	Reason   string
}

// Start 启动后台探测循环。
func (m *Monitor) Start(ctx context.Context) {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.stop = make(chan struct{})
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stop:
				return
			case <-ticker.C:
				m.RunOnce()
			}
		}
	}()
}

// Stop 停止后台循环。
func (m *Monitor) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	close(m.stop)
	m.running = false
	m.mu.Unlock()
	m.wg.Wait()
}

// 保证 discovery.Registry 可作为 sink 与 view 使用（编译期检查）。
var (
	_ HealthSink    = (*discovery.Registry)(nil)
	_ DiscoveryView = (*discovery.Registry)(nil)
)
