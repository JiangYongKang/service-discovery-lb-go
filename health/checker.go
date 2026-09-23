package health

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

// ProbeFunc 对单个实例执行一次健康探测；nil error 视为健康。
type ProbeFunc func(ctx context.Context, inst *discovery.Instance) error

// Config 健康检查参数。
type Config struct {
	Interval         time.Duration // 正常探测周期
	Timeout          time.Duration // 单次探测超时
	FailureThreshold int           // 连续失败多少次后摘除（unhealthy）
	SuccessThreshold int           // 恢复中需连续成功多少次才回到 healthy
	ProbeInterval    time.Duration // 恢复中（probing）实例的探测周期
}

// DefaultConfig 默认配置。
func DefaultConfig() Config {
	return Config{
		Interval:         200 * time.Millisecond,
		Timeout:          100 * time.Millisecond,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		ProbeInterval:    100 * time.Millisecond,
	}
}

// Decision 记录一次健康判定的依据，便于日志解释与测试断言。
type Decision struct {
	InstanceID string
	Service    string
	OldState   discovery.InstanceState
	NewState   discovery.InstanceState
	FailCount  int
	Healthy    bool // 本次探测结果
	Reason     string
	At         time.Time
}

func (d Decision) String() string {
	return "health-decision instance=" + d.InstanceID +
		" service=" + d.Service +
		" " + d.OldState.String() + "->" + d.NewState.String() +
		" probeOK=" + boolStr(d.Healthy) +
		" failCount=" + itoa(d.FailCount) +
		" reason=" + d.Reason
}

// Checker 周期性对注册实例做健康探测，驱动状态机：
//
//	healthy --1次失败--> probing --连续失败达阈值--> unhealthy
//	unhealthy --1次成功--> probing --连续成功达阈值--> healthy
//
// probing（失败/恢复中）与 unhealthy 实例均不可被路由；
// deregistered 实例永不参与状态迁移。
type Checker struct {
	reg   *discovery.Registry
	probe ProbeFunc
	cfg   Config
	log   *log.Logger

	mu        sync.Mutex
	streakOK  int // 保留字段
	fails     map[string]int
	oks       map[string]int
	lastProbe map[string]time.Time

	decCh chan Decision
	drop  int
}

// NewChecker 创建健康检查器。
func NewChecker(reg *discovery.Registry, probe ProbeFunc, cfg Config) *Checker {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultConfig().Interval
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultConfig().Timeout
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = DefaultConfig().FailureThreshold
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = DefaultConfig().SuccessThreshold
	}
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = DefaultConfig().ProbeInterval
	}
	return &Checker{
		reg:       reg,
		probe:     probe,
		cfg:       cfg,
		fails:     map[string]int{},
		oks:       map[string]int{},
		lastProbe: map[string]time.Time{},
		decCh:     make(chan Decision, 256),
	}
}

// WithLogger 注入日志记录器（nil 表示不输出日志）。
func (c *Checker) WithLogger(l *log.Logger) *Checker { c.log = l; return c }

// Start 启动后台探测循环，直到 ctx 取消。
func (c *Checker) Start(ctx context.Context) {
	go c.loop(ctx)
}

func (c *Checker) loop(ctx context.Context) {
	// 调度粒度取正常周期与恢复周期的最小值，确保 probing 实例更快被复检。
	granularity := c.cfg.ProbeInterval
	if granularity <= 0 || c.cfg.Interval < granularity {
		granularity = c.cfg.Interval
	}
	t := time.NewTicker(granularity)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.scanOnce(ctx)
		}
	}
}

// scanOnce 对快照中未注销实例按各自周期探测：
// probing 实例使用更短的 ProbeInterval，其余使用 Interval。
func (c *Checker) scanOnce(ctx context.Context) {
	now := time.Now()
	snap := c.reg.Snapshot()
	for _, inst := range snap.Instances {
		if inst.State() == discovery.StateDeregistered {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		interval := c.cfg.Interval
		if inst.State() == discovery.StateProbing ||
			inst.State() == discovery.StateUnhealthy {
			interval = c.cfg.ProbeInterval
		}
		c.mu.Lock()
		last := c.lastProbe[inst.ID]
		c.mu.Unlock()
		if !last.IsZero() && now.Sub(last) < interval {
			continue
		}
		c.mu.Lock()
		c.lastProbe[inst.ID] = now
		c.mu.Unlock()
		_, _ = c.check(ctx, inst.ID)
	}
}

// CheckOnce 对单个实例立即执行一轮探测并返回判定。
// 实例已注销时返回带原因的 Decision 且不做任何状态迁移。
func (c *Checker) CheckOnce(ctx context.Context, id string) (Decision, error) {
	if _, ok := c.reg.Get(id); !ok {
		d := Decision{InstanceID: id, NewState: discovery.StateDeregistered,
			Reason: "instance-not-found", At: time.Now()}
		c.emit(d)
		return d, discovery.ErrInstanceNotFound
	}
	return c.check(ctx, id)
}

func (c *Checker) check(ctx context.Context, id string) (Decision, error) {
	inst, ok := c.reg.Get(id)
	if !ok {
		d := Decision{InstanceID: id, NewState: discovery.StateDeregistered,
			Reason: "instance-gone-before-probe", At: time.Now()}
		c.emit(d)
		return d, discovery.ErrInstanceNotFound
	}

	old := inst.State()
	probeCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	perr := c.probe(probeCtx, inst)
	cancel()
	healthy := perr == nil

	c.mu.Lock()
	newState, reason := c.decideLocked(inst, healthy, perr)
	failCount := c.fails[id]
	c.mu.Unlock()

	d := Decision{
		InstanceID: id, Service: inst.Service,
		OldState: old, NewState: newState,
		FailCount: failCount, Healthy: healthy,
		Reason: reason, At: time.Now(),
	}

	// 并发注销窗口：Transition 对已注销实例返回 false，实例不会回到健康集合。
	if newState != old {
		if updated, moved := c.reg.Transition(id, newState, failCount); !moved {
			d.Reason = "transition-rejected-deregistered"
			if updated != nil {
				d.NewState = updated.State()
			}
		}
	} else if _, moved := c.reg.Transition(id, newState, failCount); !moved {
		d.Reason = "transition-rejected-deregistered"
	}
	c.emit(d)
	return d, perr
}

// decideLocked 依据状态机与连续成败计数给出下一状态；调用方持有 c.mu。
func (c *Checker) decideLocked(inst *discovery.Instance, healthy bool, perr error) (discovery.InstanceState, string) {
	id := inst.ID
	state := inst.State()
	if healthy {
		c.fails[id] = 0
		switch state {
		case discovery.StateRegistered:
			c.oks[id] = 0
			return discovery.StateHealthy, "first-probe-ok->healthy"
		case discovery.StateHealthy:
			c.oks[id] = 0
			return discovery.StateHealthy, "still-healthy"
		case discovery.StateUnhealthy:
			c.oks[id] = 1
			if 1 >= c.cfg.SuccessThreshold {
				return discovery.StateHealthy, "recovered->healthy"
			}
			return discovery.StateProbing, "unhealthy-recovering->probing"
		case discovery.StateProbing:
			c.oks[id]++
			if c.oks[id] >= c.cfg.SuccessThreshold {
				c.oks[id] = 0
				return discovery.StateHealthy, "recovery-threshold-met->healthy"
			}
			return discovery.StateProbing, "recovery-progress"
		}
		return state, "unexpected-state"
	}

	// 探测失败
	errName := "probe-error"
	if perr != nil {
		if isTimeout(perr) {
			errName = "probe-timeout"
		} else {
			errName = "probe-failed"
		}
	}
	c.oks[id] = 0
	switch state {
	case discovery.StateHealthy:
		c.fails[id] = 1
		return discovery.StateProbing, errName + ":healthy->probing(immediately-no-route)"
	case discovery.StateRegistered:
		c.fails[id]++
		if c.fails[id] >= c.cfg.FailureThreshold {
			return discovery.StateUnhealthy, errName + ":registered->unhealthy"
		}
		return discovery.StateRegistered, errName + ":registered-not-yet-healthy"
	case discovery.StateProbing:
		c.fails[id]++
		if c.fails[id] >= c.cfg.FailureThreshold {
			return discovery.StateUnhealthy, errName + ":probing->unhealthy(removed)"
		}
		return discovery.StateProbing, errName + ":still-probing-no-route"
	case discovery.StateUnhealthy:
		c.fails[id] = c.cfg.FailureThreshold
		return discovery.StateUnhealthy, errName + ":still-unhealthy"
	}
	return state, errName
}

// Decisions 返回判定记录通道（缓冲满时丢弃最旧/当前记录，不阻塞探测）。
func (c *Checker) Decisions() <-chan Decision { return c.decCh }

func (c *Checker) emit(d Decision) {
	if c.log != nil {
		c.log.Println(d.String())
	}
	select {
	case c.decCh <- d:
	default:
		c.drop++
	}
}

func isTimeout(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}
	type timeoutErr interface{ Timeout() bool }
	if te, ok := err.(timeoutErr); ok {
		return te.Timeout()
	}
	return false
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// ReportTransportFailure 由转发层在连接失败时调用。
// 确定性规则：
//   - healthy 实例立即迁入 probing（下一次请求就不会再选中它）；
//   - 失败计数 +1；达到 FailureThreshold 直接摘除为 unhealthy；
//   - 已注销/已摘除实例不会因此回到健康集合。
func (c *Checker) ReportTransportFailure(id string) {
	inst, ok := c.reg.Get(id)
	if !ok {
		return
	}
	old := inst.State()
	if old == discovery.StateDeregistered {
		c.emit(Decision{InstanceID: id, Service: inst.Service,
			OldState: old, NewState: old, Reason: "transport-failure-ignored-deregistered",
			At: time.Now()})
		return
	}

	c.mu.Lock()
	c.fails[id]++
	fails := c.fails[id]
	c.oks[id] = 0
	target := discovery.StateProbing
	reason := "transport-failure:->probing(immediately-no-route)"
	if fails >= c.cfg.FailureThreshold {
		target = discovery.StateUnhealthy
		reason = "transport-failure-threshold:->unhealthy(removed)"
	}
	c.mu.Unlock()

	if old == discovery.StateUnhealthy {
		target = discovery.StateUnhealthy
		reason = "transport-failure:already-removed"
	}
	if updated, moved := c.reg.Transition(id, target, fails); moved {
		c.emit(Decision{InstanceID: id, Service: inst.Service,
			OldState: old, NewState: target, FailCount: fails,
			Reason: reason, At: time.Now()})
	} else if updated != nil {
		c.emit(Decision{InstanceID: id, Service: inst.Service,
			OldState: old, NewState: updated.State(), FailCount: fails,
			Reason: "transport-failure-rejected-deregistered", At: time.Now()})
	}
}
