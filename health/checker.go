package health

import (
	"fmt"
	"sync"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// ProbeFunc 对单个实例执行一次探测，返回 error 表示探测失败。
type ProbeFunc func(ins model.Instance) error

// Status 是单个实例的健康判定状态机视图。
type Status struct {
	State           model.HealthState
	ConsecutiveFail int
	ConsecutiveOK   int
}

func (s Status) String() string {
	return fmt.Sprintf("state=%s consecutive_fail=%d consecutive_ok=%d routeable=%t",
		s.State, s.ConsecutiveFail, s.ConsecutiveOK, s.State.Routeable())
}

// Config 是健康判定阈值。
type Config struct {
	// FailThreshold 连续失败次数达到该值 -> 摘除（UNHEALTHY）。
	FailThreshold int
	// RecoverThreshold 连续成功次数达到该值 -> 恢复（HEALTHY）；
	// 摘除后出现部分成功但未达阈值的区间为 RECOVERING。
	RecoverThreshold int
}

// Checker 维护每个实例的失败/恢复计数，按阈值摘除与恢复。
// 判定规则（确定性状态机，任何抖动都会重置连续计数）：
//
//	UNKNOWN    -- 连续成功达阈值 --> HEALTHY；连续失败达阈值 --> UNHEALTHY
//	HEALTHY    -- 连续失败达阈值 --> UNHEALTHY
//	UNHEALTHY  -- 首次成功 --> RECOVERING；连续成功达阈值 --> HEALTHY；任一失败 --> UNHEALTHY
//	RECOVERING -- 连续成功达阈值 --> HEALTHY；任一失败 --> UNHEALTHY
//
// 只有 HEALTHY 可路由；RECOVERING / UNKNOWN / UNHEALTHY 一律不进入候选集合。
type Checker struct {
	mu     sync.Mutex
	cfg    Config
	probe  ProbeFunc
	status map[string]Status
}

// New 创建检查器。
func New(cfg Config, probe ProbeFunc) *Checker {
	if cfg.FailThreshold <= 0 {
		cfg.FailThreshold = 1
	}
	if cfg.RecoverThreshold <= 0 {
		cfg.RecoverThreshold = 1
	}
	return &Checker{cfg: cfg, probe: probe, status: make(map[string]Status)}
}

// OnProbeResult 上报一次探测结果，推进状态机，返回新状态与判定依据。
func (c *Checker) OnProbeResult(id string, err error) (model.HealthState, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.status[id]
	if err != nil {
		st.ConsecutiveFail++
		st.ConsecutiveOK = 0
		switch st.State {
		case model.StateHealthy:
			if st.ConsecutiveFail >= c.cfg.FailThreshold {
				st.State = model.StateUnhealthy
				c.status[id] = st
				return st.State, fmt.Sprintf(
					"连续探测失败 %d 次达到摘除阈值 %d，HEALTHY -> UNHEALTHY，摘除实例",
					st.ConsecutiveFail, c.cfg.FailThreshold)
			}
			c.status[id] = st
			return st.State, fmt.Sprintf(
				"探测失败但未达摘除阈值（%d/%d），保持 HEALTHY 观察",
				st.ConsecutiveFail, c.cfg.FailThreshold)
		case model.StateRecovering:
			st.State = model.StateUnhealthy
			c.status[id] = st
			return st.State, "恢复观察期内探测失败，连续成功计数清零，RECOVERING -> UNHEALTHY"
		default:
			if st.ConsecutiveFail >= c.cfg.FailThreshold {
				st.State = model.StateUnhealthy
			}
			c.status[id] = st
			return st.State, fmt.Sprintf(
				"探测失败，连续失败 %d/%d，状态保持/进入 UNHEALTHY",
				st.ConsecutiveFail, c.cfg.FailThreshold)
		}
	}

	// 探测成功
	st.ConsecutiveOK++
	st.ConsecutiveFail = 0
	switch st.State {
	case model.StateHealthy:
		c.status[id] = st
		return st.State, "探测成功，实例保持 HEALTHY，可路由"
	case model.StateUnknown:
		if st.ConsecutiveOK >= c.cfg.RecoverThreshold {
			st.State = model.StateHealthy
			c.status[id] = st
			return st.State, fmt.Sprintf(
				"新实例连续探测成功 %d 次达到就绪阈值 %d，UNKNOWN -> HEALTHY，加入路由",
				st.ConsecutiveOK, c.cfg.RecoverThreshold)
		}
		c.status[id] = st
		return st.State, fmt.Sprintf(
			"新实例探测成功但未达就绪阈值（%d/%d），保持 UNKNOWN，暂不路由",
			st.ConsecutiveOK, c.cfg.RecoverThreshold)
	case model.StateUnhealthy:
		if st.ConsecutiveOK >= c.cfg.RecoverThreshold {
			st.State = model.StateHealthy
			c.status[id] = st
			return st.State, fmt.Sprintf(
				"连续探测成功 %d 次达到恢复阈值 %d，UNHEALTHY -> HEALTHY，恢复路由",
				st.ConsecutiveOK, c.cfg.RecoverThreshold)
		}
		st.State = model.StateRecovering
		c.status[id] = st
		return st.State, fmt.Sprintf(
			"摘除后探测成功但恢复未完成（%d/%d），UNHEALTHY -> RECOVERING，仍不路由",
			st.ConsecutiveOK, c.cfg.RecoverThreshold)
	default: // RECOVERING
		if st.ConsecutiveOK >= c.cfg.RecoverThreshold {
			st.State = model.StateHealthy
			c.status[id] = st
			return st.State, fmt.Sprintf(
				"恢复期连续成功达阈值 %d，RECOVERING -> HEALTHY，恢复路由",
				c.cfg.RecoverThreshold)
		}
		c.status[id] = st
		return st.State, fmt.Sprintf(
			"恢复观察中，连续成功 %d/%d，保持 RECOVERING，仍不路由",
			st.ConsecutiveOK, c.cfg.RecoverThreshold)
	}
}

// GetStatus 查询实例状态机视图。
func (c *Checker) GetStatus(id string) Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status[id]
}

// Reset 在实例注册/重新注册时清空其判定状态为 UNKNOWN。
func (c *Checker) Reset(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status[id] = Status{State: model.StateUnknown}
}

// Remove 在实例注销时清除其状态机记录。
func (c *Checker) Remove(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.status, id)
}

// Tick 对一个实例执行一次探测并推进状态机。
func (c *Checker) Tick(ins model.Instance) (model.HealthState, string) {
	return c.OnProbeResult(ins.ID, c.probe(ins))
}
