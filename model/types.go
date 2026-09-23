package model

import "time"

// HealthState 表示实例的健康状态。
type HealthState int

const (
	// StateUnknown 未参与健康探测（新注册初始态）。
	StateUnknown HealthState = iota
	// StateHealthy 健康，可被路由。
	StateHealthy
	// StateUnhealthy 探测失败达到摘除阈值，不可路由。
	StateUnhealthy
	// StateRecovering 恢复观察期，不可路由。
	StateRecovering
)

// String 返回状态的可解释名称。
func (s HealthState) String() string {
	switch s {
	case StateHealthy:
		return "HEALTHY"
	case StateUnhealthy:
		return "UNHEALTHY"
	case StateRecovering:
		return "RECOVERING"
	default:
		return "UNKNOWN"
	}
}

// Routeable 报告该状态是否允许参与路由选择。
// 只有显式 HEALTHY 的实例可路由；UNKNOWN / UNHEALTHY / RECOVERING 均不可路由。
func (s HealthState) Routeable() bool { return s == StateHealthy }

// Instance 描述一个本地后端进程实例。
type Instance struct {
	ID      string
	Service string
	Addr    string
	Meta    map[string]string
}

// Snapshot 是某一时刻实例注册表的不可变快照。
type Snapshot struct {
	Version   uint64
	Instances []Instance
	Healthy   map[string]HealthState
}

// RequestContext 是一次转发请求的上下文。
type RequestContext struct {
	Service        string
	Method         string
	Path           string
	IdempotencyKey string
	Header         map[string]string
	Timeout        time.Duration
}

// Attempt 记录一次尝试的实例与结果，用于可解释日志与判定依据输出。
type Attempt struct {
	InstanceID string
	StatusCode int
	ErrKind    ErrorKind
	Reason     string
}
