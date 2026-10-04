package model

import (
	"errors"
	"fmt"
)

// ErrorKind 用于区分不同的失败原因，调用方可以据此做出确定性的降级决策。
type ErrorKind string

const (
	// ErrDuplicateRegistration 重复注册：实例 ID 仍在册。
	ErrDuplicateRegistration ErrorKind = "duplicate_registration"
	// ErrInstanceDead 实例已注销（已失效），其请求必须被拒绝。
	ErrInstanceDead ErrorKind = "instance_dead"
	// ErrInstanceNotRegistered 注销一个不存在的实例。
	ErrInstanceNotRegistered ErrorKind = "instance_not_registered"
	// ErrInstanceUnhealthy 实例处于摘除态。
	ErrInstanceUnhealthy ErrorKind = "instance_unhealthy"
	// ErrInstanceRecovering 实例处于恢复观察期。
	ErrInstanceRecovering ErrorKind = "instance_recovering"
	// ErrNoHealthyInstance 目标服务没有任何可路由实例。
	ErrNoHealthyInstance ErrorKind = "no_healthy_instance"
	// ErrProbeFailed 健康探测请求本身失败。
	ErrProbeFailed ErrorKind = "probe_failed"
	// ErrConnectFailed 连接后端失败（连接被拒/立即失败）。
	ErrConnectFailed ErrorKind = "connect_failed"
	// ErrTimeout 请求超时。
	ErrTimeout ErrorKind = "timeout"
	// ErrUpstream5xx 后端返回 5xx。
	ErrUpstream5xx ErrorKind = "upstream_5xx"
	// ErrUpstream4xx 后端返回 4xx（客户端错误，默认不重试）。
	ErrUpstream4xx ErrorKind = "upstream_4xx"
	// ErrNoRetryBudget 重试次数已用尽。
	ErrNoRetryBudget ErrorKind = "no_retry_budget"
	// ErrRuleInvalid 路由规则校验失败。
	ErrRuleInvalid ErrorKind = "rule_invalid"
	// ErrServiceNotFound 路由规则中不存在该服务。
	ErrServiceNotFound ErrorKind = "service_not_found"
	// ErrRetryDisabled 当前规则显式关闭重试。
	ErrRetryDisabled ErrorKind = "retry_disabled"
)

// RetryableByDefault 返回该错误类别是否属于默认可重试的传输类错误。
// 4xx 与显式不可重试的错误不在其中。
func (k ErrorKind) RetryableByDefault() bool {
	switch k {
	case ErrConnectFailed, ErrTimeout, ErrUpstream5xx:
		return true
	default:
		return false
	}
}

// CallError 携带可区分的错误类型、实例信息与判定依据。
type CallError struct {
	Kind       ErrorKind
	InstanceID string
	Detail     string
	cause      error
}

func (e *CallError) Error() string {
	s := string(e.Kind)
	if e.InstanceID != "" {
		s += " instance=" + e.InstanceID
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	if e.cause != nil {
		s += " (" + e.cause.Error() + ")"
	}
	return s
}

// Unwrap 支持 errors.Is/As 穿透。
func (e *CallError) Unwrap() error { return e.cause }

// NewError 构造一个 CallError。
func NewError(kind ErrorKind, instanceID, detail string) *CallError {
	return &CallError{Kind: kind, InstanceID: instanceID, Detail: detail}
}

// WrapError 在 CallError 外附带底层错误。
func WrapError(kind ErrorKind, instanceID, detail string, cause error) *CallError {
	return &CallError{Kind: kind, InstanceID: instanceID, Detail: detail, cause: cause}
}

// ErrKind 解包错误的 ErrorKind，未识别返回空串。
func ErrKind(err error) ErrorKind {
	var ce *CallError
	if errors.As(err, &ce) {
		return ce.Kind
	}
	return ""
}

// RootKind 沿 cause 链解包到最内层的 CallError 类别。
// 例如 no_retry_budget(cause=connect_failed) 的根类别是 connect_failed，
// 用于在“预算耗尽/包装降级”后仍能区分最初导致失败的那一类原因。
func RootKind(err error) ErrorKind {
	var root ErrorKind
	for err != nil {
		var ce *CallError
		if !errors.As(err, &ce) {
			break
		}
		root = ce.Kind
		err = ce.cause
	}
	return root
}

// DescribeKind 给出错误类别的中文判定依据，供日志解释。
func DescribeKind(k ErrorKind) string {
	switch k {
	case ErrDuplicateRegistration:
		return "实例仍在注册表中，拒绝重复注册"
	case ErrInstanceDead:
		return "实例已被注销并标记失效，拒绝转发"
	case ErrInstanceNotRegistered:
		return "目标实例不在注册表中"
	case ErrInstanceUnhealthy:
		return "连续探测失败达到摘除阈值，实例已摘除"
	case ErrInstanceRecovering:
		return "实例处于恢复观察期，成功次数未达阈值"
	case ErrNoHealthyInstance:
		return "当前服务没有任何 HEALTHY 实例"
	case ErrProbeFailed:
		return "健康探测请求失败"
	case ErrConnectFailed:
		return "与后端建立连接失败"
	case ErrTimeout:
		return "在规定时间内未得到后端响应"
	case ErrUpstream5xx:
		return "后端返回 5xx 服务端错误"
	case ErrUpstream4xx:
		return "后端返回 4xx 客户端错误，不重试"
	case ErrNoRetryBudget:
		return "已达最大尝试次数，放弃并向上游报错"
	case ErrRuleInvalid:
		return "新版本规则校验未通过，保留上一版"
	case ErrServiceNotFound:
		return "当前路由规则中不存在该服务"
	case ErrRetryDisabled:
		return "规则显式关闭重试，首次失败即降级返回"
	default:
		return "未知错误类别"
	}
}

// Errf 是便捷格式化构造。
func Errf(kind ErrorKind, instanceID, format string, args ...any) *CallError {
	return NewError(kind, instanceID, fmt.Sprintf(format, args...))
}
