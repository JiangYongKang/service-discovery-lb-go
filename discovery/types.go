package discovery

import "time"

// InstanceState 表示实例在注册中心中的生命周期状态。
type InstanceState int

const (
	// StateRegistered 已注册，但尚未确认健康。
	StateRegistered InstanceState = iota
	// StateHealthy 健康，可被路由。
	StateHealthy
	// StateProbing 健康状态存疑（失败恢复中），不可被路由。
	StateProbing
	// StateUnhealthy 探测失败被摘除，不可被路由。
	StateUnhealthy
	// StateDeregistered 已注销，实例失效（墓碑，不可重新注册）。
	StateDeregistered
)

func (s InstanceState) String() string { return stateNames[s] }

// RejectReason 用于明确区分拒绝请求的原因。
type RejectReason int

const (
	RejectNone RejectReason = iota
	RejectDuplicateRegister
	RejectInstanceDeregistered
	RejectInstanceUnhealthy
	RejectInstanceProbing
	RejectInstanceNotFound
)

func (r RejectReason) String() string { return rejectReasonNames[r] }

// Instance 表示一个本地模拟的后端服务实例。
type Instance struct {
	ID      string
	Service string
	Addr    string
	Tags    map[string]string

	state      InstanceState
	failCount  int
	registered time.Time
	updatedAt  time.Time
}

// NewInstance 构造一个处于 StateRegistered 状态的实例。
func NewInstance(id, service, addr string, tags map[string]string) *Instance {
	t := make(map[string]string, len(tags))
	for k, v := range tags {
		t[k] = v
	}
	return &Instance{ID: id, Service: service, Addr: addr, Tags: t, state: StateRegistered}
}

func (i *Instance) State() InstanceState    { return i.state }
func (i *Instance) FailCount() int          { return i.failCount }
func (i *Instance) RegisteredAt() time.Time { return i.registered }
func (i *Instance) UpdatedAt() time.Time    { return i.updatedAt }

// Routable 仅健康实例可被路由；probing/unhealthy/deregistered 一律不可路由。
func (i *Instance) Routable() bool { return i.state == StateHealthy }

// clone 返回值拷贝（Tags 深拷贝），快照与 Get 均通过它隔离内部状态。
func (i *Instance) clone() *Instance {
	c := *i
	if i.Tags != nil {
		c.Tags = make(map[string]string, len(i.Tags))
		for k, v := range i.Tags {
			c.Tags[k] = v
		}
	}
	return &c
}

var stateNames = [...]string{
	"registered", "healthy", "probing", "unhealthy", "deregistered",
}

var rejectReasonNames = [...]string{
	"none", "duplicate-register", "instance-deregistered",
	"instance-unhealthy", "instance-probing", "instance-not-found",
}
