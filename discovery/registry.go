package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrDuplicateInstance 重复注册同一实例 ID。
var ErrDuplicateInstance = errors.New("discovery: duplicate instance registration")

// ErrInstanceNotFound 实例不存在。
var ErrInstanceNotFound = errors.New("discovery: instance not found")

// ErrInstanceDeregistered 请求落在已注销（失效）实例上。
var ErrInstanceDeregistered = errors.New("discovery: instance already deregistered")

// RejectError 携带可区分拒绝原因的错误，调用方可通过 errors.As 精确判定。
type RejectError struct {
	Reason  RejectReason
	Message string
}

func (e *RejectError) Error() string { return e.Message }

// Unwrap 允许 errors.Is 匹配对应的哨兵错误。
func (e *RejectError) Unwrap() error {
	switch e.Reason {
	case RejectDuplicateRegister:
		return ErrDuplicateInstance
	case RejectInstanceDeregistered:
		return ErrInstanceDeregistered
	case RejectInstanceNotFound:
		return ErrInstanceNotFound
	default:
		return nil
	}
}

func rejectError(reason RejectReason, format string, args ...any) *RejectError {
	return &RejectError{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// Snapshot 是某一时刻注册中心的不可变视图（实例为克隆，后续变更不影响快照）。
type Snapshot struct {
	Version   uint64
	TakenAt   time.Time
	Instances map[string]*Instance
}

// HealthyOf 返回快照中某服务的健康（可路由）实例。
func (s *Snapshot) HealthyOf(service string) []*Instance {
	out := make([]*Instance, 0)
	for _, inst := range s.Instances {
		if inst.Service == service && inst.Routable() {
			out = append(out, inst)
		}
	}
	return out
}

// Registry 线程安全的本地服务注册中心。
// 任何写操作（注册/注销/状态迁移）都会单调递增版本号并广播变更。
type Registry struct {
	mu        sync.RWMutex
	instances map[string]*Instance
	version   uint64
	changed   chan struct{}
}

// NewRegistry 创建注册中心。
func NewRegistry() *Registry {
	return &Registry{
		instances: make(map[string]*Instance),
		changed:   make(chan struct{}),
	}
}

// Register 注册一个实例。
//   - ID 已存在且仍有效：返回 RejectDuplicateRegister；
//   - ID 曾存在但已注销（墓碑）：返回 RejectInstanceDeregistered；
//
// 两种失败原因可通过 RejectError.Reason 区分。
func (r *Registry) Register(inst *Instance) error {
	if inst == nil || inst.ID == "" {
		return rejectError(RejectInstanceNotFound, "discovery: nil instance or empty id")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.instances[inst.ID]; ok {
		if old.state == StateDeregistered {
			return rejectError(RejectInstanceDeregistered,
				"discovery: instance %q has been deregistered and cannot be re-registered", inst.ID)
		}
		return rejectError(RejectDuplicateRegister,
			"discovery: instance %q already registered for service %q", inst.ID, old.Service)
	}
	now := time.Now()
	inst.registered = now
	inst.updatedAt = now
	if inst.state == 0 {
		inst.state = StateRegistered
	}
	if inst.Tags == nil {
		inst.Tags = map[string]string{}
	}
	r.instances[inst.ID] = inst
	r.bumpLocked()
	return nil
}

// Deregister 注销实例：
//   - 不存在：RejectInstanceNotFound；
//   - 已注销：RejectInstanceDeregistered（重复注销必须被明确拒绝）。
func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst, ok := r.instances[id]
	if !ok {
		return rejectError(RejectInstanceNotFound, "discovery: instance %q not found", id)
	}
	if inst.state == StateDeregistered {
		return rejectError(RejectInstanceDeregistered,
			"discovery: instance %q is already deregistered", id)
	}
	inst.state = StateDeregistered
	inst.updatedAt = time.Now()
	r.bumpLocked()
	return nil
}

// Get 返回实例的克隆快照与是否存在。返回克隆以避免调用方持有可变指针。
func (r *Registry) Get(id string) (*Instance, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	inst, ok := r.instances[id]
	if !ok {
		return nil, false
	}
	return inst.clone(), true
}

// SetState 在合法前提下改变实例状态；已注销实例不允许再迁移。
func (r *Registry) SetState(id string, state InstanceState) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst, ok := r.instances[id]
	if !ok || inst.state == StateDeregistered {
		return false
	}
	if inst.state == state {
		return true
	}
	inst.state = state
	inst.updatedAt = time.Now()
	r.bumpLocked()
	return true
}

// Transition 原子地更新健康状态与连续失败计数（供健康检查器使用）。
// 已注销实例的状态迁移一律拒绝（摘除/恢复都不得作用于失效实例）。
func (r *Registry) Transition(id string, state InstanceState, failCount int) (*Instance, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst, ok := r.instances[id]
	if !ok {
		return nil, false
	}
	if inst.state == StateDeregistered {
		return inst.clone(), false
	}
	inst.state = state
	inst.failCount = failCount
	inst.updatedAt = time.Now()
	r.bumpLocked()
	return inst.clone(), true
}

// HealthyInstances 返回某服务当前可路由实例的克隆列表。
func (r *Registry) HealthyInstances(service string) []*Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.healthyLocked(service)
}

func (r *Registry) healthyLocked(service string) []*Instance {
	out := make([]*Instance, 0)
	for _, inst := range r.instances {
		if inst.Service == service && inst.Routable() {
			out = append(out, inst.clone())
		}
	}
	return out
}

// Snapshot 取一份不可变快照（深拷贝），并发路由可脱离锁长期持有。
func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := make(map[string]*Instance, len(r.instances))
	for id, inst := range r.instances {
		m[id] = inst.clone()
	}
	return Snapshot{Version: r.version, TakenAt: time.Now(), Instances: m}
}

// Version 返回当前版本号。
func (r *Registry) Version() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
}

// WatchVersion 阻塞直到注册中心版本大于 version，或 ctx 取消。
// 返回最新版本号，用于热更新/测试等待收敛。
func (r *Registry) WatchVersion(ctx context.Context, version uint64) (uint64, error) {
	for {
		r.mu.RLock()
		cur := r.version
		ch := r.changed
		r.mu.RUnlock()
		if cur > version {
			return cur, nil
		}
		select {
		case <-ctx.Done():
			return cur, ctx.Err()
		case <-ch:
		}
	}
}

// bumpLocked 递增版本并广播；调用方必须持有写锁。
func (r *Registry) bumpLocked() {
	r.version++
	close(r.changed)
	r.changed = make(chan struct{})
}
