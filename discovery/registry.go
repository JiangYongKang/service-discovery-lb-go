package discovery

import (
	"sync"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// Registry 维护服务实例注册表，提供并发安全的注册/注销与快照。
//
// 不变量：
//  1. 已在册实例重复注册 -> ErrDuplicateRegistration；
//  2. 注销是幂等的：首次注销移除实例并标记失效，重复注销返回 ErrInstanceNotRegistered；
//  3. 已失效（已注销）实例可被 IsDead 识别，转发层必须以 ErrInstanceDead 拒绝；
//  4. 只有健康状态为 HEALTHY 的实例才会进入可路由集合；
//  5. 每次成员或健康状态变化都产生新版本号，快照不可变，在途请求可安全持有。
type Registry struct {
	mu        sync.RWMutex
	version   uint64
	instances map[string]model.Instance
	dead      map[string]bool
	healthy   map[string]model.HealthState
	svcIndex  map[string]map[string]struct{}
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{
		instances: make(map[string]model.Instance),
		dead:      make(map[string]bool),
		healthy:   make(map[string]model.HealthState),
		svcIndex:  make(map[string]map[string]struct{}),
	}
}

// Register 注册实例；重复注册必须被明确拒绝。
// 同名 ID 在被注销后允许重新注册（重新上线），此时失效标记与健康状态被重置。
func (r *Registry) Register(ins model.Instance) error {
	if ins.ID == "" || ins.Service == "" {
		return model.NewError(model.ErrRuleInvalid, ins.ID, "注册信息缺少 ID 或 Service")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.instances[ins.ID]; ok {
		return model.Errf(model.ErrDuplicateRegistration, ins.ID,
			"service=%s addr=%s 仍在注册表中", ins.Service, ins.Addr)
	}
	r.instances[ins.ID] = ins
	delete(r.dead, ins.ID)
	r.healthy[ins.ID] = model.StateUnknown
	set, ok := r.svcIndex[ins.Service]
	if !ok {
		set = make(map[string]struct{})
		r.svcIndex[ins.Service] = set
	}
	set[ins.ID] = struct{}{}
	r.version++
	return nil
}

// Deregister 注销实例；并发注销保持幂等语义，已注销实例被标记为失效。
func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	ins, ok := r.instances[id]
	if !ok {
		if r.dead[id] {
			return model.NewError(model.ErrInstanceDead, id, "实例此前已注销并失效")
		}
		return model.NewError(model.ErrInstanceNotRegistered, id, "实例不在注册表中")
	}
	delete(r.instances, id)
	delete(r.healthy, id)
	if set := r.svcIndex[ins.Service]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(r.svcIndex, ins.Service)
		}
	}
	r.dead[id] = true
	r.version++
	return nil
}

// SetHealth 更新实例健康状态（由健康检查器驱动）。
// 已注销实例的状态更新将被忽略。
func (r *Registry) SetHealth(id string, state model.HealthState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.instances[id]; !ok {
		return
	}
	if r.healthy[id] != state {
		r.healthy[id] = state
		r.version++
	}
}

// IsDead 报告实例是否曾被注销且未重新注册（已失效）。
func (r *Registry) IsDead(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.dead[id]
}

// Get 查询单个实例及其当前健康状态。
func (r *Registry) Get(id string) (model.Instance, model.HealthState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ins, ok := r.instances[id]
	if !ok {
		return model.Instance{}, model.StateUnknown, false
	}
	return ins, r.healthy[id], true
}

// HealthyInstances 返回某服务当前可路由实例 ID 集合（仅 HEALTHY）。
func (r *Registry) HealthyInstances(service string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := r.svcIndex[service]
	ids := make([]string, 0, len(set))
	for id := range set {
		if r.healthy[id] == model.StateHealthy {
			ids = append(ids, id)
		}
	}
	return ids
}

// Snapshot 返回带版本号的不可变快照，供热更新期间在途请求持有。
func (r *Registry) Snapshot() model.Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]model.Instance, 0, len(r.instances))
	health := make(map[string]model.HealthState, len(r.healthy))
	for id, ins := range r.instances {
		list = append(list, ins)
		health[id] = r.healthy[id]
	}
	return model.Snapshot{Version: r.version, Instances: list, Healthy: health}
}

// Version 返回当前注册表版本号。
func (r *Registry) Version() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
}
