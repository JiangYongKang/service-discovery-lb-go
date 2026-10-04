package forwarder

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// 结果复用的默认保留参数：可通过 DedupeConfig 覆盖。
const (
	defaultSuccessTTL    = 30 * time.Second
	defaultFailureTTL    = 5 * time.Second
	defaultCacheCapacity = 4096
)

// OutcomeKind 区分缓存结论是成功还是失败——失败结论绝不能被后续调用当成成功复用。
type OutcomeKind int

const (
	// OutcomeSuccess  leader 最终成功（2xx/3xx）。
	OutcomeSuccess OutcomeKind = iota
	// OutcomeFailure  leader 最终失败，ErrKind 保留原始失败类别。
	OutcomeFailure
)

// CachedOutcome 是一个业务键被收敛后的最终结论快照。
// 同一业务键在保留窗口内的所有调用（并发在途 / 窗口内重放）都复用它。
type CachedOutcome struct {
	Kind       OutcomeKind
	InstanceID string
	StatusCode int
	// 仅失败结论使用：保留最终错误类别（可能是 no_retry_budget 等包装类别）。
	ErrKind model.ErrorKind
	// CauseKind 保留失败原因链最内层的“原始那一类”，
	// 如 no_retry_budget 下最初的 connect_failed/timeout/upstream_5xx，
	// 使失败重放既能看到降级结论，也能区分最初原因。
	CauseKind model.ErrorKind
	Detail    string
	// CachedAt 为结论发布时刻，保留窗口从该时刻起算。
	CachedAt time.Time
}

// DedupeConfig 配置结果复用缓存的保留窗口与容量上限。
type DedupeConfig struct {
	// SuccessTTL 成功结论保留时长；<=0 使用默认值。
	SuccessTTL time.Duration
	// FailureTTL 失败结论保留时长；<=0 使用默认值。
	// 失败结论同样可被一致复用，但有独立（通常更短）的保留窗口。
	FailureTTL time.Duration
	// Capacity 保留结论总条数上限；<=0 使用默认值。超限时按 LRU 淘汰最久未被访问的键。
	Capacity int
	// Now 注入时钟（测试使用）；为 nil 使用 time.Now。
	Now func() time.Time
}

// cachedEntry 是 map 中的条目，elem 指向 LRU 链表中的位置。
type cachedEntry struct {
	outcome CachedOutcome
	elem    *list.Element
}

// DedupeStore 保存 业务键 -> 最终结论，并对在途调用做收敛。
//
// 语义：
//   - 同一业务键下，只有第一个调用（leader）真正访问后端；
//   - leader 在途期间到达的调用（follower）等待同一结论，不重复访问后端；
//   - leader 结束（成功或“最终”失败）后发布结论，等待者与窗口内后来者全部复用；
//   - 成功与失败结论分别按 SuccessTTL / FailureTTL 保留，过期或超容量被安全淘汰；
//   - 淘汰/过期之后同键再来的调用视为一次全新请求，正常执行，不会被卡住或报错。
type DedupeStore struct {
	successTTL time.Duration
	failureTTL time.Duration
	capacity   int
	now        func() time.Time

	mu      sync.Mutex
	done    map[string]cachedEntry // 已发布结论
	waiters map[string][]chan struct{}
	ll      *list.List // LRU：表头最近访问，表尾最久未访问
	pos     map[string]*list.Element
}

// NewDedupeStore 使用默认保留参数创建缓存。
func NewDedupeStore() *DedupeStore {
	return NewDedupeStoreWith(DedupeConfig{})
}

// NewDedupeStoreWith 使用指定参数创建缓存。
func NewDedupeStoreWith(cfg DedupeConfig) *DedupeStore {
	if cfg.SuccessTTL <= 0 {
		cfg.SuccessTTL = defaultSuccessTTL
	}
	if cfg.FailureTTL <= 0 {
		cfg.FailureTTL = defaultFailureTTL
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = defaultCacheCapacity
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &DedupeStore{
		successTTL: cfg.SuccessTTL,
		failureTTL: cfg.FailureTTL,
		capacity:   cfg.Capacity,
		now:        cfg.Now,
		done:       make(map[string]cachedEntry),
		waiters:    make(map[string][]chan struct{}),
		ll:         list.New(),
		pos:        make(map[string]*list.Element),
	}
}

// ttlFor 返回某类结论的保留窗口。
func (s *DedupeStore) ttlFor(k OutcomeKind) time.Duration {
	if k == OutcomeFailure {
		return s.failureTTL
	}
	return s.successTTL
}

// sweepExpiredLocked 惰性清理所有过期结论。调用方必须持有 mu。
func (s *DedupeStore) sweepExpiredLocked(now time.Time) {
	for key, e := range s.done {
		if now.Sub(e.outcome.CachedAt) >= s.ttlFor(e.outcome.Kind) {
			s.removeLocked(key, e)
		}
	}
}

// removeLocked 删除一个已发布结论及其 LRU 位置。调用方必须持有 mu。
func (s *DedupeStore) removeLocked(key string, e cachedEntry) {
	if e.elem != nil {
		s.ll.Remove(e.elem)
	}
	delete(s.done, key)
}

// acquire 尝试占位。
//   - leader=true：调用方负责真正发起请求，并在得到“最终”结论后调用
//     complete / completeFailure（重试中途的失败不得提前发布）；
//   - leader=false：wait 在 leader 结论发布时关闭，之后通过 Lookup / lookupOutcome 读取。
//     已发布且仍在保留窗口内的结论会立即返回一个已关闭的 channel。
func (s *DedupeStore) acquire(key string) (leader bool, wait <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepExpiredLocked(now)
	if e, ok := s.done[key]; ok {
		// 命中保留窗口内结论：刷新 LRU 位置，立即复用。
		s.ll.MoveToFront(e.elem)
		ch := make(chan struct{})
		close(ch)
		return false, ch
	}
	if ws, inflight := s.waiters[key]; inflight {
		ch := make(chan struct{})
		s.waiters[key] = append(ws, ch)
		return false, ch
	}
	s.waiters[key] = nil
	return true, nil
}

// publish 发布最终结论：写入缓存、按容量做 LRU 淘汰、唤醒全部等待者。
func (s *DedupeStore) publish(key string, o CachedOutcome) {
	o.CachedAt = s.now()
	s.mu.Lock()
	if old, ok := s.done[key]; ok {
		s.removeLocked(key, old)
	}
	el := s.ll.PushFront(key)
	s.done[key] = cachedEntry{outcome: o, elem: el}
	// 容量上限：淘汰最久未被访问的已完成结论（在途键不在此结构中，不会被误删）。
	for s.ll.Len() > s.capacity {
		victim := s.ll.Back()
		if victim == nil {
			break
		}
		vKey := victim.Value.(string)
		if e, ok := s.done[vKey]; ok {
			s.removeLocked(vKey, e)
		} else {
			s.ll.Remove(victim)
		}
	}
	ws := s.waiters[key]
	delete(s.waiters, key)
	s.mu.Unlock()
	for _, ch := range ws {
		close(ch)
	}
}

// complete 由 leader 在请求最终成功时调用。
func (s *DedupeStore) complete(key, instanceID string, statusCode int) {
	s.publish(key, CachedOutcome{
		Kind: OutcomeSuccess, InstanceID: instanceID, StatusCode: statusCode,
	})
}

// completeFailure 由 leader 收到最终失败结论时调用。
// kind 为最终类别（可能是 no_retry_budget），causeKind 为原因链最内层类别；
// 失败结论携带可区分的错误类别，等待者复用同一个失败，而不会被当成成功。
func (s *DedupeStore) completeFailure(key string, kind, causeKind model.ErrorKind, instanceID, detail string) {
	s.publish(key, CachedOutcome{
		Kind: OutcomeFailure, InstanceID: instanceID,
		ErrKind: kind, CauseKind: causeKind, Detail: detail,
	})
}

// lookupOutcome 读取已发布结论；结论不存在或已过期时返回 false。
func (s *DedupeStore) lookupOutcome(key string) (CachedOutcome, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, found := s.done[key]
	if !found {
		return CachedOutcome{}, false
	}
	if s.now().Sub(e.outcome.CachedAt) >= s.ttlFor(e.outcome.Kind) {
		s.removeLocked(key, e)
		return CachedOutcome{}, false
	}
	s.ll.MoveToFront(e.elem)
	return e.outcome, true
}

// Lookup 兼容旧调用点：只返回“成功”结论；失败结论返回 ok=false，绝不伪装成成功。
func (s *DedupeStore) Lookup(key string) (instanceID string, statusCode int, ok bool) {
	o, found := s.lookupOutcome(key)
	if !found || o.Kind != OutcomeSuccess {
		return "", 0, false
	}
	return o.InstanceID, o.StatusCode, true
}

// Len 返回当前缓存的有效结论条数（含成功与失败，过期条目先惰性清理），仅供测试与观测。
func (s *DedupeStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepExpiredLocked(s.now())
	return len(s.done)
}

// Delete 主动淘汰指定键；键不存在或在途时为空操作。淘汰后同键再来即全新请求。
func (s *DedupeStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.done[key]; ok {
		s.removeLocked(key, e)
	}
}

// waitForConclusion 等待 leader 发布结论；等待过程受调用方 ctx 约束，
// 保证 follower 不会因任何意外无限挂起，在可预期时间内返回明确结论。
func waitForConclusion(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return model.WrapError(model.ErrTimeout, "", "等待同键在途结论期间请求被取消", ctx.Err())
		}
		return model.WrapError(model.ErrTimeout, "", "等待同键在途结论超过截止时间", ctx.Err())
	}
}
