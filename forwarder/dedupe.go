package forwarder

import (
	"net/http"
	"sync"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// CachedOutcome 是一个幂等键最终收敛到的“同一份结论”。
// 成功与失败共用同一结构，由 Err 是否为 nil 区分；失败结论携带原始错误类别，
// 绝不会以“成功 + status=0”的形式被复用。
type CachedOutcome struct {
	// 成功结论字段。
	InstanceID string
	StatusCode int
	Header     http.Header
	// Attempts 是 leader 实际走过的尝试链，复用方据此仍能解释结论的来由。
	Attempts []model.Attempt
	// Err 非 nil 表示这是一份失败结论；ErrKind() 保留原始失败类别
	// （connect_failed / timeout / upstream_4xx / upstream_5xx /
	// no_healthy_instance / no_retry_budget ...）。
	Err error

	storedAt  time.Time
	expiresAt time.Time
}

// Failed 报告该结论是否为失败结论。
func (o CachedOutcome) Failed() bool { return o.Err != nil }

// DedupeConfig 配置结果复用存储。
type DedupeConfig struct {
	// TTL 每条结论（成功/失败一致）的保留时长；<=0 采用默认值。
	TTL time.Duration
	// MaxEntries 结论总条数上限；超限时按最旧发布顺序（FIFO）淘汰；<=0 采用默认值。
	MaxEntries int
	// WaitBudget 复用方等待在途 leader 发布结论的最长时间；<=0 采用默认值。
	WaitBudget time.Duration
	// Now 注入时钟，便于确定性测试；nil 时使用 time.Now。
	Now func() time.Time
}

// 默认复用参数。
const (
	defaultDedupeTTL        = 30 * time.Second
	defaultDedupeMaxEntries = 4096
	defaultDedupeWaitBudget = 10 * time.Second
)

// DedupeStore 保存 幂等键 -> 已完成结论（成功或失败）。
//
// 语义：同一幂等键下，只有第一个进入临界区的调用（leader）会真正访问后端；
// 在其完成前到达的并发调用在 inflight 屏障上等待同一份结论（inflight 去重），
// 完成后到达的调用直接复用结论（结果复用）。结论有保留时长与条数上限，
// 过期或因超量被淘汰后，同键请求会被当作一次全新请求正常执行。
//
// 淘汰规则（惰性执行，不依赖后台协程）：
//  1. 结论在 TTL 到期后于下一次 Acquire/Lookup/Sweep 时被安全删除；
//  2. 发布新结论导致条数超过 MaxEntries 时，先清理已过期条目，
//     再按最旧发布顺序 FIFO 淘汰，直到不超过上限；
//  3. 淘汰只影响“之后到达”的请求，已拿到结论的调用不受影响。
type DedupeStore struct {
	cfg DedupeConfig

	mu       sync.Mutex
	entries  map[string]CachedOutcome
	order    []string // 已发布结论的键，按发布先后排列（FIFO 淘汰用）
	inflight map[string][]chan struct{}
}

// NewDedupeStore 创建带保留窗口与容量上限的去重存储。
func NewDedupeStore(cfg DedupeConfig) *DedupeStore {
	if cfg.TTL <= 0 {
		cfg.TTL = defaultDedupeTTL
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = defaultDedupeMaxEntries
	}
	if cfg.WaitBudget <= 0 {
		cfg.WaitBudget = defaultDedupeWaitBudget
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &DedupeStore{
		cfg:      cfg,
		entries:  make(map[string]CachedOutcome),
		inflight: make(map[string][]chan struct{}),
	}
}

// Acquire 尝试占位。
//   - leader=true：调用方负责真正发起请求，并在结束时（任何路径）调用 Publish；
//   - leader=false：已有同键结论可复用（wait 已关闭），或同键 leader 在途
//     （wait 在结论发布时关闭）；之后通过 Lookup 读取结论。
//
// 过期结论在此时被视为不存在并被清理，同键请求随之成为全新一次请求的 leader。
func (s *DedupeStore) Acquire(key string) (leader bool, wait <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok {
		if !s.cfg.Now().Before(e.expiresAt) {
			// 已过期：安全删除后按全新请求处理。
			s.removeLocked(key)
		} else {
			ch := make(chan struct{})
			close(ch)
			return false, ch
		}
	}
	if ws, inflight := s.inflight[key]; inflight {
		ch := make(chan struct{})
		s.inflight[key] = append(ws, ch)
		return false, ch
	}
	s.inflight[key] = nil
	return true, nil
}

// Publish 由 leader 在请求结束（无论成功或最终失败）时调用：
// 发布唯一结论、设置保留窗口、唤醒全部等待者。
func (s *DedupeStore) Publish(key string, o CachedOutcome) {
	now := s.cfg.Now()
	o.storedAt = now
	o.expiresAt = now.Add(s.cfg.TTL)

	s.mu.Lock()
	defer s.mu.Unlock()
	// 发布前顺带清理过期条目，避免它们长期占用容量。
	s.dropExpiredLocked(now)
	_, replaced := s.entries[key]
	s.entries[key] = o
	if !replaced {
		s.order = append(s.order, key)
	}
	// 容量上限：按最旧发布顺序淘汰（刚发布的 key 不会淘汰自己，除非 MaxEntries<1，
	// 而构造时已保证 >=1）。
	for len(s.entries) > s.cfg.MaxEntries {
		if !s.evictOldestLocked(key) {
			break
		}
	}
	for _, ch := range s.inflight[key] {
		close(ch)
	}
	delete(s.inflight, key)
}

// Lookup 读取已发布结论；不存在或已过期（并顺手清理）时 ok=false。
func (s *DedupeStore) Lookup(key string) (CachedOutcome, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, found := s.entries[key]
	if !found {
		return CachedOutcome{}, false
	}
	if !s.cfg.Now().Before(e.expiresAt) {
		s.removeLocked(key)
		return CachedOutcome{}, false
	}
	return e, true
}

// Sweep 主动清理全部已过期结论，返回删除条数；可由上层周期性调用，也可不调用。
func (s *DedupeStore) Sweep() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropExpiredLocked(s.cfg.Now())
}

// Len 返回当前保存的结论条数（含尚未到期的失败结论）。
func (s *DedupeStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// removeLocked 删除单条结论并同步整理 FIFO 队列。调用方持锁。
func (s *DedupeStore) removeLocked(key string) {
	delete(s.entries, key)
	out := s.order[:0]
	for _, k := range s.order {
		if k != key {
			out = append(out, k)
		}
	}
	s.order = out
}

// evictOldestLocked 淘汰最旧的一条结论；keepKey 是本次刚发布的键，
// 容量至少为 1 时它不会被淘汰。返回是否实际淘汰。调用方持锁。
func (s *DedupeStore) evictOldestLocked(keepKey string) bool {
	for len(s.order) > 0 {
		oldest := s.order[0]
		s.order = s.order[1:]
		if oldest == keepKey {
			// 刚发布的条目排到队尾，继续淘汰更旧的。
			s.order = append(s.order, oldest)
			continue
		}
		if _, ok := s.entries[oldest]; ok {
			delete(s.entries, oldest)
			return true
		}
		// 已不在 entries（理论上不会），跳过继续。
	}
	return false
}

// dropExpiredLocked 清理所有过期结论并整理 FIFO 队列，返回删除条数。调用方持锁。
func (s *DedupeStore) dropExpiredLocked(now time.Time) int {
	if len(s.entries) == 0 {
		s.order = s.order[:0]
		return 0
	}
	removed := 0
	out := s.order[:0]
	alive := make(map[string]struct{}, len(s.entries))
	for _, k := range s.order {
		e, ok := s.entries[k]
		if !ok {
			continue // 条目已不存在，丢弃队列残留
		}
		if !now.Before(e.expiresAt) {
			delete(s.entries, k)
			removed++
			continue
		}
		out = append(out, k)
		alive[k] = struct{}{}
	}
	// entries 中存在但 order 没有的键（不应出现）也检查一次过期。
	for k, e := range s.entries {
		if _, queued := alive[k]; queued {
			continue
		}
		if !now.Before(e.expiresAt) {
			delete(s.entries, k)
			removed++
		} else {
			out = append(out, k)
		}
	}
	s.order = out
	return removed
}
