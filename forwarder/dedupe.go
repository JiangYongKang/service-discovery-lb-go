package forwarder

import "sync"

// dedupeEntry 记录一个幂等键对应的已完成结果。
type dedupeEntry struct {
	instanceID string
	statusCode int
}

// DedupeStore 保存 幂等键 -> 已完成结果。
// 语义：同一幂等键下，只有第一个进入临界区的调用会真正访问后端；
// 在其完成前到达的并发调用等待同一结果（inflight 去重），
// 完成后到达的调用直接复用结果。由此保证“重试/并发不产生重复副作用”。
type DedupeStore struct {
	mu      sync.Mutex
	done    map[string]dedupeEntry
	waiters map[string][]chan struct{}
}

// NewDedupeStore 创建去重存储。
func NewDedupeStore() *DedupeStore {
	return &DedupeStore{
		done:    make(map[string]dedupeEntry),
		waiters: make(map[string]waiterList),
	}
}

type waiterList = []chan struct{}

// acquire 尝试占位。
//   - leader=true：调用方负责真正发起请求并在结束时调用 complete；
//   - leader=false：已有同键调用在进行或已完成，wait 在结果就绪时关闭，
//     之后可通过 Lookup 读取已完成结果。
func (s *DedupeStore) acquire(key string) (leader bool, wait <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.done[key]; ok {
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

// complete 由 leader 在请求结束（无论成功或最终失败）时调用，发布结果并唤醒等待者。
func (s *DedupeStore) complete(key, instanceID string, statusCode int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done[key] = dedupeEntry{instanceID: instanceID, statusCode: statusCode}
	for _, ch := range s.waiters[key] {
		close(ch)
	}
	delete(s.waiters, key)
}

// Lookup 读取已完成结果。
func (s *DedupeStore) Lookup(key string) (instanceID string, statusCode int, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, found := s.done[key]
	if !found {
		return "", 0, false
	}
	return e.instanceID, e.statusCode, true
}
