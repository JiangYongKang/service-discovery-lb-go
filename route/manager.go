package route

import (
	"sync"
)

// Manager 包装 Table，串行化热更新并提供版本变化广播。
// 并发 Update 全部经过同一把锁：要么整版生效，要么全部不生效，
// 绝不会出现“一半新规则一半旧规则”的中间态。
type Manager struct {
	table *Table

	mu      sync.Mutex
	waiters map[chan uint64]struct{}
}

// NewManager 创建规则管理器。
func NewManager(table *Table) *Manager {
	return &Manager{table: table, waiters: map[chan uint64]struct{}{}}
}

// Update 热更新：校验通过才整体生效并返回新版本号；失败保留旧版并返回错误。
func (m *Manager) Update(rules []*Rule) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ver, err := m.table.Publish(rules)
	if err != nil {
		return m.table.Current().Version, err
	}
	for ch := range m.waiters {
		select {
		case ch <- ver.Version:
		default:
		}
	}
	return ver.Version, nil
}

// CurrentVersion 返回当前版本号。
func (m *Manager) CurrentVersion() uint64 { return m.table.Current().Version }

// Table 返回底层规则表。
func (m *Manager) Table() *Table { return m.table }
