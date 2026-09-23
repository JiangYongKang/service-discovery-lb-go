package backend

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

// TCPProbe 返回一个基于真实 TCP 拨号的健康探测函数。
// 不健康实例会拖延响应 -> 探测超时失败；已 Stop 实例 -> 连接拒绝失败。
// 同时支持用可控的内存探测表（见 SimulatedProbe）做确定性单测。
func TCPProbe() func(ctx context.Context, inst *discovery.Instance) error {
	return func(ctx context.Context, inst *discovery.Instance) error {
		d := net.Dialer{Timeout: 100 * time.Millisecond}
		conn, err := d.DialContext(ctx, "tcp", inst.Addr)
		if err != nil {
			return fmt.Errorf("probe dial %s: %w", inst.Addr, err)
		}
		defer conn.Close()
		if dl, ok := ctx.Deadline(); ok {
			_ = conn.SetDeadline(dl)
		}
		if _, err := fmt.Fprintf(conn, "PING %s\n", inst.ID); err != nil {
			return err
		}
		br := bufio.NewReader(conn)
		line, err := br.ReadString('\n')
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				return context.DeadlineExceeded
			}
			return err
		}
		if !strings.HasPrefix(line, "OK") {
			return fmt.Errorf("probe bad reply: %q", strings.TrimSpace(line))
		}
		return nil
	}
}

// ProbeTable 是实例 ID -> 健康 的内存可控表，便于确定性测试。
type ProbeTable struct {
	// impl 在测试/门面装配时直接使用 map + RWMutex 即可，
	// 这里提供具名类型与方法避免裸 map 竞争。
	mu    sync.RWMutex
	alive map[string]bool
}

// NewProbeTable 创建探测表（默认未设置的实例视为可拨号，这里统一返回健康）。
func NewProbeTable() *ProbeTable { return &ProbeTable{alive: map[string]bool{}} }

// Set 设置实例健康。
func (t *ProbeTable) Set(id string, healthy bool) {
	t.mu.Lock()
	t.alive[id] = healthy
	t.mu.Unlock()
}

// Get 读取实例健康；未登记默认 true。
func (t *ProbeTable) Get(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if v, ok := t.alive[id]; ok {
		return v
	}
	return true
}

// Func 返回适配 health.ProbeFunc 的闭包；不健康实例通过超时延迟模拟。
func (t *ProbeTable) Func(timeoutIfUnhealthy time.Duration) func(ctx context.Context, inst *discovery.Instance) error {
	return func(ctx context.Context, inst *discovery.Instance) error {
		if t.Get(inst.ID) {
			return nil
		}
		select {
		case <-time.After(timeoutIfUnhealthy):
			return fmt.Errorf("probe %s: simulated unhealthy", inst.ID)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
