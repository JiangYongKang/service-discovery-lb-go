// Package backend 用本地 TCP 进程模拟后端服务实例，
// 提供健康翻转、失败注入与可观测的请求计数，供发现/调度链路验证。
package backend

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server 一个本地 TCP 后端实例。
// 行为：
//   - healthy=true：接受连接，读取一行请求，回 "OK <id> <seq>"；
//   - healthy=false：接受 TCP（仍监听）但不响应直至超时，模拟实例假死；
//   - Close 后端口不可连接，模拟进程下线（连接拒绝）。
type Server struct {
	ID      string
	Service string
	Addr    string // host:port，New 后填充

	listener net.Listener

	mu       sync.RWMutex
	healthy  bool
	deadline time.Duration // unhealthy 时的拖延时长
	failNext int32         // >0 时接下来的 N 个请求返回 ERROR

	reqCount atomic.Int64
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// New 创建并启动模拟后端，监听 127.0.0.1 上随机端口。
func New(id, service string, healthy bool) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("backend %s: listen: %w", id, err)
	}
	s := &Server{
		ID:       id,
		Service:  service,
		Addr:     ln.Addr().String(),
		listener: ln,
		healthy:  healthy,
		deadline: 5 * time.Second,
		stopCh:   make(chan struct{}),
	}
	s.wg.Add(1)
	go s.serve()
	return s, nil
}

// RequestCount 返回已成功处理的请求数。
func (s *Server) RequestCount() int64 { return s.reqCount.Load() }

// IsHealthy 返回当前健康标记。
func (s *Server) IsHealthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

// SetHealthy 切换健康状态（用于测试探测抖动）。
func (s *Server) SetHealthy(h bool) {
	s.mu.Lock()
	s.healthy = h
	s.mu.Unlock()
}

// Stall 设置不健康时拖延连接的时长。
func (s *Server) Stall(d time.Duration) {
	s.mu.Lock()
	s.deadline = d
	s.mu.Unlock()
}

// FailNext 让接下来 n 个请求返回 ERROR（不计数），用于注入业务失败。
func (s *Server) FailNext(n int) { atomic.StoreInt32(&s.failNext, int32(n)) }

// Stop 关闭监听并等待连接循环退出（模拟进程下线）。
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		_ = s.listener.Close()
		s.wg.Wait()
	})
}

func (s *Server) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			return
		}
		s.wg.Add(1)
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return
	}
	_ = line

	s.mu.RLock()
	healthy := s.healthy
	stall := s.deadline
	s.mu.RUnlock()

	if !healthy {
		// 假死：拖到截止或服务器停止，不写任何响应。
		t := time.NewTimer(stall)
		defer t.Stop()
		select {
		case <-s.stopCh:
		case <-t.C:
		}
		return
	}
	if n := atomic.AddInt32(&s.failNext, -1); n >= 0 {
		_, _ = io.WriteString(conn, "ERROR "+s.ID+" injected-failure\n")
		return
	}
	// 仅 CALL 计入业务请求数；PING 为健康探测，不计入。
	seq := int64(0)
	if strings.HasPrefix(line, "CALL") {
		seq = s.reqCount.Add(1)
	}
	_, _ = io.WriteString(conn, "OK "+s.ID+" "+strconv.FormatInt(seq, 10)+"\n")
}

// DialInvoke 构造一个真实 TCP 调用：发一行请求并读一行响应。
// 连接被拒绝时返回包装 ErrConnectionRefused 的错误；超时返回 ErrUpstreamTimeout。
func DialInvoke() func(ctx context.Context, addr, id string) (string, error) {
	return func(ctx context.Context, addr, id string) (string, error) {
		d := net.Dialer{Timeout: 200 * time.Millisecond}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return "", err
		}
		defer conn.Close()
		if dl, ok := ctx.Deadline(); ok {
			_ = conn.SetDeadline(dl)
		}
		if _, err := fmt.Fprintf(conn, "CALL %s\n", id); err != nil {
			return "", err
		}
		br := bufio.NewReader(conn)
		resp, err := br.ReadString('\n')
		if err != nil {
			return "", err
		}
		return resp[:len(resp)-1], nil
	}
}

var _ = context.Background
