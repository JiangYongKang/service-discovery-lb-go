package backend

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// Behavior 控制本地模拟后端的故障注入。
type Behavior struct {
	FailProbe   bool          // 健康探测路径返回 503
	ProbeStatus int           // 探测路径自定义状态码（0 且 !FailProbe 时为 200）
	CallStatus  int           // 业务路径返回状态码，0 表示 200
	Latency     time.Duration // 业务路径处理前的固定延迟（模拟超时）
}

// Instance 是一个基于 httptest 的本地后端进程模拟。
type Instance struct {
	ID         string
	Service    string
	server     *httptest.Server
	bh         atomic.Pointer[Behavior]
	calls      atomic.Int64
	probeCalls atomic.Int64
}

// New 创建并启动本地后端实例。
func New(id, service string, bh Behavior) (*Instance, error) {
	if bh.ProbeStatus == 0 {
		bh.ProbeStatus = http.StatusOK
	}
	i := &Instance{ID: id, Service: service}
	i.bh.Store(&bh)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", i.handleProbe)
	mux.HandleFunc("/", i.handleCall)
	i.server = httptest.NewServer(mux)
	return i, nil
}

func (i *Instance) handleProbe(w http.ResponseWriter, r *http.Request) {
	i.probeCalls.Add(1)
	bh := *i.bh.Load()
	if bh.FailProbe {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "probe fail")
		return
	}
	if bh.ProbeStatus != 0 {
		w.WriteHeader(bh.ProbeStatus)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (i *Instance) handleCall(w http.ResponseWriter, r *http.Request) {
	bh := *i.bh.Load()
	if bh.Latency > 0 {
		select {
		case <-time.After(bh.Latency):
		case <-r.Context().Done():
			// 客户端已超时/取消；不写入任何成功响应，避免被误认为已执行。
			return
		}
	}
	// 业务副作用计数：只有真正开始处理请求才会计数，
	// 因此重试命中不同实例时可通过 Calls() 验证“无重复副作用”。
	i.calls.Add(1)
	status := bh.CallStatus
	if status == 0 {
		status = http.StatusOK
	}
	w.Header().Set("X-Instance-ID", i.ID)
	w.Header().Set("X-Instance-Addr", i.Addr())
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "served-by=%s path=%s", i.ID, r.URL.Path)
}

// ToModel 导出注册信息。
func (i *Instance) ToModel() model.Instance {
	return model.Instance{ID: i.ID, Service: i.Service, Addr: i.server.URL}
}

// SetBehavior 动态调整故障注入（模拟抖动）。
func (i *Instance) SetBehavior(bh Behavior) {
	if bh.ProbeStatus == 0 {
		bh.ProbeStatus = http.StatusOK
	}
	i.bh.Store(&bh)
}

// Calls 返回业务请求被实际处理次数。
func (i *Instance) Calls() int64 { return i.calls.Load() }

// ProbeCalls 返回探测请求次数。
func (i *Instance) ProbeCalls() int64 { return i.probeCalls.Load() }

// Addr 返回监听地址。
func (i *Instance) Addr() string { return i.server.URL }

// Close 停止实例（模拟进程下线 / 连接拒绝）。
func (i *Instance) Close() { i.server.Close() }
