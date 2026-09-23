package forwarder

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// sharedTransport 使用短拨号超时，便于把“连接失败”与“请求超时”区分开。
var sharedTransport = &http.Transport{
	Proxy: nil,
	DialContext: (&net.Dialer{
		Timeout: 200 * time.Millisecond,
	}).DialContext,
	MaxIdleConnsPerHost: 1,
	DisableKeepAlives:   true, // 避免连接复用掩盖连接失败场景
}

// doOnce 对单个实例发起一次真实 HTTP 调用，返回状态码、响应头与响应体。
// 调用方通过 ctx 控制单次尝试的截止时间。
func doOnce(ctx context.Context, ins model.Instance, req model.RequestContext) (int, http.Header, []byte, error) {
	url := strings.TrimRight(ins.Addr, "/") + req.Path
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, url, nil)
	if err != nil {
		return 0, nil, nil, model.WrapError(model.ErrConnectFailed, ins.ID, "构造请求失败", err)
	}
	for k, v := range req.Header {
		httpReq.Header.Set(k, v)
	}
	if req.IdempotencyKey != "" {
		httpReq.Header.Set("Idempotency-Key", req.IdempotencyKey)
	}
	resp, err := sharedTransport.RoundTrip(httpReq)
	if err != nil {
		return 0, nil, nil, classifyTransport(err, ins.ID)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, resp.Header.Clone(), body, nil
}

// classifyTransport 把底层传输错误归类为可区分的 CallError。
func classifyTransport(err error, instanceID string) error {
	if err == nil {
		return nil
	}
	// 截止时间/取消优先识别为超时（在途请求的 ctx 截止由调用方设置）。
	if errors.Is(err, context.DeadlineExceeded) || isTimeoutErr(err) {
		return model.WrapError(model.ErrTimeout, instanceID, "单次尝试超过截止时间", err)
	}
	if errors.Is(err, context.Canceled) {
		return model.WrapError(model.ErrTimeout, instanceID, "请求被取消", err)
	}
	// 连接被拒绝、连接重置、DNS 错误等统一归为连接失败。
	var opErr *net.OpError
	if errors.As(err, &opErr) || strings.Contains(err.Error(), "connection refused") {
		return model.WrapError(model.ErrConnectFailed, instanceID, "无法建立到后端的连接", err)
	}
	return model.WrapError(model.ErrConnectFailed, instanceID, "传输层错误", err)
}

func isTimeoutErr(err error) bool {
	var te interface{ Timeout() bool }
	if errors.As(err, &te) && te.Timeout() {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "timeout")
}

// classifyStatus 把 HTTP 状态码归类为 CallError（2xx/3xx 视为成功返回 nil）。
func classifyStatus(status int, instanceID string) error {
	switch {
	case status >= 200 && status < 400:
		return nil
	case status >= 400 && status < 500:
		return model.Errf(model.ErrUpstream4xx, instanceID, "后端返回 %d，属客户端错误", status)
	case status > 0:
		return model.Errf(model.ErrUpstream5xx, instanceID, "后端返回 %d，属服务端错误", status)
	default:
		return model.NewError(model.ErrConnectFailed, instanceID, "未获得任何 HTTP 状态码")
	}
}

// Classify 供外部使用的分类入口：传输错误优先，否则按状态码。
func Classify(err error, status int, instanceID string) error {
	if err != nil {
		return err
	}
	return classifyStatus(status, instanceID)
}
