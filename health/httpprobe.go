package health

import (
	"net/http"
	"strings"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// HTTPProbe 对实例的 /healthz 发起 GET；
// 只有 2xx/3xx 视为探测成功，连接错误与非成功状态码均为探测失败。
type HTTPProbe struct {
	Client  *http.Client
	Path    string
	ProbeAs func(error) error
}

// NewHTTPProbe 创建默认 HTTP 探针。
func NewHTTPProbe(timeout time.Duration) *HTTPProbe {
	return &HTTPProbe{
		Client: &http.Client{Timeout: timeout},
		Path:   "/healthz",
	}
}

// Check 执行一次探测，返回 model 层可区分的 ErrProbeFailed 或 nil。
func (p *HTTPProbe) Check(ins model.Instance) error {
	resp, err := p.Client.Get(strings.TrimRight(ins.Addr, "/") + p.Path)
	if err != nil {
		return model.WrapError(model.ErrProbeFailed, ins.ID, "健康探测请求失败", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return model.Errf(model.ErrProbeFailed, ins.ID, "健康探测返回状态码 %d", resp.StatusCode)
	}
	return nil
}

// Func 返回可直接注入 Checker/Monitor 的 ProbeFunc。
func (p *HTTPProbe) Func() ProbeFunc { return p.Check }
