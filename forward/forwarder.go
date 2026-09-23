package forward

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/route"
)

// Outcome 降级/转发最终结论，便于解释路由结果。
type Outcome string

const (
	OutcomeSuccess     Outcome = "success"
	OutcomeFailOpen    Outcome = "fail-open"
	OutcomeNoBackend   Outcome = "no-backend"
	OutcomeTimeout     Outcome = "timeout"
	OutcomeConnRefused Outcome = "conn-refused"
	OutcomeRejected    Outcome = "rejected"
)

var (
	// ErrUpstreamTimeout 后端在规定时间内未响应。
	ErrUpstreamTimeout = errors.New("forward: upstream timeout")
	// ErrConnectionRefused 连接失败（端口未监听/实例已下线），请求确定未到达后端。
	ErrConnectionRefused = errors.New("forward: connection refused")
	// ErrUnavailable 后端不可用且未允许 fail-open。
	ErrUnavailable = errors.New("forward: upstream unavailable")
)

// Degraded 是 fail-open 降级时返回的占位响应。
type Degraded struct {
	Service string
	Reason  string
}

// InvokeFunc 对选中实例执行一次真实调用。
// 语义约定：
//   - 返回 ErrConnectionRefused 表示请求未发出/未到达后端，允许安全重试；
//   - 返回 ErrUpstreamTimeout 表示结果未知（可能已产生副作用），默认不重试；
//   - 其它 error 一律视为后端已参与处理，不重试。
type InvokeFunc func(ctx context.Context, inst *discovery.Instance) (resp any, err error)

// FailureReporter 传输层失败反馈（由健康检查器实现），
// 使连接失败的实例立即离开健康集合，而不是等下一轮探测。
type FailureReporter interface {
	ReportTransportFailure(id string)
}

// Options 单次转发参数（取自路由规则快照，热更新不影响在途请求）。
type Options struct {
	Service       string
	HashKey       string
	Strategy      lb.Strategy
	Timeout       time.Duration
	MaxAttempts   int
	RetrySameNode bool
	FailOpen      bool
	Weights       map[string]int
	RuleVersion   uint64
}

// Attempt 记录每一跳的选择与结果，保证路由结论可解释。
type Attempt struct {
	Index      int
	InstanceID string
	State      discovery.InstanceState
	Start      time.Time
	Cost       time.Duration
	Err        error
	Retried    bool // 该跳是一次重试
	Skipped    bool // 选中后因并发注销/状态变化被跳过（无副作用）
	SkipReason string
}

// Result 一次转发的最终结论。
type Result struct {
	Outcome  Outcome
	Response any
	Attempts []Attempt
	RuleVer  uint64
	FailOpen bool
	Err      error
}

func (r *Result) Explain() string {
	s := "outcome=" + string(r.Outcome) +
		" ruleVer=" + fmt.Sprintf("%d", r.RuleVer) +
		" attempts=" + fmt.Sprintf("%d", len(r.Attempts))
	for _, a := range r.Attempts {
		s += "\n  - " + a.explain()
	}
	if r.Err != nil {
		s += "\n  final-error=" + r.Err.Error()
	}
	return s
}

func (a Attempt) explain() string {
	s := fmt.Sprintf("#%d instance=%s state=%s", a.Index, a.InstanceID, a.State)
	if a.Retried {
		s += " [retry]"
	}
	if a.Skipped {
		s += " [skipped:" + a.SkipReason + "]"
	}
	if a.Err != nil {
		s += " err=" + a.Err.Error()
	}
	return s + " cost=" + a.Cost.String()
}

// Forwarder 组合注册中心/负载均衡完成转发、重试与降级。
type Forwarder struct {
	reg      *discovery.Registry
	reporter FailureReporter

	// 负载均衡器按 service+strategy 复用：轮询/加权的计数器必须跨请求保持。
	bals sync.Map // key -> lb.Balancer
}

// NewForwarder 创建转发器。
func NewForwarder(reg *discovery.Registry) *Forwarder {
	return &Forwarder{reg: reg}
}

// balancerFor 返回（或创建并缓存）某服务某策略的负载均衡器。
// 不同策略互不共享计数器；同策略热更新权重时复用计数器（计数器语义不受权重影响）。
func (f *Forwarder) balancerFor(service string, strategy lb.Strategy, weights map[string]int) (lb.Balancer, error) {
	key := string(strategy) + "@" + service
	if v, ok := f.bals.Load(key); ok {
		return v.(lb.Balancer), nil
	}
	b, err := lb.New(strategy, weights)
	if err != nil {
		return nil, err
	}
	actual, _ := f.bals.LoadOrStore(key, b)
	return actual.(lb.Balancer), nil
}

// WithFailureReporter 注入传输失败上报器。
func (f *Forwarder) WithFailureReporter(r FailureReporter) *Forwarder {
	f.reporter = r
	return f
}

// Do 执行一次带路由快照的转发。
// 策略在进入时即固定（opts），在途期间规则热更新不影响本次请求；
// 候选实例集来自入口快照，每跳还会向注册中心复核状态，
// 因此并发注销/摘除的实例绝不会真正收到请求。
func (f *Forwarder) Do(ctx context.Context, opts Options, invoke InvokeFunc) (*Result, error) {
	res := &Result{RuleVer: opts.RuleVersion, FailOpen: opts.FailOpen}
	balancer, err := f.balancerFor(opts.Service, opts.Strategy, opts.Weights)
	if err != nil {
		res.Outcome = OutcomeRejected
		res.Err = err
		return res, err
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	snap := f.reg.Snapshot()
	candidates := snap.HealthyOf(opts.Service)
	tried := map[string]bool{}
	usedAttempts := 0 // 真正发出请求的跳数（副作用计数，绝不超过 MaxAttempts）

	// 选择轮次上限：防止候选在并发抖动下反复“选中后跳过”造成死循环。
	selectionCap := maxAttempts + len(candidates) + 1
	req := lb.Request{Service: opts.Service, HashKey: opts.HashKey}

	for selection := 0; selection < selectionCap; selection++ {
		// 构造当前可选集合：入口快照健康 + 未试过（不重试同节点时）。
		pool := candidates[:0:0]
		for _, in := range candidates {
			if !opts.RetrySameNode && tried[in.ID] {
				continue
			}
			pool = append(pool, in)
		}

		chosen, pickErr := balancer.Pick(req, pool)
		if pickErr != nil {
			// 无健康实例：根据 fail-open 做确定降级。
			return f.finishUnavailable(res, opts, lastErr(res), "no-pick")
		}

		// 并发复核：选中后、发请求前再次确认注册中心当前状态。
		cur, ok := f.reg.Get(chosen.ID)
		a := Attempt{
			Index:      len(res.Attempts) + 1,
			InstanceID: chosen.ID,
			State:      chosen.State(),
			Start:      time.Now(),
			Retried:    usedAttempts > 0,
		}
		if !ok || cur.State() == discovery.StateDeregistered {
			a.Skipped = true
			a.State = discovery.StateDeregistered
			a.SkipReason = "instance-deregistered-in-flight"
			a.Cost = time.Since(a.Start)
			res.Attempts = append(res.Attempts, a)
			// 从本次候选中剔除已失效实例，继续选择下一个（该跳没有任何副作用）。
			candidates = removeByID(candidates, chosen.ID)
			tried[chosen.ID] = true
			continue
		}
		if !cur.Routable() { // probing / unhealthy：并发摘除窗口
			a.Skipped = true
			a.State = cur.State()
			a.SkipReason = "instance-not-routable(" + cur.State().String() + ")"
			a.Cost = time.Since(a.Start)
			res.Attempts = append(res.Attempts, a)
			candidates = removeByID(candidates, chosen.ID)
			tried[chosen.ID] = true
			continue
		}

		// 副作用预算：真正发送的次数不得超过 MaxAttempts。
		if usedAttempts >= maxAttempts {
			return f.finishUnavailable(res, opts, lastErr(res), "attempt-budget-exhausted")
		}
		usedAttempts++

		callCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
		resp, callErr := invoke(callCtx, cur)
		cancel()
		a.Cost = time.Since(a.Start)
		a.Err = callErr
		res.Attempts = append(res.Attempts, a)

		if callErr == nil {
			res.Outcome = OutcomeSuccess
			res.Response = resp
			return res, nil
		}

		classified := classify(callCtx, callErr)
		a.Err = classified
		res.Attempts[len(res.Attempts)-1] = a

		// 连接失败：确定无副作用，实例立即摘除并安全重试其它节点。
		if errors.Is(classified, ErrConnectionRefused) {
			if f.reporter != nil {
				f.reporter.ReportTransportFailure(chosen.ID)
			}
			tried[chosen.ID] = true
			candidates = removeByID(candidates, chosen.ID)
			continue
		}
		// 超时/其它错误：可能已产生副作用，按确定规则不重试，直接收尾。
		return f.finishUnavailable(res, opts, classified, "no-retry-policy")
	}
	return f.finishUnavailable(res, opts, lastErr(res), "selection-cap")
}

// finishUnavailable 统一的不可用收尾：允许 fail-open 则降级放行，否则明确失败。
func (f *Forwarder) finishUnavailable(res *Result, opts Options, cause error, why string) (*Result, error) {
	if opts.FailOpen {
		res.Outcome = OutcomeFailOpen
		res.FailOpen = true
		res.Response = Degraded{Service: opts.Service, Reason: why}
		res.Err = cause
		return res, nil
	}
	res.Outcome = outcomeFor(cause)
	wrapped := fmt.Errorf("%w: service=%s reason=%s", ErrUnavailable, opts.Service, why)
	if cause != nil {
		wrapped = fmt.Errorf("%w: %w", wrapped, cause)
	}
	res.Err = wrapped
	return res, wrapped
}

func outcomeFor(err error) Outcome {
	switch {
	case errors.Is(err, ErrUpstreamTimeout):
		return OutcomeTimeout
	case errors.Is(err, ErrConnectionRefused):
		return OutcomeConnRefused
	default:
		return OutcomeNoBackend
	}
}

func lastErr(res *Result) error {
	for i := len(res.Attempts) - 1; i >= 0; i-- {
		if res.Attempts[i].Err != nil {
			return res.Attempts[i].Err
		}
	}
	return nil
}

func removeByID(list []*discovery.Instance, id string) []*discovery.Instance {
	out := list[:0]
	for _, in := range list {
		if in.ID != id {
			out = append(out, in)
		}
	}
	return out
}

// classify 把调用错误归一化为本包的可解释哨兵错误。
func classify(callCtx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrConnectionRefused), errors.Is(err, ErrUpstreamTimeout):
		return err
	case errors.Is(err, context.DeadlineExceeded):
		return ErrUpstreamTimeout
	case errors.Is(err, context.Canceled):
		return err
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		if nerr.Timeout() {
			return ErrUpstreamTimeout
		}
		return fmt.Errorf("%w: %v", ErrConnectionRefused, err)
	}
	// 常见的连接拒绝文本（invoke 用 net.Dial 时可能拿到 syscall 错误）。
	if isConnRefusedText(err) {
		return fmt.Errorf("%w: %v", ErrConnectionRefused, err)
	}
	if callCtx.Err() == context.DeadlineExceeded {
		return ErrUpstreamTimeout
	}
	return err
}

func isConnRefusedText(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "connection refused") || contains(msg, "connect: cannot assign")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// WithRule 从规则版本中取服务规则生成 Options 的便捷方法。
func WithRule(ver *route.VersionedRules, service, hashKey string) (Options, bool) {
	if ver == nil {
		return Options{}, false
	}
	r := ver.Lookup(service)
	if r == nil {
		return Options{}, false
	}
	return Options{
		Service:       service,
		HashKey:       hashKey,
		Strategy:      r.Strategy,
		Timeout:       r.Timeout,
		MaxAttempts:   r.MaxAttempts,
		RetrySameNode: r.RetrySameNode,
		FailOpen:      r.FailOpen,
		Weights:       r.Weights,
		RuleVersion:   ver.Version,
	}, true
}
