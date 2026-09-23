package forwarder

import (
	"context"
	"net/http"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// InstanceFilter 在真正发起连接前复核实例是否仍在册且可路由，
// 用于堵截“选择之后、连接之前”发生的并发注销/摘除。
type InstanceFilter func(id string) (model.Instance, bool)

// Selector 在某次请求的候选集合中挑选下一个实例（排除已尝试实例）。
type Selector interface {
	Pick(exclude map[string]bool) (model.Instance, error)
}

// BeginSelector 由需要原子“选择并占用资源”的均衡器实现（如 least_conn）。
// PickAndBegin 必须在均衡器内部临界区完成选择与在途计数 +1，
// 避免并发请求在计数更新前重复选中同一低负载实例。
type BeginSelector interface {
	PickAndBegin(exclude map[string]bool) (model.Instance, error)
}

// RetryPolicy 描述重试与降级规则。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次）；<=1 表示不重试。
	MaxAttempts int
	// RetryableKinds 允许重试的错误类别；为空时采用默认可重试集合（连接失败/超时/5xx）。
	RetryableKinds map[model.ErrorKind]bool
	// PerCallTimeout 每次尝试的独立超时预算。
	PerCallTimeout time.Duration
}

func (p RetryPolicy) retryable(k model.ErrorKind) bool {
	if len(p.RetryableKinds) == 0 {
		return k.RetryableByDefault()
	}
	return p.RetryableKinds[k]
}

// Result 是一次转发（含重试）的最终结果。
type Result struct {
	InstanceID string
	StatusCode int
	Header     http.Header
	Attempts   []model.Attempt
	DedupeHit  bool
}

// Forwarder 负责实际转发、错误分类、幂等去重与重试降级。
//
// 降级规则（确定性）：
//   - 起始即无健康候选            -> no_healthy_instance，不消耗尝试；
//   - 选择后发现实例已注销/摘除    -> 跳过且不消耗尝试，记录判定依据；
//   - 4xx 等不可重试错误          -> 首次即返回，保留原始错误类别；
//   - 可重试但已达 MaxAttempts    -> no_retry_budget，cause 保留最后一次错误；
//   - 重试过程中候选被排空        -> no_healthy_instance，Attempts 记录历史。
//
// 副作用安全：转发器永不修改健康集合（失败实例不会被“放回”，只是本请求内排除）；
// 非幂等请求（无 IdempotencyKey）仅允许对“确定未产生副作用”的 connect_failed 重试。
type Forwarder struct {
	filter  InstanceFilter
	dedupe  *DedupeStore
	onBegin func(id string)
	onEnd   func(id string)
	logf    func(format string, args ...any)
}

// Option 配置转发器。
type Option func(*Forwarder)

// WithInflightHooks 注入在途计数钩子（如 least_conn 的 Begin/End）。
func WithInflightHooks(begin, end func(id string)) Option {
	return func(f *Forwarder) { f.onBegin, f.onEnd = begin, end }
}

// WithLogger 注入解释性日志。
func WithLogger(logf func(format string, args ...any)) Option {
	return func(f *Forwarder) { f.logf = logf }
}

// New 创建转发器。
func New(filter InstanceFilter, opts ...Option) *Forwarder {
	f := &Forwarder{
		filter: filter,
		dedupe: NewDedupeStore(),
		logf:   func(string, ...any) {},
	}
	for _, o := range opts {
		o(f)
	}
	return f
}

// Execute 按重试策略执行请求。
func (f *Forwarder) Execute(ctx context.Context, req model.RequestContext, sel Selector, rp RetryPolicy) (*Result, error) {
	if rp.MaxAttempts < 1 {
		rp.MaxAttempts = 1
	}

	// 幂等去重：同键并发/重试只允许一个调用真正到达后端。
	if req.IdempotencyKey != "" {
		leader, wait := f.dedupe.acquire(req.IdempotencyKey)
		if !leader {
			<-wait
			id, status, ok := f.dedupe.Lookup(req.IdempotencyKey)
			if ok {
				f.logf("[dedupe] key=%s 复用已完成结果 instance=%s status=%d", req.IdempotencyKey, id, status)
				return &Result{InstanceID: id, StatusCode: status, DedupeHit: true}, nil
			}
			return nil, model.NewError(model.ErrNoRetryBudget, "", "幂等键等待被异常释放")
		}
	}

	res := &Result{}
	exclude := make(map[string]bool)
	attempt := 0 // 已发起的真实尝试次数
	skips := 0   // 因并发注销/摘除而跳过的次数（不消耗尝试预算）
	beginSel, hasBeginSel := sel.(BeginSelector)

	for {
		var ins model.Instance
		var pickErr error
		beganBySelector := false
		if hasBeginSel {
			ins, pickErr = beginSel.PickAndBegin(exclude)
			beganBySelector = pickErr == nil
		} else {
			ins, pickErr = sel.Pick(exclude)
		}
		if pickErr != nil {
			if attempt == 0 {
				return res, model.Errf(model.ErrNoHealthyInstance, "",
					"服务 %s 起始无健康候选: %v", req.Service, pickErr)
			}
			return res, model.Errf(model.ErrNoHealthyInstance, "",
				"重试过程中健康候选已排空，已尝试 %d 个实例: %v", attempt, res.Attempts)
		}

		// 连接前二次复核：堵截并发注销 / 并发摘除。跳过不消耗尝试预算。
		if f.filter != nil {
			if cur, ok := f.filter(ins.ID); !ok {
				if beganBySelector && f.onEnd != nil {
					f.onEnd(ins.ID) // 归还选择时原子占用的在途计数
				}
				reason := "实例在连接前已注销/摘除，跳过且不计尝试"
				f.logf("[guard] instance=%s %s", ins.ID, reason)
				res.Attempts = append(res.Attempts, model.Attempt{
					InstanceID: ins.ID, ErrKind: model.ErrInstanceDead, Reason: reason})
				exclude[ins.ID] = true
				skips++
				if skips > 64 {
					return res, model.Errf(model.ErrNoHealthyInstance, ins.ID, "候选反复失效，终止")
				}
				continue
			} else {
				ins = cur
			}
		}
		attempt++

		callCtx, cancel := ctx, func() {}
		if rp.PerCallTimeout > 0 {
			callCtx, cancel = context.WithTimeout(ctx, rp.PerCallTimeout)
		}
		if f.onBegin != nil && !beganBySelector {
			f.onBegin(ins.ID)
		}
		status, hdr, _, callErr := doOnce(callCtx, ins, req)
		if f.onEnd != nil {
			f.onEnd(ins.ID)
		}
		cancel()

		if err := Classify(callErr, status, ins.ID); err != nil {
			kind := model.ErrKind(err)
			at := model.Attempt{
				InstanceID: ins.ID, StatusCode: status, ErrKind: kind,
				Reason: model.DescribeKind(kind),
			}
			res.Attempts = append(res.Attempts, at)
			exclude[ins.ID] = true
			f.logf("[attempt %d/%d] instance=%s -> kind=%s status=%d 依据=%s",
				attempt, rp.MaxAttempts, ins.ID, kind, status, at.Reason)

			if req.IdempotencyKey != "" {
				f.dedupe.complete(req.IdempotencyKey, ins.ID, status)
			}
			// 不可重试类别（如 4xx）：首次失败即降级返回，保留原类别。
			if !rp.retryable(kind) {
				return res, err
			}
			// 副作用安全：无幂等键时，只有确定未到达业务处理的连接失败可重试。
			if req.IdempotencyKey == "" && kind != model.ErrConnectFailed {
				f.logf("[retry] 非幂等请求且 kind=%s 不保证无重复副作用，停止重试", kind)
				return res, err
			}
			if attempt >= rp.MaxAttempts {
				budget := model.WrapError(model.ErrNoRetryBudget, ins.ID,
					"已达最大尝试次数，执行确定性降级（向上游返回失败）", err)
				return res, budget
			}
			f.logf("[retry] kind=%s 可重试，排除 instance=%s 后在剩余健康集合中重选", kind, ins.ID)
			continue
		}

		// 成功
		res.InstanceID, res.StatusCode = ins.ID, status
		res.Header = hdr
		res.Attempts = append(res.Attempts, model.Attempt{
			InstanceID: ins.ID, StatusCode: status, Reason: "尝试成功",
		})
		if req.IdempotencyKey != "" {
			f.dedupe.complete(req.IdempotencyKey, ins.ID, status)
		}
		f.logf("[attempt %d/%d] instance=%s 成功 status=%d", attempt, rp.MaxAttempts, ins.ID, status)
		return res, nil
	}
}
