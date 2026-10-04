package forwarder

import (
	"context"
	"errors"
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

// BeginSelector 由需要原子“选择并占用资源”的均衡器选择器实现（如 least_conn）。
// PickAndBegin 必须在均衡器内部临界区完成选择与在途计数 +1，
// 避免并发请求在计数更新前重复选中同一低负载实例。
type BeginSelector interface {
	PickAndBegin(exclude map[string]bool) (model.Instance, error)
}

// InflightReleaser 归还 PickAndBegin 原子占用的在途名额。
// 选择与归还必须落在同一个均衡器计数实例上，
// 因此归还动作经选择器自身闭环，而不是由外部钩子在别处计数，
// 从而保证“长/短请求、失败、超时、跳过”任何路径下计数都只涨必落。
type InflightReleaser interface {
	Release(id string)
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
	// DedupeHit=true 表示该结论复用自同业务键 leader 的已发布结论（成功或失败重放均可能置位）。
	DedupeHit bool
}

// Forwarder 负责实际转发、错误分类、结论收敛（结果复用）与重试降级。
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
//
// 结论收敛：携带 IdempotencyKey 的请求，无论同时到达还是保留窗口内重放，
// 成功与“最终失败”结论各只由 leader 产生一次，其余调用复用同一结论；
// 失败结论携带可区分错误类别，绝不会被记成成功。
type Forwarder struct {
	filter InstanceFilter
	dedupe *DedupeStore
	logf   func(format string, args ...any)
}

// Option 配置转发器。
type Option func(*Forwarder)

// WithLogger 注入解释性日志。
func WithLogger(logf func(format string, args ...any)) Option {
	return func(f *Forwarder) { f.logf = logf }
}

// WithDedupeConfig 注入结果复用缓存参数（保留窗口、容量上限）。
func WithDedupeConfig(cfg DedupeConfig) Option {
	return func(f *Forwarder) { f.dedupe = NewDedupeStoreWith(cfg) }
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

// DedupeStore 暴露结论缓存，便于观测容量与主动淘汰（测试/运维）。
func (f *Forwarder) DedupeStore() *DedupeStore { return f.dedupe }

// Execute 按重试策略执行请求。
func (f *Forwarder) Execute(ctx context.Context, req model.RequestContext, sel Selector, rp RetryPolicy) (*Result, error) {
	if rp.MaxAttempts < 1 {
		rp.MaxAttempts = 1
	}

	// 幂等结论收敛：同键并发/保留窗口内重放只允许一个调用真正到达后端。
	if req.IdempotencyKey != "" {
		leader, wait := f.dedupe.acquire(req.IdempotencyKey)
		if !leader {
			// follower：等待在途 leader 的结论；等待本身受调用方截止时间约束，
			// 任何情况下都能在可预期时间内得到明确结论而不是无限挂起。
			if err := waitForConclusion(ctx, wait); err != nil {
				return nil, err
			}
			return f.reuseConclusion(req.IdempotencyKey)
		}
		// leader：真正执行；成功或最终失败只发布一次结论。
		res, err := f.runAttempts(ctx, req, sel, rp)
		f.publishConclusion(req.IdempotencyKey, res, err)
		return res, err
	}

	return f.runAttempts(ctx, req, sel, rp)
}

// reuseConclusion 复用 leader 已发布的结论；失败结论还原为可区分错误，绝不伪装成成功。
func (f *Forwarder) reuseConclusion(key string) (*Result, error) {
	o, ok := f.dedupe.lookupOutcome(key)
	if !ok {
		// 极端情况下结论恰好在被唤醒后过期/被淘汰：当作一次全新请求由调用方重试，
		// 这里给出可区分的临时结论，调用方不会被卡死。
		return nil, model.NewError(model.ErrNoRetryBudget, "", "同键结论已过期或被淘汰，请作为全新请求重试")
	}
	if o.Kind == OutcomeSuccess {
		f.logf("[dedupe] key=%s 复用成功结论 instance=%s status=%d", key, o.InstanceID, o.StatusCode)
		return &Result{InstanceID: o.InstanceID, StatusCode: o.StatusCode, DedupeHit: true}, nil
	}
	f.logf("[dedupe] key=%s 复用失败结论 kind=%s cause=%s instance=%s",
		key, o.ErrKind, o.CauseKind, o.InstanceID)
	// 还原失败：外层为最终降级类别（如 no_retry_budget），
	// cause 保留“原来那一类”（connect_failed/timeout/upstream_5xx/upstream_4xx…），
	// model.RootKind 可解包，调用方既能看到降级结论也能区分最初原因。
	detail := "复用同键失败结论: " + o.Detail
	final := model.NewError(o.ErrKind, o.InstanceID, detail)
	if o.CauseKind != "" && o.CauseKind != o.ErrKind {
		final = model.WrapError(o.ErrKind, o.InstanceID,
			detail+"（原始原因="+string(o.CauseKind)+"："+model.DescribeKind(o.CauseKind)+"）",
			model.NewError(o.CauseKind, o.InstanceID, o.Detail))
	}
	return nil, final
}

// publishConclusion 由 leader 在得到“最终”结论后发布一次。
// 重试中途的失败不发布：等待者只应看到整次请求收敛后的唯一结论。
func (f *Forwarder) publishConclusion(key string, res *Result, err error) {
	if err == nil {
		f.dedupe.complete(key, res.InstanceID, res.StatusCode)
		return
	}
	var ce *model.CallError
	instanceID := ""
	detail := err.Error()
	if errors.As(err, &ce) {
		instanceID = ce.InstanceID
		detail = ce.Detail
	}
	f.dedupe.completeFailure(key, model.ErrKind(err), model.RootKind(err), instanceID, detail)
}

// releaseInflight 归还一次 PickAndBegin 占用的在途名额。
func releaseInflight(sel Selector, beganBySelector bool, id string) {
	if !beganBySelector {
		return
	}
	if r, ok := sel.(InflightReleaser); ok {
		r.Release(id)
	}
}

// runAttempts 执行真正的尝试循环（无去重逻辑，去重由 Execute 包裹）。
func (f *Forwarder) runAttempts(ctx context.Context, req model.RequestContext, sel Selector, rp RetryPolicy) (*Result, error) {
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
			// 无论起始还是重试途中候选排空，都是本次请求的最终失败（在途计数此前已逐次归还）。
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
				releaseInflight(sel, beganBySelector, ins.ID) // 归还选择时原子占用的在途计数
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
		status, hdr, _, callErr := doOnce(callCtx, ins, req)
		// 任何结论路径（成功/失败/超时）都立即归还在途名额，计数只涨必落。
		releaseInflight(sel, beganBySelector, ins.ID)
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
		f.logf("[attempt %d/%d] instance=%s 成功 status=%d", attempt, rp.MaxAttempts, ins.ID, status)
		return res, nil
	}
}
