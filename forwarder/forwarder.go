package forwarder

import (
	"context"
	"fmt"
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

// inflightTracker 由同时持有在途计数器的选择器实现。
// 转发器通过它保证：凡 PickAndBegin 原子 +1 的计数，一定由同一计数器 -1 归还，
// 从而“在哪计数就在哪归还”，失败、超时、跳过都不漏归还。
type inflightTracker interface {
	InflightBegin(id string)
	InflightEnd(id string)
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
// 幂等结论收敛：携带 IdempotencyKey 时，leader 在任何结束路径（成功、失败、
// 起始无健康实例）都恰好发布一次结论；在途等待者与后来者复用同一份结论，
// 失败结论保留原始错误类别且绝不以成功形式被复用。
//
// 副作用安全：转发器永不修改健康集合（失败实例不会被“放回”，只是本请求内排除）；
// 非幂等请求（无 IdempotencyKey）仅允许对“确定未产生副作用”的 connect_failed 重试。
type Forwarder struct {
	filter    InstanceFilter
	dedupe    *DedupeStore
	dedupeCfg DedupeConfig
	onBegin   func(id string)
	onEnd     func(id string)
	logf      func(format string, args ...any)
}

// Option 配置转发器。
type Option func(*Forwarder)

// WithInflightHooks 注入在途计数钩子（如 least_conn 的 Begin/End）。
// 若选择器自身实现了 inflightTracker，则以选择器的计数器为准，钩子不生效。
func WithInflightHooks(begin, end func(id string)) Option {
	return func(f *Forwarder) { f.onBegin, f.onEnd = begin, end }
}

// WithDedupeConfig 配置结论复用的保留窗口、容量上限与等待预算。
func WithDedupeConfig(cfg DedupeConfig) Option {
	return func(f *Forwarder) { f.dedupeCfg = cfg }
}

// WithLogger 注入解释性日志。
func WithLogger(logf func(format string, args ...any)) Option {
	return func(f *Forwarder) { f.logf = logf }
}

// New 创建转发器。
func New(filter InstanceFilter, opts ...Option) *Forwarder {
	f := &Forwarder{
		filter: filter,
		logf:   func(string, ...any) {},
	}
	for _, o := range opts {
		o(f)
	}
	f.dedupe = NewDedupeStore(f.dedupeCfg)
	return f
}

// Dedupe 返回结论复用存储，供外部做主动 Sweep 或可观测性检查。
func (f *Forwarder) Dedupe() *DedupeStore { return f.dedupe }

// Execute 按重试策略执行请求。
func (f *Forwarder) Execute(ctx context.Context, req model.RequestContext, sel Selector, rp RetryPolicy) (res *Result, err error) {
	if rp.MaxAttempts < 1 {
		rp.MaxAttempts = 1
	}
	res = &Result{}

	// 幂等去重：同键并发/重试只允许一个调用真正到达后端。
	if req.IdempotencyKey != "" {
		leader, wait := f.dedupe.Acquire(req.IdempotencyKey)
		if !leader {
			return f.awaitOutcome(ctx, req.IdempotencyKey, wait)
		}
		// leader 在任何返回路径（含起始无健康实例、panic 之外的全部错误）
		// 都恰好发布一次结论，等待者不会因此挂住。
		defer func() {
			f.dedupe.Publish(req.IdempotencyKey, CachedOutcome{
				InstanceID: res.InstanceID,
				StatusCode: res.StatusCode,
				Header:     cloneHeader(res.Header),
				Attempts:   append([]model.Attempt(nil), res.Attempts...),
				Err:        err,
			})
			switch {
			case err != nil:
				f.logf("[dedupe] key=%s 发布失败结论 kind=%s，全部同键调用复用该失败",
					req.IdempotencyKey, model.ErrKind(err))
			default:
				f.logf("[dedupe] key=%s 发布成功结论 instance=%s status=%d，后来调用直接复用",
					req.IdempotencyKey, res.InstanceID, res.StatusCode)
			}
		}()
	}

	exclude := make(map[string]bool)
	attempt := 0 // 已发起的真实尝试次数
	skips := 0   // 因并发注销/摘除而跳过的次数（不消耗尝试预算）
	beginSel, hasBeginSel := sel.(BeginSelector)
	// 在途计数优先归还到选择器自己持有的计数器，避免跨计数器错配。
	tracker, hasTracker := sel.(inflightTracker)
	begin := func(id string) {
		if hasTracker {
			tracker.InflightBegin(id)
		} else if f.onBegin != nil {
			f.onBegin(id)
		}
	}
	end := func(id string) {
		if hasTracker {
			tracker.InflightEnd(id)
		} else if f.onEnd != nil {
			f.onEnd(id)
		}
	}

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
			kind := model.ErrNoHealthyInstance
			switch {
			case attempt == 0:
				// 起始即无健康候选：一律 no_healthy_instance，不消耗尝试预算，
				// 也不包成 no_retry_budget（即使 MaxAttempts=1）。
				err = model.Errf(kind, "", "服务 %s 起始无健康候选: %v", req.Service, pickErr)
			case attempt+1 >= rp.MaxAttempts:
				// 最后一笔预算对应的真实尝试已失败（随后重选无候选）：
				// 结论保留最后一次真实失败，包成 no_retry_budget。
				// 注意 attempt 尚未包含刚结束的那一次，故用 attempt+1 判断预算用尽。
				last := res.Attempts[len(res.Attempts)-1]
				cause := model.NewError(last.ErrKind, last.InstanceID, last.Reason)
				err = model.WrapError(model.ErrNoRetryBudget, last.InstanceID,
					fmt.Sprintf("已达最大尝试次数 %d（最后失败 %s），执行确定性降级",
						rp.MaxAttempts, last.ErrKind), cause)
			default:
				err = model.Errf(kind, "",
					"重试过程中健康候选已排空，已尝试 %d 个实例: %v", attempt, res.Attempts)
			}
			res.Attempts = append(res.Attempts, model.Attempt{
				ErrKind: model.ErrKind(err), Reason: model.DescribeKind(model.ErrKind(err))})
			f.logf("[pick] key=%s 结论 kind=%s: %v", req.IdempotencyKey, model.ErrKind(err), err)
			return res, err
		}

		// 连接前二次复核：堵截并发注销 / 并发摘除。跳过不消耗尝试预算。
		if f.filter != nil {
			if cur, ok := f.filter(ins.ID); !ok {
				if beganBySelector {
					end(ins.ID) // 归还选择时原子占用的在途计数
				}
				reason := "实例在连接前已注销/摘除，跳过且不计尝试"
				f.logf("[guard] instance=%s %s", ins.ID, reason)
				res.Attempts = append(res.Attempts, model.Attempt{
					InstanceID: ins.ID, ErrKind: model.ErrInstanceDead, Reason: reason})
				exclude[ins.ID] = true
				skips++
				if skips > 64 {
					err = model.Errf(model.ErrNoHealthyInstance, ins.ID, "候选反复失效，终止")
					return res, err
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
		if !beganBySelector {
			begin(ins.ID)
		}
		status, hdr, _, callErr := doOnce(callCtx, ins, req)
		end(ins.ID) // 成功、失败、超时都必须归还在途计数
		cancel()

		if classified := Classify(callErr, status, ins.ID); classified != nil {
			kind := model.ErrKind(classified)
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
				err = classified
				return res, err
			}
			// 副作用安全：无幂等键时，只有确定未到达业务处理的连接失败可重试。
			if req.IdempotencyKey == "" && kind != model.ErrConnectFailed {
				f.logf("[retry] 非幂等请求且 kind=%s 不保证无重复副作用，停止重试", kind)
				err = classified
				return res, err
			}
			if attempt >= rp.MaxAttempts {
				err = model.WrapError(model.ErrNoRetryBudget, ins.ID,
					"已达最大尝试次数，执行确定性降级（向上游返回失败）", classified)
				return res, err
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

// cloneHeader 复制响应头，避免调用方修改复用结果时污染缓存结论。
func cloneHeader(h http.Header) http.Header {
	if h == nil {
		return nil
	}
	return h.Clone()
}

// awaitOutcome 处理同键 follower：有界等待 leader 发布结论，并原样复用成功或失败结论。
// 等待超过预算（兜底）或自身 ctx 结束时返回明确错误，绝不无限挂起。
func (f *Forwarder) awaitOutcome(ctx context.Context, key string, wait <-chan struct{}) (*Result, error) {
	budget := f.dedupe.cfg.WaitBudget
	if budget > 0 {
		timer := time.NewTimer(budget)
		select {
		case <-wait:
			timer.Stop()
		case <-timer.C:
			err := model.Errf(model.ErrDedupeWaitTimeout, "",
				"幂等键 %s 的在途请求超过等待预算 %s 仍未发布结论", key, budget)
			f.logf("[dedupe] %v", err)
			return &Result{DedupeHit: true}, err
		case <-ctx.Done():
			timer.Stop()
			return &Result{DedupeHit: true},
				model.WrapError(model.ErrTimeout, "", "等待同键结论期间调用方放弃", ctx.Err())
		}
	} else {
		<-wait
	}

	o, ok := f.dedupe.Lookup(key)
	if !ok {
		// 理论不可达：leader 的 Publish 先关屏障再删等待队列。
		return &Result{DedupeHit: true},
			model.NewError(model.ErrDedupeWaitTimeout, "", "幂等键等待被异常释放且无结论")
	}
	r := &Result{
		InstanceID: o.InstanceID,
		StatusCode: o.StatusCode,
		Header:     cloneHeader(o.Header),
		Attempts:   append([]model.Attempt(nil), o.Attempts...),
		DedupeHit:  true,
	}
	if o.Err != nil {
		f.logf("[dedupe] key=%s 复用失败结论 kind=%s instance=%s（不记为成功）",
			key, model.ErrKind(o.Err), o.InstanceID)
		return r, o.Err
	}
	f.logf("[dedupe] key=%s 复用成功结论 instance=%s status=%d", key, o.InstanceID, o.StatusCode)
	return r, nil
}
