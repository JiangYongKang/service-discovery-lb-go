# 本地服务发现与流量调度（service-discovery-lb-go）

用本地进程实例（`httptest` loopback HTTP 服务）模拟后端，提供一套**可复现、结论可解释**的
服务发现、健康探测摘除、多策略负载均衡、确定性降级/重试与规则热更新能力。

设计目标：在节点上下线、健康探测抖动、并发注销、规则热更新等并发场景下，
路由结论保持一致，且每一个失败都能被区分到明确的错误类别（`ErrorKind`）与判定依据。

---

## 1. 目录结构

| 包 | 职责 |
| --- | --- |
| `model` | 核心类型：实例、健康状态、请求/尝试记录、**可区分错误类别 `CallError`** |
| `discovery` | 并发安全的注册表：注册/注销、失效标记、版本化不可变快照、健康集合视图 |
| `health` | 健康状态机（阈值摘除/恢复）、HTTP 探针、周期/单步探测协调器 `Monitor` |
| `lb` | `round_robin` / `random` / `weighted`（SWRR）/ `least_conn` 四种策略 |
| `forwarder` | 真实 HTTP 转发、传输错误分类、**幂等结论收敛（成功/失败一致复用、TTL 与容量 FIFO 淘汰）**、重试预算与确定性降级、least_conn 在途计数成对归还 |
| `router` | 规则版本化、原子热更新（validate-then-swap）、请求编排器 `Dispatcher` |
| `backend` | 本地后端进程模拟：`/healthz` 探测、故障注入（503/延迟/状态码/断连） |
| `mesh` | 门面：把上述组件装配成注册、探测、调用、热更新的统一 API |
| `cmd/demo` | 端到端可运行示例 |

---

## 2. 服务发现协议

### 2.1 注册 / 注销

- `Registry.Register(ins)`：
  - 实例必须带 `ID` 与 `Service`，否则返回 `rule_invalid`；
  - **重复注册**：实例仍在册时返回 `duplicate_registration`，明确拒绝；
  - 同 ID 实例在**注销后允许重新注册**（重新上线），此时清除失效标记、健康状态重置为 `UNKNOWN`，
    必须重新通过探测就绪后才会被路由。
- `Registry.Deregister(id)`：
  - 首次注销：从注册表移除并写入 `dead` 失效集合，版本号递增；
  - 重复注销：返回 `instance_dead`（此前已注销），与 `instance_not_registered`（从未注册）区分；
  - 操作幂等：32 个 goroutine 并发注销同一实例，恰好 1 次成功、其余全部得到 `instance_dead`。
- 转发路径在**真正建立连接之前**通过 `Dispatcher.Filter` 二次复核实例状态，
  堵截“已被选中、但在连接前被并发注销/摘除”的实例；该跳过**不消耗重试预算**。

### 2.2 版本化快照

注册表在成员或健康状态每次变化时递增版本号。`Snapshot()` 返回深拷贝的不可变视图：

- 在途请求在开始时取一次快照，整个请求（含重试）都依据该视图；
- 旧快照不会被后续修改污染（有测试显式断言）。

---

## 3. 健康判定规则

健康状态是一个**确定性有限状态机**，只有显式 `HEALTHY` 才可路由（`HealthState.Routeable()`）。

```
                 连续成功 >= RecoverThreshold
   UNKNOWN ───────────────────────────────► HEALTHY
     │                                        │
     │ 连续失败 >= FailThreshold              │ 连续失败 >= FailThreshold
     ▼                                        ▼
   UNHEALTHY ◄───────────────────────────────┘
     │  ▲   首次成功                  ▲
     │  └────── RECOVERING ──────────┘
     │   恢复中成功但未达阈值         恢复期内任一失败（抖动清零）
     └── 恢复期内任一失败
```

| 状态 | 可路由 | 含义 |
| --- | --- | --- |
| `UNKNOWN` | 否 | 新注册，连续成功尚未达到就绪阈值 |
| `HEALTHY` | 是 | 唯一可路由状态 |
| `UNHEALTHY` | 否 | 连续失败达到摘除阈值 |
| `RECOVERING` | 否 | 摘除后出现成功，但连续成功未达恢复阈值 |

关键规则：

- **抖动安全**：任何一次失败都会把连续成功计数清零。恢复阈值未连续达成前，
  实例只能停留在 `RECOVERING`/`UNHEALTHY`，绝不参与路由。
- 健康集合的唯一写者是健康检查状态机；**转发层永不修改健康集合**
  （重试时失败实例只是“本次请求内排除”，不会被摘除，也不会被重新放回）。
- 探测通过 HTTP `GET /healthz` 完成：连接错误或非 2xx/3xx 一律算探测失败（`probe_failed`）。
- `Monitor.RunOnce()`/`RunOnceWith()` 提供同步单步探测，便于确定性测试；
  `Start()` 提供周期后台循环。每次迁移都打印实例状态与判定依据。

---

## 4. 负载均衡策略

所有均衡器都只在“**当前健康候选 − 本次已尝试实例**”中选择；候选为空返回 `no_healthy_instance`。

| 策略 | 行为 |
| --- | --- |
| `round_robin` | 全局单调游标对当前候选集合取模；健康集合变化结论仍确定 |
| `random` | 随机选择，随机源可注入种子（测试可复现） |
| `weighted` | nginx 风格平滑加权轮询 SWRR；权重缺省 1，长期比例精确且单轮平滑 |
| `least_conn` | 选择在途连接数最少者，并列时取候选顺序第一个；`PickAndBegin` 在同一临界区内完成“选择 + 计数 +1”，结束（成功/失败/超时/跳过）后由同一版本均衡器 -1，全部结束计数归零 |

每个规则版本拥有独立的均衡器游标（按 `规则版本 + 服务` 缓存），
因此热更新后旧版本在途请求使用的仍是旧策略/旧权重。

---

## 5. 转发、降级与重试语义

### 5.1 错误分类（失败原因可区分）

| `ErrorKind` | 触发条件 | 默认可重试 |
| --- | --- | --- |
| `duplicate_registration` | 重复注册 | – |
| `instance_dead` / `instance_not_registered` | 已注销失效 / 从未注册 | – |
| `instance_unhealthy` / `instance_recovering` | 摘除态 / 恢复观察期 | – |
| `no_healthy_instance` | 无任何健康候选（起始或重试途中被排空） | – |
| `connect_failed` | 连接被拒/重置等（确定未执行业务） | 是 |
| `timeout` | 单次尝试超过 `PerCallTimeout` | 仅幂等请求 |
| `upstream_5xx` | 后端 5xx | 是 |
| `upstream_4xx` | 后端 4xx | **否** |
| `no_retry_budget` | 达到 `MaxAttempts` 后仍失败 | – |
| `dedupe_wait_timeout` | 同键在途 leader 超过等待预算未发布结论（兜底，正常路径不会出现） | – |
| `rule_invalid` / `service_not_found` | 规则非法 / 服务未配置 | – |

`CallError` 携带 `Kind`、`InstanceID`、可读 `Detail` 与底层 cause，
可用 `model.ErrKind(err)` 解包、`model.DescribeKind(kind)` 取中文判定依据。

### 5.2 确定性降级规则

1. 起始即无健康候选 → 立即返回 `no_healthy_instance`（不消耗尝试预算）；
2. 选中后在连接前发现实例已注销/被摘除 → 跳过该实例、不消耗预算，记录 `instance_dead`；
3. 4xx 等不可重试错误 → 首次失败即返回，**保留原始错误类别**（不被包装成超时等）；
4. 可重试错误但已达 `MaxAttempts` → 返回 `no_retry_budget`，cause 保留最后一次错误；
5. 重试途中健康候选被排空 → 返回 `no_healthy_instance`，`Attempts` 保留完整尝试链。

### 5.3 重试不产生重复副作用

- 请求携带 `IdempotencyKey` 时，`DedupeStore` 保证同一键下：
  - 只有第一个调用（leader）真正访问后端；
  - 进行中到达的并发调用等待同一结果；
  - 完成后到达的调用直接复用结果（`Result.DedupeHit=true`）。
  - 测试：20 个并发同键请求，后端**仅处理 1 次**，1 leader + 19 复用。
- 无幂等键的请求：只允许对**确定未到达业务处理**的 `connect_failed` 重试；
  `timeout`（可能已在后端执行）直接返回，绝不重试，避免重复扣款类副作用。
- 后端业务计数只在“真正开始处理请求”后递增，可直接核对副作用次数。

### 5.4 结果复用（结论收敛）语义

携带 `IdempotencyKey` 的请求，无论同时打进来还是前后脚打进来，最终都收敛到
**同一份结论**（`forwarder.CachedOutcome`）。成功与失败共用同一结构，由 `Err` 区分：

| 维度 | 语义 |
| --- | --- |
| 收敛对象 | leader 实际得到的最终结论：成功（实例/状态码/响应头/尝试链）或失败（错误对象） |
| 保留窗口 `TTL` | 每条结论保留固定时长（**成功与失败一致**，默认 30s），到期后惰性删除 |
| 容量上限 `MaxEntries` | 结论总条数上限（默认 4096）；发布新结论超限时先清过期项，再按**最旧发布顺序 FIFO** 淘汰 |
| 淘汰后行为 | 过期或被淘汰的键再次请求时，当作**一次全新请求**正常执行，不卡住、不报错 |
| 主动清理 | `Forwarder.Dedupe().Sweep()` / `Mesh.SweepResultCache()` 可周期性清理；不清理也安全（惰性） |
| 失败能否复用 | **能，且必须复用为失败**：复用方拿到的是同一个错误对象，`model.ErrKind(err)` 保持原类别，绝不会以“成功 + status=0”返回 |

失败结论的类别区分（所有在途等待者与后来者拿到同一结论）：

| 原始情形 | 最终结论 `ErrKind` | 原始原因保留 |
| --- | --- | --- |
| 起始/重放时整套后端无 HEALTHY 实例 | `no_healthy_instance`（立即返回，不消耗预算） | – |
| 连不上后端 | 预算内可重试；耗尽后 `no_retry_budget` | cause 链保留 `connect_failed` |
| 单次尝试超时 | 幂等请求可重试；耗尽后 `no_retry_budget` | cause 链保留 `timeout` |
| 后端返回 4xx | `upstream_4xx`（首次即返回，不重试） | 原类别 |
| 后端返回 5xx | 预算内重试；耗尽后 `no_retry_budget` | cause 链保留 `upstream_5xx` |

判定依据可用 `model.ErrKind(err)` 读最终类别、沿 `errors.Unwrap` 链（或 `errors.As`）
读原始类别，`model.DescribeKind(kind)` 取中文解释。

**无健康实例的有界性**：leader 即使在“起始即无健康候选”路径也会发布结论，
因此同键并发调用不会挂起；等待方另设 `WaitBudget`（默认 10s）兜底，
超时得到明确的 `dedupe_wait_timeout`，绝不无限等待。调用方自身 ctx 结束时返回
`timeout`（detail 标注“等待同键结论期间放弃”）。

配置入口：`forwarder.WithDedupeConfig(DedupeConfig{TTL, MaxEntries, WaitBudget, Now})`，
或装配时使用 `mesh.Config{DedupeTTL, DedupeMaxEntries, DedupeWaitBudget}`；
`Now` 为可注入时钟，供确定性单测推进 TTL。

### 5.5 在途计数（least_conn）如何解释

- `least_conn` 选择当前在途请求数最少的健康候选；`PickAndBegin` 在**同一临界区**
  完成“选择 + 计数 +1”，因此并发涌入时不会因计数更新滞后而反复选中同一实例。
- **在哪计数就在哪归还**：转发器通过选择器自身实现的
  `InflightBegin/InflightEnd`（内部经 `lb.InflightCounter`）把 -1 归还到
  **同一个规则版本**的均衡器。每次尝试无论成功、失败、超时还是“连接前被跳过”，
  +1 都严格成对地有一次 -1；一批请求全部结束后计数必须回到起始值 0（有测试断言）。
- 每次调度日志输出三元组解释：`rules_v`（规则版本）+ `registry_v`（注册表版本）
  + `inflight`（选择时刻各实例在途数快照），即“**当时的在途数 + 规则版本 +
  注册表版本**”完整决定路由结论。
- 与热更新的关系：均衡器按“规则版本 + 服务”缓存，热更新换版后新版本的计数器
  **从 0 开始**，不继承旧版本在途数；旧版本在途请求持有的是旧均衡器引用。
  可用 `Dispatcher.BalancerInflight(version, service)` 查询某版本计数快照。
- 失败实例不会因失败重放被“放回”健康集合：转发层从不修改健康集合，
  失败只是本次请求内排除 + 复用失败结论；健康集合的唯一写者仍是健康检查状态机。

---

## 6. 路由规则动态更新与运行期一致性

- 规则以整版 `Rules{Version, ByService}` 为单位，发布前做深拷贝 + `Validate()`：
  - 策略名必须合法、`MaxAttempts` 非负、权重非负、重试错误类别受白名单约束；
  - **任一服务规则非法 → 整版拒绝**（包括“一个合法、一个非法”的部分更新）。
- 热更新采用 **validate-then-swap**：校验通过并规范化（补默认策略/超时/次数）后，
  用一次原子指针替换发布；校验失败时旧版本完全保留，绝不出现部分生效。
- 在途请求在开始时取 `Router.Current()`，整个请求生命周期都使用该版本，
  热更新不会中途改变其策略/权重/重试参数。测试在 `-race` 下验证
  “64 个在途读 + 20 次并发热更新”期间所有旧快照保持完整可用。

---

## 7. 后端模拟与故障注入

`backend.Instance` 基于 `httptest` 启动真实 loopback HTTP 进程：

- `GET /healthz`：受 `Behavior.FailProbe` / `ProbeStatus` 控制；
- 业务路径：受 `CallStatus`（注入 4xx/5xx）与 `Latency`（注入超时）控制；
- `Close()` 关闭监听器模拟进程下线（连接拒绝）；
- `SetBehavior()` 可在运行期动态切换以制造探测抖动；
- 响应头 `X-Instance-ID` / `X-Instance-Addr` 用于在并发上下线测试中
  核对“响应确实来自调用时刻真实存活的进程地址”。

---

## 8. 验证方法

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全量单元/集成测试（日志带“业务 / 用例 / 结果”前缀，-v 可见覆盖明细）
go test ./... -v

# 并发场景必须在竞态检测下多轮复跑（全部通过、无 DATA RACE）
go test ./... -race -count=10

# 覆盖率
go test ./... -cover

# 端到端演示（注册→就绪→故障摘除→重试→热更新）
go run ./cmd/demo
```

### 8.1 本轮新增/扩展的关键测试

| 测试 | 覆盖的业务 / 用例 / 结果 |
| --- | --- |
| `forwarder.TestDedupeConcurrentSameKeySingleSideEffect` | 业务=幂等转发；同键 20 并发；结果=后端仅 1 次副作用，1 leader + 19 复用 |
| `forwarder.TestFailureOutcomeConcurrentlyReused` | 业务=失败收敛；4xx / 5xx；结果=12 并发 + 重放全部拿到同一失败类别，无一记为成功 |
| `forwarder.TestTransportFailureOutcomeReused` | 业务=失败收敛；连接失败 / 超时；结果=`no_retry_budget` 的 cause 分别保留 `connect_failed`/`timeout` |
| `forwarder.TestExpiredFailureRetriedAsFreshRequest` | 业务=保留窗口；失败缓存 TTL 过期 + 后端恢复；结果=同键作为全新请求成功 |
| `forwarder.TestFollowerBoundedWaitWhenLeaderNeverPublishes` | 业务=有界等待；leader 异常不发布；结果=follower 在 WaitBudget 内得到 `dedupe_wait_timeout` |
| `forwarder.TestDedupeTTLExpiryMakesKeyFresh` | 业务=存储 TTL；过期清理、Sweep 只删过期、过期键再来为全新 leader |
| `forwarder.TestDedupeCapacityFIFOEviction` | 业务=存储容量；超量按 FIFO 淘汰、条数恒定、被淘汰键可重新执行 |
| `forwarder.TestDedupeFailureOutcomeReusedAsFailure` | 业务=失败复用；六类失败；结果=类别可区分且 `Failed()=true` |
| `forwarder.TestDedupeWaitersConverge` | 业务=在途收敛；16 等待者；成功/失败两版均收敛到同一份结论 |
| `mesh.TestNoHealthySameKeyConvergesBounded` | 业务=整套后端不可用；同键 24 并发 + 重放 + 全新键；结果=有限时间内全部 `no_healthy_instance` |
| `mesh.TestCachedFailureExpiresThenFreshSuccess` | 业务=淘汰；短 TTL 下失败过期、实例恢复；结果=同键全新执行成功、后端处理 1 次 |
| `mesh.TestResultCacheCapacityEvictionAtMesh` | 业务=容量上限；容量 3 发布 5 条；结果=最旧 FIFO 淘汰，淘汰键重放真正再打后端 |
| `mesh.TestLeastConnInflightReturnsToZero` | 业务=在途计数；2 长 + 20 短混合；结果=峰值与真实在途一致、结束后归零 |
| `mesh.TestLeastConnSurgeSpreadsAcrossInstances` | 业务=并发涌入；40 请求同时放行；结果=均匀分散（极差受限），无反复选中同一实例 |
| `mesh.TestLeastConnInflightZeroAfterFailures` | 业务=失败/超时；连接失败 + 5xx + 超时 12 次；结果=计数全部成对归还归零 |
| `mesh.TestLeastConnInflightAcrossHotUpdate` | 业务=热更新并发；旧版本在途时换版；结果=新版本计数从 0 起、不串味、结束归零 |
| `lb.TestLeastConnBalancedPairsReturnToZero` | 业务=计数单元不变量；200 次交错 Begin/End（含跳过/排除候选）；结果=严格归零 |

既有并发用例（`TestConcurrentUpDown` / `TestHotUpdateDuringRequests` /
`TestDeregisterRaceGuard` 等）在 `-race -count=10` 下多轮复跑不回归。

---

## 9. 兼容范围与约束

- **Go 版本**：go.mod 声明 `go 1.25.0`（在 go1.26 工具链下构建验证）。
- **API 兼容**：
  - `forwarder.NewDedupeStore()` 无参构造改为 `NewDedupeStore(DedupeConfig{})`；
    零值配置即等价旧默认（另加 TTL/容量/等待预算），仅包内使用，外部无破坏；
  - `forwarder.New`、`Forwarder.Execute`、`router.Dispatcher`、`mesh.New` 的既有
    签名与默认行为保持兼容；新增能力均通过 Option 或 `mesh.Config` 新字段开启；
  - 新增导出：`forwarder.CachedOutcome/DedupeConfig`、`Forwarder.Dedupe()`、
    `lb.InflightCounter`、`Dispatcher.BalancerInflight`、
    `Mesh.SweepResultCache()/CachedResultCount()`。
- **语义变化（行为修正，调用方需知悉）**：
  - 同键失败不再被错误地当成成功复用：复用失败结论时 `Execute` 返回非 nil error；
  - 起始无健康实例时同键 follower 不再无限等待，而是立即收敛到 `no_healthy_instance`；
  - 单实例/候选提前排空且预算已用尽时，最终结论为 `no_retry_budget`（cause 保留
    最后一次真实失败类别），此前会误报为 `no_healthy_instance`；
  - `least_conn` 在真实装配下的在途计数现在会正确回落（此前转发器未把 -1
    归还到均衡器），一批请求结束后归零。
- **不引入**后台清理协程：淘汰全部惰性执行；`Sweep` 为可选的主动维护入口。
- **不修改**既有注册/注销、健康状态机、规则校验与热更新的原子性保证。

### 8.2 既有能力的测试（回归基线）

测试覆盖（均带实例状态与判定依据日志，`go test -v` 可见）：

| 测试 | 覆盖场景 |
| --- | --- |
| `discovery.TestRegisterDuplicateAndDeregister` | 重复注册、重复/不存在注销、重新上线 |
| `discovery.TestConcurrentDeregisterOnce` | 32 并发注销：结论稳定、错误可区分 |
| `discovery.TestHealthySetRoutingGate` | UNKNOWN/RECOVERING/UNHEALTHY 一律不路由 |
| `health.TestStateMachineTransitions` | 完整状态迁移与阈值判定依据 |
| `health.TestJitterNeverRouteableUntilThreshold` | 探测抖动下恢复阈值未达成绝不路由 |
| `lb.*` | 轮询确定性、空候选、SWRR 精确平滑分布、least-conn 并发不变量、非法策略 |
| `forwarder.TestDedupeConcurrentSameKeySingleSideEffect` | 20 并发同键仅 1 次副作用 |
| `forwarder.TestFailedInstanceNeverReturnedToHealthySet` | 失败实例不被重新放回健康集合 |
| `forwarder.TestNonIdempotentTimeoutNotRetried` | 非幂等超时不重试 |
| `mesh.TestRetryOnConnectFailureNoDuplicateSideEffect` | 连接失败重试到其他健康实例 |
| `mesh.TestTimeoutExhaustsBudget` / `Test4xxNotRetried` | 超时降级预算 / 4xx 不重试 |
| `mesh.TestConcurrentUpDown` | 8 worker × 50 请求与反复上下线并发（-race） |
| `mesh.TestProbeJitterKeepsInstanceOut` | 摘除、RECOVERING 不路由、恢复后回归 |
| `mesh.TestHotUpdateDuringRequests` | 240 请求与合法/非法热更新交错并发（-race） |
| `mesh.TestDeregisterRaceGuard` | 200 请求与注销竞速，全部得到可区分结论 |
| `router.TestFailedUpdateKeepsOldVersion` / `TestInFlightRequestKeepsOldSnapshot` | 热更新原子性与在途一致性 |
