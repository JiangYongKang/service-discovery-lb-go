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
| `forwarder` | 真实 HTTP 转发、传输错误分类、**同键结论收敛（成功/失败结果复用、TTL、LRU 容量淘汰）**、重试预算与确定性降级 |
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
| `least_conn` | 选择在途连接数最少者，并列时取候选顺序第一个；`PickAndBegin` 在同一临界区内完成“选择 + 计数 +1” |

**在途计数闭环（只涨必落）**：`PickAndBegin` 在均衡器临界区内完成选择与 +1，
归还动作由选择器自身的 `Release`（落到同一代均衡器的 `End`）在每次尝试结束时执行——
**成功、失败、超时、连接前复核被跳过**的每一条路径都成对归还，不依赖外部钩子。因此：

- 长请求与短请求混合、请求中途失败/超时/实例被跳过，计数都不会错乱；
- 一批请求全部结束后，在途计数（`leastConn.TotalInflight` / `Dispatcher.InflightTotal`）**回到 0**；
- 并发涌入在同一临界区“选择 + 计数”，不会因计数更新滞后而反复选中同一实例。

每个规则版本拥有独立的均衡器游标与在途计数（按 `规则版本 + 服务` 缓存），
因此热更新后旧版本在途请求使用的仍是旧策略/旧权重/旧计数。
`Dispatcher.InflightTotal(service)` 汇总该服务在所有已缓存版本均衡器上的在途数之和；
配合调度日志 `rules_v=<规则版本> registry_v=<注册表版本> inflight_now=<当时在途总数>`，
**热更新之后的每一个路由结论仍可由“当时的在途数 + 规则版本 + 注册表版本”解释清楚，两代计数不串味。**

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
| `rule_invalid` / `service_not_found` | 规则非法 / 服务未配置 | – |

`CallError` 携带 `Kind`、`InstanceID`、可读 `Detail` 与底层 cause，
可用 `model.ErrKind(err)` 解包最终类别、`model.RootKind(err)` 解包原因链最内层的
**原始那一类**（如 `no_retry_budget` 内的 `connect_failed`/`timeout`/`upstream_5xx`）、
`model.DescribeKind(kind)` 取中文判定依据。同键失败结论复用时保留这两层类别。

### 5.2 确定性降级规则

1. 起始即无健康候选 → 立即返回 `no_healthy_instance`（不消耗尝试预算）；
2. 选中后在连接前发现实例已注销/被摘除 → 跳过该实例、不消耗预算，记录 `instance_dead`；
3. 4xx 等不可重试错误 → 首次失败即返回，**保留原始错误类别**（不被包装成超时等）；
4. 可重试错误但已达 `MaxAttempts` → 返回 `no_retry_budget`，cause 保留最后一次错误；
5. 重试途中健康候选被排空 → 返回 `no_healthy_instance`，`Attempts` 保留完整尝试链。

### 5.3 重试不产生重复副作用与同键结论收敛（结果复用）

请求携带 `IdempotencyKey` 时，由带保留窗口与容量上限的 `DedupeStore` 把同键调用收敛到
**同一份说得清的结论**：

- **只有第一个调用（leader）真正访问后端**；leader 在途期间到达的调用（follower）等待同一结论；
- **成功结论**：等待者与保留窗口内后来的调用直接复用（`Result.DedupeHit=true`），后端只执行一次。
  测试：20 个并发同键请求，后端**仅处理 1 次**，1 leader + 19 复用；窗口内重放仍为 0 次额外副作用。
- **失败结论同样被一致复用，但绝不被记成成功**：
  - `DedupeStore.Lookup` 只返回成功结论；失败结论永远不会被后来的调用当成成功结果；
  - 失败结论同时记录**最终降级类别**（如 `no_retry_budget`）与**原因链根类别**
    （`connect_failed`/`timeout`/`upstream_5xx`/`upstream_4xx`/`no_healthy_instance`）。
    可用 `model.ErrKind(err)` 取最终类别、`model.RootKind(err)` 取“最初那一类”；
  - `connect_failed`、`timeout`、4xx/5xx、**整套后端无健康实例**这几类失败，
    所有并发等待者与窗口内重放者都拿到**同一个**可区分失败。
- **保留窗口（TTL）**：成功与失败使用各自独立窗口
  （`DedupeConfig.SuccessTTL` 默认 30s、`FailureTTL` 默认 5s，均可配置）。
  过期条目惰性清理（占位、读取、容量整理时触发）；**过期之后同键再来即一次全新请求**，
  正常执行，不会卡住或报错；失败窗口过期 + 后端恢复后同键重试可以成功。
- **容量上限（LRU）**：`DedupeConfig.Capacity`（默认 4096）限制结论总条数，
  超限时淘汰**最久未被访问**的键；命中复用会刷新 LRU 位置。在途键不在 LRU 结构中，不会被误淘汰。
- **等待有界**：follower 等待受自身 `ctx` 截止时间约束（`waitForConclusion`），
  不会因 leader 长耗时或异常而无限挂起；超时/取消返回可区分的 `timeout`。
  “当前没有任何健康实例”时，leader 在选实例阶段立即得到 `no_healthy_instance` 并发布，
  同键并发调用与窗口内重放都在**可预期的极短时间**内拿到明确失败。
- **失败不复活实例**：失败重放只读取缓存结论，**不经过、也不修改健康集合**，
  摘除/无健康的实例不会因为重放被重新当成可用。
- 无幂等键的请求：只允许对**确定未到达业务处理**的 `connect_failed` 重试；
  `timeout`（可能已在后端执行）直接返回，绝不重试，避免重复扣款类副作用。
- 后端业务计数只在“真正开始处理请求”后递增，可直接核对副作用次数。

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

## 8. 兼容范围与行为约定

- **仅对携带 `IdempotencyKey` 的请求启用结果复用**；无键请求行为与既有版本一致（按重试策略执行，
  不写入结论缓存）。键的生成与去重粒度（业务键，而非实例地址）由调用方保证。
- **成功/失败结论都只在“整次请求的最终结论”产生时发布一次**：重试中途的单次失败不会提前发布，
  在途等待者只看到收敛后的唯一结论。
- **失败可复用，但有独立且通常更短的失败保留窗口**：默认成功 30s、失败 5s、容量 4096，
  可通过 `mesh.Config.Dedupe`（`forwarder.DedupeConfig`）覆盖；零值取默认。
- 失败结论的复用语义是“**短期内一致地返回同一个可区分失败**”，不是熔断；窗口过期后允许重新尝试，
  且重放路径完全不触碰健康状态机（摘除的实例不会因重放复活）。
- **淘汰是安全惰性的**：过期/超量淘汰后同键再来当作全新请求执行，不会报错或卡住；
  即使 follower 被唤醒时结论恰好过期，也会得到“请作为全新请求重试”的可区分临时结论。
- 在途计数的选择与归还绑定在同一代（规则版本 + 服务）均衡器上；热更新只影响更新后开始的请求，
  在途请求继续使用其开始时的版本，两代计数相加即真实在途。
- 并发安全：结论缓存、注册表、规则发布、在途计数全部在各自临界区/原子指针下访问，
  整套用例在 `go test ./... -race -count=10` 下无 DATA RACE。

---

## 9. 验证方法

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全量单元/集成测试
go test ./... -v

# 并发场景必须在竞态检测下多轮复跑（全部通过、无 DATA RACE）
go test ./... -race -count=10

# 覆盖率
go test ./... -cover

# 端到端演示（注册→就绪→故障摘除→重试→热更新）
go run ./cmd/demo
```

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
| `forwarder.TestDedupeConcurrentFailureReplayedWithKind` | 同键并发失败结论一致复用、后端仅 1 次、失败不记成成功 |
| `forwarder.TestDedupeFailureKindsDistinguishable` | connect/timeout/5xx/4xx 根类别可区分地被并发与重放一致复用 |
| `forwarder.TestDedupeNoHealthyInstanceBoundedWait` | 无健康实例 8 并发在有限时间内得到 no_healthy_instance，重放同样快速失败 |
| `forwarder.TestDedupeSuccessTTLExpiry` / `TestDedupeFailureTTLExpiry` | 成功/失败保留窗口过期淘汰，同键再来为全新请求（失败恢复后可成功） |
| `forwarder.TestDedupeStoreLRU` | 容量上限 LRU 淘汰最久未访问、命中刷新、淘汰后可重新发布 |
| `forwarder.TestDedupeStoreFailureAndSuccessTTL` | 失败/成功独立 TTL（注入时钟），失败不伪装成功 |
| `forwarder.TestDedupeFollowerBoundedByContext` | follower 等待受自身截止时间约束，不无限挂起 |
| `forwarder.TestInflightCounterReturnsZero` | 长/短/失败/跳过混合并发结束后在途计数归零 |
| `forwarder.TestFailedInstanceNeverReturnedToHealthySet` | 失败实例不被重新放回健康集合 |
| `forwarder.TestNonIdempotentTimeoutNotRetried` | 非幂等超时不重试 |
| `mesh.TestMeshSameKeyConcurrentSingleSideEffect` | 端到端 20 并发同键 1 次副作用，窗口内重放仍复用 |
| `mesh.TestMeshNoHealthyInstanceBoundedAndReplay` | 端到端整组不可用有界返回，失败实例不复活，重放同结论 |
| `mesh.TestMeshFailureOutcomeEvictedThenFreshSuccess` | 端到端失败窗口过期 + 后端恢复后同键重新成功 |
| `mesh.TestMeshLeastConnInflightZeroAndHotUpdate` | 端到端热更新交错下 60 请求结束跨版本在途归零、负载不压单实例、可重复归零 |
| `mesh.TestRetryOnConnectFailureNoDuplicateSideEffect` | 连接失败重试到其他健康实例 |
| `mesh.TestTimeoutExhaustsBudget` / `Test4xxNotRetried` | 超时降级预算 / 4xx 不重试 |
| `mesh.TestConcurrentUpDown` | 8 worker × 50 请求与反复上下线并发（-race） |
| `mesh.TestProbeJitterKeepsInstanceOut` | 摘除、RECOVERING 不路由、恢复后回归 |
| `mesh.TestHotUpdateDuringRequests` | 240 请求与合法/非法热更新交错并发（-race） |
| `mesh.TestDeregisterRaceGuard` | 200 请求与注销竞速，全部得到可区分结论 |
| `router.TestFailedUpdateKeepsOldVersion` / `TestInFlightRequestKeepsOldSnapshot` | 热更新原子性与在途一致性 |
