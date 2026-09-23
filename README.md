# 本地服务发现与流量调度（service-discovery-lb-go）

用本地进程实例（真实 TCP 监听）模拟后端服务，提供服务注册/注销、健康探测摘除、
多种负载均衡策略、带确定降级语义的转发重试，以及路由规则的版本化热更新。
所有路由结论都带有**可解释的逐跳记录与拒绝原因**，并通过并发测试保证上下线、
探测抖动与热更新期间结论稳定一致。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `discovery` | 线程安全注册中心：注册/注销、实例状态机、版本号与不可变快照 |
| `health` | 周期健康探测：连续成败阈值、恢复中状态、传输失败即时反馈 |
| `lb` | 负载均衡策略：轮询、随机、平滑加权、一致性哈希 |
| `route` | 路由规则版本表：全量校验、原子发布、失败保留上一版 |
| `forward` | 转发器：快照选路、发前复核、重试预算、fail-open 降级、结论解释 |
| `backend` | 本地 TCP 模拟后端：健康翻转/假死注入/下线，TCP 探活实现 |
| `sdlb` | 门面，把上述组件装配为 `New / Start / RegisterInstance / Call / SetRules` |
| `cmd/demo` | 可运行的端到端演示 |

## 快速开始

```bash
go run ./cmd/demo          # 端到端演示
go test ./... -race -count=3   # 全部测试（竞态检测，重复 3 轮）
go test ./sdlb/ -race -v       # 只看真实 TCP 后端的集成测试
```

## 服务发现协议

### 实例生命周期状态

```
registered ──首次探活成功──────────────▶ healthy
healthy    ──1 次探测失败──────────────▶ probing   （立即不可路由）
probing    ──连续失败达 FailureThreshold▶ unhealthy （摘除）
unhealthy  ──1 次探测成功──────────────▶ probing   （恢复中，仍不可路由）
probing    ──连续成功达 SuccessThreshold▶ healthy   （恢复）
任意活跃状态 ──Deregister──────────────▶ deregistered（墓碑，终态）
```

* **只有 `healthy` 可路由**。`probing`（失败后/恢复中）、`unhealthy`、
  `deregistered` 一律不会收到请求。
* 注册中心每次写入单调递增 **版本号** 并广播变更（`WatchVersion`）。
* `Snapshot()` 返回深拷贝的不可变视图；在途路由持有快照后，后续上下线不影响它。

### 注册/注销拒绝语义（原因可区分）

所有显式拒绝都返回 `*discovery.RejectError`，可用 `errors.As` 读取 `Reason`，
且哨兵错误可用 `errors.Is` 匹配：

| 场景 | Reason | 哨兵错误 |
| --- | --- | --- |
| 注册仍然有效的重复 ID | `RejectDuplicateRegister` | `ErrDuplicateInstance` |
| 对已注销实例再次注册（墓碑不可复用） | `RejectInstanceDeregistered` | `ErrInstanceDeregistered` |
| 对已注销实例再次注销 | `RejectInstanceDeregistered` | `ErrInstanceDeregistered` |
| 注销/操作不存在的 ID | `RejectInstanceNotFound` | `ErrInstanceNotFound` |

## 健康判定规则

`health.Config` 字段（默认值见 `DefaultConfig`）：

| 参数 | 含义 | 默认 |
| --- | --- | --- |
| `Interval` | 正常探测周期 | 200ms |
| `Timeout` | 单次探测超时（超时即失败） | 100ms |
| `FailureThreshold` | 连续失败多少次摘除为 `unhealthy` | 3 |
| `SuccessThreshold` | 恢复中需连续成功多少次才回 `healthy` | 2 |
| `ProbeInterval` | `probing` 实例的更快探测周期 | 100ms |

* **抖动保护**：健康实例第 1 次失败立即进 `probing`（马上不路由），但只有连续
  失败达到阈值才摘除；恢复时必须连续成功达到阈值，单次成功不会放回流量，
  避免探测抖动造成的反复横跳。
* 每次判定产出一条 `Decision`（旧状态→新状态、连续失败数、原因），写入带缓冲
  通道并可写日志，这是单测日志中“实例状态与判定依据”的来源。
* **并发注销窗口**：探测若在 `Deregister` 之后才返回成功，`Transition` 对
  `deregistered` 实例一律返回失败，实例不会被“晚到的成功”复活。
* **传输失败即时反馈**：转发器在“连接被拒绝”时调用
  `Checker.ReportTransportFailure(id)`，实例立刻进 `probing`（失败计数 +1，
  达阈值直接 `unhealthy`），无需等待下一轮探测；已摘除/已注销实例不会因此
  回到健康集合。

## 负载均衡策略

* `round-robin`：按实例 ID 排序后轮转；输入顺序不影响结论，计数器跨请求复用。
* `random`：等概率随机（排序索引上的稳定随机）。
* `weighted`：平滑加权轮询（SWRR 思想），权重来自规则 `Weights`。
* `consistent-hash`：FNV-1 哈希环 + 每实例 32 个虚拟节点（权重映射为倍数）；
  同一 `HashKey` 在同一成员集合下恒定路由；摘除非属主节点不引起重映射。

无健康实例时统一返回 `lb.ErrNoHealthyInstance`。

## 降级与重试语义

`forward.Forwarder.Do` 的确定性规则：

1. **策略快照固定**：进入请求时从规则表取不可变 `*VersionedRules`，
   并从注册表取一次实例快照；热更新或上下线都不会中途改变本次请求的策略。
2. **发前复核（两窗口防护）**：LB 选实例后、真正拨号前再次读取注册中心状态：
   - 已注销 → 记录 `skipped(instance-deregistered-in-flight)`，换下一个节点，
     该跳**不消耗重试次数、没有任何副作用**；
   - `probing/unhealthy` → 同样跳过（`instance-not-routable(...)`）。
   另外真实拨号若在实例刚下线后发生，会得到连接拒绝，按下面第 4 条安全重试。
3. **副作用预算**：真正发出的请求次数（`usedAttempts`）绝不超过
   `MaxAttempts`；“选中后跳过”不计入该预算，但有选择轮次上限防止抖动死循环。
4. **错误分类与重试策略**：
   - `ErrConnectionRefused`（连接失败/拨号失败）：请求确定未到达后端，
     **无副作用**，允许换**其它**节点重试（默认不重试同一节点，除非规则
     `RetrySameNode=true`），并立即把失败实例反馈给健康检查器摘除。
   - `ErrUpstreamTimeout`：结果未知、可能已产生副作用，**不重试**，直接收尾。
   - 其它业务错误：视为后端已参与处理，**不重试**。
5. **降级（fail-open）**：无可用节点 / 全部尝试失败 / 预算耗尽时，
   - 规则 `FailOpen=true` → 返回 `OutcomeFailOpen` 与占位响应
     `Degraded{Service, Reason}`，错误仍记录在 `Result.Err`；
   - 否则返回包装 `ErrUnavailable` 的错误，`Outcome` 精确为
     `no-backend / conn-refused / timeout` 之一。
6. **可解释结论**：`Result` 含 `Outcome`、规则版本、每跳 `Attempt`
   （实例、状态、是否重试、是否跳过及原因、耗时、错误），`Explain()` 直接可读。

> 重试从不把失败实例重新放回健康集合：连接失败只触发摘除/进 probing，
> 恢复完全由健康探测的连续成功阈值驱动。

## 规则热更新一致性

* 每条规则包含 `Service / Strategy / Timeout / MaxAttempts /
  RetrySameNode / FailOpen / Weights`。
* `Manager.Update` 对整批规则做**全量校验**：空列表、空服务名、重复服务、
  未知策略、负超时/负权重等任意一条非法 → 整体拒绝，
  **当前版本指针不变，绝不部分生效**。
* 校验通过后构造新的不可变 `VersionedRules`（版本 +1）一次性替换；
  在途请求持有的旧指针继续指导其完整生命周期（已由 1600 个并发请求 ×
  交错热更新的测试验证）。
* 未配置规则的服务使用内置兜底规则（round-robin、500ms 超时、不重试）。

## 验证方法

* `discovery/registry_test.go`：重复注册/墓碑拒绝原因区分；并发注销下快照一致。
* `health/checker_test.go`：摘除/恢复阈值、探测抖动不回路由、探测与注销并发时
  晚到成功不复活、传输失败摘除。
* `lb/balancer_test.go`：稳定轮转顺序、加权比 3:1、一致哈希粘性与最小迁移。
* `route/rule_test.go`：非法更新回滚保留旧版；热更新期间在途请求策略恒定。
* `forward/forwarder_test.go`：无后端/fail-open、连接失败换节点重试且只发送 2 次、
  超时不重试、全失败降级、100 轮并发注销零误路由、重试预算上限。
* `sdlb/integration_test.go`：**真实 TCP 后端**的四个端到端场景——
  注册探活与轮转分布、假死摘除 + 进程下线失败转移、探测抖动 × 12 并发调用者、
  一致哈希粘性 + 热更新期间多版本在途并存。测试日志用 `t.Logf` 打印实例状态
  （`sdlb.ExplainStatus`）、健康判定（`Decision.String()`）与逐跳路由依据
  （`Result.Explain()`）。

```bash
go test ./... -race -v        # 查看每个用例打印的状态与判定依据
go run ./cmd/demo             # 观察摘除、重试、降级、热更新回滚的完整输出
```

## 设计取舍说明

* 单进程内用注册表 + 真实 TCP 环回后端模拟多节点，重点验证调度结论的一致性，
  不依赖外部网络或容器。
* 注册表返回克隆/快照而非内部指针，配合不可变规则版本，避免在途请求读到半状态。
* 负载均衡器按 `service@strategy` 缓存，保证轮转/加权计数器跨请求连续，
  同时不同策略互不干扰。
