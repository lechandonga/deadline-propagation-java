# deadline-propagation-java

端到端的请求时间预算与超时取消传播：同一请求在同步调用、异步任务与流式输出
三条执行路径下，超时与取消行为一致、确定且可解释。

## 预算模型

- **每请求一个预算**：`BudgetWebFilter` 为每个进入 `/api/*` 的请求创建
  `RequestBudget`（默认 5s，见配置项），并以请求属性与 `BudgetContext`
  线程上下文两种形式携带。
- **预算只减不增**：预算锚定在绝对截止时间（`deadline = 创建时刻 + 总额度`）。
  跨方法、跨线程、嵌套并发调用共享同一个截止时间；任何阶段超时后都不会
  重新获得完整预算。`remaining()` 单调不增，耗尽后恒为零。
- **主动截止执行**：共享的 `budget-deadline-enforcer` 调度线程在截止时间
  主动触发 `DEADLINE_EXCEEDED`，正在睡眠或阻塞的工作也会被中断，而不是
  等到下一次 `checkAlive()` 才被发现。
- **阶段边界检查**：各阶段入口调用 `budget.checkAlive()`；预算耗尽抛
  `BudgetExhaustedException`，被取消抛 `RequestCancelledException`，
  二者类型可区分。剩余预算耗尽时新工作被**拒绝**（立即抛异常），
  不会继续排队。

## 取消语义

- **终止原因唯一且首因生效**：`TerminationReason` 区分
  `DEADLINE_EXCEEDED`（到达截止时间）、`CLIENT_CANCELLED`（调用方断开）、
  `CANCELLED`（应用主动取消）、`REJECTED_BUDGET_EXHAUSTED`（被拒绝未执行）、
  `ERROR`（内部错误）。第一个终止原因生效，后续覆盖被忽略，结论确定。
- **取消传播到下游**：`BudgetAwareExecutor` 为每个提交的任务注册终止回调；
  预算终止时以中断方式取消在途任务（`Future.cancel(true)`），被取消的工作
  不再占用线程、连接或锁。无法中断或已完成的工作，其 `CompletableFuture`
  仍以真实原因异常完成，最终状态确定且可区分。
- **部分完成的确定性结论**：`FanOut.gather` 将并发子任务分类为
  `SUCCESS / TIMEOUT / FAILED / CANCELLED`，聚合成唯一结论：
  - `completed()`：已确认完成的工作（副作用已发生），**重试必须排除**；
  - `retryable()`：未确认完成的工作，只有这些允许重试。
- **流式输出的确定收尾**：`/api/stream` 在预算耗尽或客户端断开
  （`IOException` → `CLIENT_CANCELLED`）后立即停止写出，不再产生任何字节，
  并在台账中记录真实终止原因。
- **请求间隔离**：`BudgetContext` 采用作用域（try-with-resources）进出，
  关闭时精确还原线程先前绑定；线程池复用、嵌套异步下前序请求的预算、
  截止时间与取消标记不会泄漏到后续请求。

## 可观测性

`BudgetLedger` 记录每个阶段消耗的预算（阶段名 + 毫秒数）、终止原因以及
工作是否真正停止（`workStopped`），并输出单行摘要日志：

```
budget stage=fanout consumedMs=213
budget terminated reason=DEADLINE_EXCEEDED workStopped=true
request finished budget-ledger[fanout=213ms terminated=DEADLINE_EXCEEDED workStopped=true]
```

台账只记录阶段名与时长，**不包含请求体或任何敏感请求内容**。

## 预算内重试

重试能力（`retry` 包，入口 `RetryingFanOut`）建立在同一个收缩预算之上，
**永远不会因为重试拉长整体时间**：

- **只重试没确认完成的工作**：一次尝试成功返回才算“确认完成”，其副作用已落地，
  这类条目被记入 `completed`，无论后续发生什么都不会再执行第二遍；只有
  `unfinished`（超时 / 失败 / 取消 / 被拒）的条目允许重试。
- **次数上限 + 间隔**：每个条目的尝试总次数（含首次）受
  `max-attempts` 限制，两次尝试之间至少等待 `backoff`；退避等待本身也挂在
  预算上，预算一终止（到期或取消）等待立即结束，**超时之后不会冒出新预算**。
- **启动成本阈值**：每次尝试前检查 `remaining`，剩余时间低于
  `retry-threshold` 时直接放弃重试（`REMAINING_BELOW_THRESHOLD`），
  不启动注定失败的尝试。
- **取消 / 断连后零重试**：预算终止原因为 `CLIENT_CANCELLED` / `CANCELLED`
  后，无论是在尝试中还是退避等待中，都不会再发起任何尝试。
- **默认关闭，完全兼容**：默认 `max-attempts=1`，未配置的入口行为与以前
  完全一致（例如 `/api/sync` 超预算仍然是 500）。

## 降级结果

重试也救不回来时，请求**不再整体失败**，而是返回一份三态可区分的
`DegradedResult`：

| state | 含义 |
| --- | --- |
| `ALL_SUCCESS` | 全部条目确认完成，`stopReason=NONE` |
| `PARTIAL_SUCCESS` | 有确认完成的条目、也有未完成条目（降级返回，HTTP 仍为 200） |
| `FAILED` | 没有任何条目确认完成 |

结果中 `completed` 列出**已确认完成、绝不能再重试**的条目，`unfinished`
列出未确认完成的条目；`items` 给出每条的最终状态（`SUCCESS / TIMEOUT /
FAILED / CANCELLED / REJECTED`）、实际尝试次数与停止原因。聚合停止原因
`stopReason` 由条目集合按固定优先级得出（取消 > 预算耗尽 > 低于阈值 >
并发受限 > 次数用尽 > 内部错误），**与谁先跑完无关**，同样的输入必然得到
同样的降级结论。停止原因区分 `BUDGET_EXHAUSTED`、`CLIENT_CANCELLED` /
`CANCELLED`、`INTERNAL_ERROR` 等。

**流式路径**（`/api/stream-retry`）语义一致：一个 tick 在字节真正写出
（确认点）之前失败可以重试，已写出的字节**绝不重复、也不会凭空多出**；
整个流以且仅以一个终止标记收尾：`END:COMPLETE`（全部完成）、
`END:BUDGET_EXHAUSTED`、`END:CLIENT_CANCELLED` 等，终止后不再写任何字节。

## 下游保护：入口并发上限

每个入口可配置同时“真正在跑 / 在重试”的尝试数上限 `max-concurrency`
（`0` 或负数表示不限）。许可只在尝试执行期间持有，退避等待期间释放。
到达上限时**不排队硬塞**：条目不执行、`attempts=0`、状态 `REJECTED`、
停止原因 `CONCURRENCY_LIMITED`，直接进入降级结果。

## 配置项

全局默认（未配置的入口沿用，默认行为与旧版本一致）：

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `request.budget.default-budget` | `5s` | 每个入口请求获得整体时间预算 |
| `request.budget.termination-grace` | `500ms` | 终止后等待在途工作停止的宽限期（预留给关闭策略） |
| `request.budget.default-max-attempts` | `1` | 每个条目的尝试总次数（含首次）；`1` 表示不重试 |
| `request.budget.default-retry-backoff` | `100ms` | 两次尝试之间的最小间隔 |
| `request.budget.default-retry-threshold` | `50ms` | 剩余预算低于该值则不再发起尝试 |
| `request.budget.max-concurrency` | `0`（不限） | 默认的入口在跑尝试数上限 |

按入口覆盖（key 为请求 URI 路径，任何一项不写就继承默认值）：

| 配置 | 说明 |
| --- | --- |
| `request.budget.entrypoints[/path].budget` | 该入口总预算 |
| `request.budget.entrypoints[/path].max-attempts` | 该入口尝试次数上限 |
| `request.budget.entrypoints[/path].backoff` | 该入口重试间隔 |
| `request.budget.entrypoints[/path].retry-threshold` | 该入口的启动成本阈值 |
| `request.budget.entrypoints[/path].max-concurrency` | 该入口并发在跑尝试上限 |

配置在应用启动时（`@PostConstruct`）强校验：预算必须为正、次数 ≥ 1、
间隔不能为负且必须小于预算等；写错会**直接启动失败并给出明确报错**，
不会悄悄退回默认值。单个请求在进入时对配置拍一次不可变快照
（`EntrypointSettings`），运行中改动配置不会影响在途请求。

## 代码结构

| 类 | 职责 |
| --- | --- |
| `budget/RequestBudget` | 预算与截止时间、终止原因、终止回调、主动截止执行 |
| `budget/BudgetContext` | 线程上下文传播与作用域还原（请求间隔离） |
| `exec/BudgetAwareExecutor` | 跨线程传播、拒绝排队、取消中断在途任务、在途计数 |
| `exec/FanOut` | 部分成功/超时/失败的确定性聚合与重试范围界定 |
| `retry/RetryingFanOut` | 预算内自动重试（次数/间隔/阈值）、取消即停、降级聚合 |
| `retry/DegradedResult` / `ItemResult` / `StopReason` | 三态降级结果与确定性停止原因 |
| `retry/RetryPolicy` / `EntrypointSettings` / `EntrypointResolver` | 按入口的不可变配置快照解析 |
| `retry/RetryScheduler` | 预算可中断的退避等待（到期/取消立即放行，不再重试） |
| `retry/ConcurrencyLimiter` | 入口级并发上限：到顶即降级、绝不排队 |
| `web/BudgetWebFilter` | 每请求预算分配、异步收尾、台账终结 |
| `observability/BudgetLedger` | 预算流水与终止依据记录（不含敏感内容） |

## 演示端点

- `GET /api/sync?workMs=50` — 同步分阶段工作，阶段间检查预算。
- `GET /api/fanout?workMs=50` — 三个并发子任务（快/慢/失败），返回确定性的
  `outcomes`、`completed` 与 `retryable`。
- `GET /api/stream` — 流式输出，预算耗尽或客户端断开后确定性停止。
- `GET /api/retry/fanout?workMs=80` — 预算内重试 + 降级：`fast` 一次成功，
  `flaky` 首次失败、重试成功，`slow` 在 400ms 预算内无法完成；返回三态
  `state`、`completed` / `unfinished`、每条 `attempts` 与 `stopReason`。
  该入口配置了 `max-concurrency=2`，并发受限时 `slow` 也可能直接
  `REJECTED / CONCURRENCY_LIMITED`。
- `GET /api/stream-retry?flakyTick=1&workMs=40` — 流式重试：指定 tick 首次
  失败、在确认写出前重试，每个 tick 恰好出现一次，末尾恰好一个
  `END:COMPLETE` 标记。

## 测试

测试以超时、取消与并发为主：

- `RequestBudgetTest` — 预算只减不增、到期/取消原因可区分、终止回调恰好一次。
- `BudgetAwareExecutorTest` — 耗尽后拒绝排队、取消后无残留任务与中断确认、
  线程池复用下上下文不串扰、嵌套阶段共享同一收缩预算。
- `FanOutTest` — 混合结局的唯一结论、重试范围不含已确认完成的工作。
- `BudgetWebFilterTest` — 每请求独立预算、线程还原、失败路径记为 `ERROR`。
- `BudgetFlowIntegrationTest` — 端到端：超预算同步请求被拒绝、扇出部分完成、
  流式在截止时间确定停止、并发请求预算相互隔离。
- `RetryingFanOutTest` — 重试成功且已完成工作不重跑、次数上限、取消后零重试、
  到期停止、剩余时间低于阈值不启动、并发上限即降级、同输入同结论。
- `DegradedResultTest` — 三态可区分、完成/未完成互斥、停止原因聚合与顺序无关。
- `EntrypointResolverTest` — 按入口隔离、部分覆盖继承默认、快照不受运行中
  改动影响、非法配置明确报错。
- `RetryFlowIntegrationTest` / `RetryBudgetFlowIntegrationTest` — 端到端：
  部分成功降级返回（预算耗尽路径与并发受限路径）、流式字节不重复不多写且
  只有一个终止标记、未配置入口保持旧的失败行为。

测试不依赖外部服务或真实网络，超时用例使用虚拟时钟或极短预算，可稳定快速跑完。

运行：`mvn test`

### 本地手工验证

启动应用（`mvn spring-boot:run`）后：

**复现“部分成功降级”**（重试成功 + 预算耗尽，绕开并发上限）：

```bash
# 临时把该入口并发放开，或直接观察默认配置下 slow 的降级原因
curl -s "http://localhost:8080/api/retry/fanout?workMs=80"
# "state":"PARTIAL_SUCCESS"，completed 含 fast/flaky，unfinished=["slow"]，
# stopReason 为 BUDGET_EXHAUSTED 或 CONCURRENCY_LIMITED，HTTP 200
```

**复现“预算不够、放弃重试”**：剩余时间低于 `retry-threshold` 时，条目不启动
新尝试，直接以 `REMAINING_BELOW_THRESHOLD` 降级；流式路径则在做不完下一个
tick 时收尾：

```bash
curl -s "http://localhost:8080/api/stream-retry?flakyTick=0&workMs=200"
# 每个已发 tick 只出现一次，末尾恰好一行 END:BUDGET_EXHAUSTED
```
