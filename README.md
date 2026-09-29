# deadline-propagation-java

端到端的请求时间预算与超时取消传播：同一请求在同步调用、异步任务与流式输出
三条执行路径下，超时与取消行为一致、确定且可解释。

在超时能及时停下的基础上，本层进一步做到**超预算不轻易整体失败**：仅重试
未确认完成的工作（已落地的副作用绝不重跑），重试花的还是原来那笔预算；
救不回来时返回三态可区分的降级结果，而不是把整个请求报失败。

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

## 重试与降级

- **只重试未确认完成的工作**：`RetryingFanOut` 在每轮结束后把工作分为
  已确认完成（副作用已落地，**绝不再次调用**）与未确认完成（仅 `TIMEOUT`
  或内部 `FAILED` 允许重试）。取消、并发上限被拒的工作不参与重试。
- **有界重试，且只花原预算**：每个工作的总尝试次数受
  `max-attempts` 限制，两次尝试之间按 `retry-interval` 等待；等待可被
  到期/取消立即打断。重试锚定同一个绝对截止时间，不会延长整体时间线，
  超时之后也不会冒出新预算。
- **剩余时间不够就不试**：发起下一次尝试前检查
  `min-remaining-budget`；剩余时间连一次启动成本都覆盖不了时直接放弃，
  降级原因为 `INSUFFICIENT_REMAINING`，绝不硬试。预算已终止
  （到期、调用方取消、连接已断）之后不再发起任何重试。
- **三态降级结果**：`DegradedResult` 明确区分
  `ALL_SUCCESS`（全部成功）、`PARTIAL_SUCCESS`（部分成功，降级返回）、
  `TOTAL_FAILURE`（彻底失败）。结果包含 `completed`（再重试不安全的已确认
  工作）、`incomplete`（未完成工作及其最后状态、终止原因、尝试次数）以及
  `stopReason`：
  - `BUDGET_EXHAUSTED`：预算用完（到达截止时间）；
  - `INSUFFICIENT_REMAINING`：剩余时间低于重试阈值，主动放弃重试；
  - `CANCELLED`：调用方取消/客户端断开；
  - `RETRIES_EXHAUSTED`：内部错误反复出现，次数用尽；
  - `CONCURRENCY_LIMITED`：达到入口并发上限，直接降级；
  - `INTERNAL_ERROR` / `NONE`。
- **并发上限保护下游**：每个入口可配 `max-concurrency`，用公平信号量限制
  真正在跑、在重试的工作数；到上限**不排队硬塞**，立即拒绝并以
  `CONCURRENCY_LIMITED` 降级。
- **结论确定性**：降级结果只由声明的输入及声明顺序决定，与子任务谁先跑完
  无关；相同输入必然得到相同的 `status / completed / incomplete /
  stopReason`。

## 流式的确定收尾

`BudgetedStreamEmitter` 保证：终止后不再写出任何字节，已经发出的字节不
重复、也不会凭空多出内容；收尾时恰好写出一行固定终止标记
`X-STREAM-TERMINATED:<原因>`（如 `DEADLINE_EXCEEDED`），它是流的最后内容。
客户端断开（写出抛 `IOException`）时记为 `CLIENT_CANCELLED`，不伪造字节。

## 可观测性

`BudgetLedger` 记录每个阶段消耗的预算（阶段名 + 毫秒数）、终止原因以及
工作是否真正停止（`workStopped`），并输出单行摘要日志：

```
budget stage=fanout consumedMs=213
budget terminated reason=DEADLINE_EXCEEDED workStopped=true
request finished budget-ledger[fanout=213ms terminated=DEADLINE_EXCEEDED workStopped=true]
```

台账只记录阶段名与时长，**不包含请求体或任何敏感请求内容**。

## 配置项

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `request.budget.default-budget` | `5s` | 每个入口请求获得整体时间预算 |
| `request.budget.termination-grace` | `500ms` | 终止后等待在途工作停止的宽限期（预留给关闭策略） |
| `request.budget.entries.<name>.path` | 无 | 该入口匹配的请求路径，如 `/api/degrade` |
| `request.budget.entries.<name>.budget` | 等于总预算 | 该入口的总时间预算（必须为正） |
| `request.budget.entries.<name>.max-attempts` | `1` | 每个工作的总尝试次数（含首次）；`1` 表示不重试 |
| `request.budget.entries.<name>.retry-interval` | `0` | 两次尝试之间的间隔；启用重试（`max-attempts>1`）时必填 |
| `request.budget.entries.<name>.min-remaining-budget` | `0` | 剩余时间低于该值则不再重试；可大于预算以彻底禁用重试 |
| `request.budget.entries.<name>.max-concurrency` | 无上限 | 同时运行/重试的工作数上限；到顶即降级不排队 |

**兼容范围**：未在 `entries` 中配置的路径行为与之前完全一致——使用
`default-budget`、不重试、无并发上限。**配置写错会在启动时直接报错**
（非法时长、`max-attempts<1`、启用重试却缺 `retry-interval`、
`max-concurrency<1`、重复/缺失 `path` 等），不会悄悄退回默认值。单个请求
在进入时解析出一份不可变的 `RequestPlan` 快照，运行期间的配置改动不会
影响在途请求。

## 代码结构

| 类 | 职责 |
| --- | --- |
| `budget/RequestBudget` | 预算与截止时间、终止原因、终止回调、主动截止执行 |
| `budget/BudgetContext` | 线程上下文传播与作用域还原（请求间隔离） |
| `exec/BudgetAwareExecutor` | 跨线程传播、拒绝排队、取消中断在途任务、在途计数 |
| `exec/FanOut` | 部分成功/超时/失败的确定性聚合与重试范围界定 |
| `retry/RetryingFanOut` | 有界、只花原预算的自动重试与三态降级聚合 |
| `retry/RetryPolicy` | 次数上限、重试间隔、剩余时间阈值 |
| `retry/ConcurrencyLimiter` | 每入口在跑/重试工作数上限（到顶即降级） |
| `degrade/DegradedResult` | 三态降级结果、已确认/未完成清单与停止原因 |
| `stream/BudgetedStreamEmitter` | 流式字节不重复不多写、唯一终止标记 |
| `web/BudgetWebFilter` | 每请求预算分配、异步收尾、台账终结 |
| `web/EntryPolicyResolver` | 按入口解析/校验配置并生成每请求不可变快照 |
| `observability/BudgetLedger` | 预算流水与终止依据记录（不含敏感内容） |

## 演示端点

- `GET /api/sync?workMs=50` — 同步分阶段工作，阶段间检查预算。
- `GET /api/fanout?workMs=50` — 三个并发子任务（快/慢/失败），返回确定性的
  `outcomes`、`completed` 与 `retryable`。
- `GET /api/stream` — 流式输出，预算耗尽或客户端断开后确定性停止。
- `GET /api/degrade?succeedOnAttempt=3` — 有界重试 + 降级：`flaky` 在第 3
  次尝试成功（被救回），`always-failing` 用尽次数后以 `PARTIAL_SUCCESS`
  降级返回；`succeedOnAttempt=1` 时为 `ALL_SUCCESS`。
- `GET /api/deadline?failFirst=true` — 首次快速失败后，剩余时间低于
  `min-remaining-budget`，不再重试，返回 `PARTIAL_SUCCESS` 且
  `stopReason=INSUFFICIENT_REMAINING`（`attempts=1`）。

降级响应示例：

```json
{
  "status": "PARTIAL_SUCCESS",
  "stopReason": "RETRIES_EXHAUSTED",
  "completed": ["confirmed", "flaky"],
  "incomplete": [
    {"name": "always-failing", "status": "FAILED", "reason": "ERROR", "attempts": 4}
  ],
  "remainingMs": 1820
}
```

## 测试

测试以超时、取消与并发为主：

- `RequestBudgetTest` — 预算只减不增、到期/取消原因可区分、终止回调恰好一次。
- `BudgetAwareExecutorTest` — 耗尽后拒绝排队、取消后无残留任务与中断确认、
  线程池复用下上下文不串扰、嵌套阶段共享同一收缩预算。
- `FanOutTest` — 混合结局的唯一结论、重试范围不含已确认完成的工作。
- `BudgetWebFilterTest` — 每请求独立预算、线程还原、失败路径记为 `ERROR`。
- `BudgetFlowIntegrationTest` — 端到端：超预算同步请求被拒绝、扇出部分完成、
  流式在截止时间确定停止、并发请求预算相互隔离。
- `RetryingFanOutTest` — 重试救回/次数上限/已确认不重跑、剩余不足放弃、
  到期或取消后不再重试、并发上限即降级、相同输入降级结论唯一。
- `BudgetedStreamEmitterTest` — 终止标记恰好一次、之后不多写不重复、客户端
  断开不伪造字节。
- `EntryPolicyResolverTest` — 按入口隔离、未配置入口保持旧约定、非法配置
  明确报错。
- `RetryDegradeIntegrationTest` — 端到端：部分成功降级、预算不够放弃重试、
  流式唯一收尾标记、未配置入口默认兼容。

运行：`mvn test`

测试全部基于即时执行器与可控时钟，不依赖外部服务或真实网络，也不靠长
时间睡眠凑时间，本地与 CI 上都能稳定、快速跑完。

## 本地手动验证

启动应用（`mvn spring-boot:run`）后：

```bash
# 复现“部分成功降级”：flaky 重试到第 3 次成功，always-failing 用尽 4 次
curl -s 'http://localhost:8080/api/degrade?succeedOnAttempt=3'
# => "status":"PARTIAL_SUCCESS", "stopReason":"RETRIES_EXHAUSTED",
#    "completed":["confirmed","flaky"], incomplete 中 attempts=4

# 复现“预算不够放弃重试”：300ms 预算、阈值 400ms，首次失败后绝不重试
curl -s 'http://localhost:8080/api/deadline?failFirst=true'
# => "status":"PARTIAL_SUCCESS", "stopReason":"INSUFFICIENT_REMAINING",
#    incomplete 中 heavy 的 attempts=1

# 流式唯一终止标记
curl -s 'http://localhost:8080/api/stream' | tail -n 1
# => X-STREAM-TERMINATED:DEADLINE_EXCEEDED
```
