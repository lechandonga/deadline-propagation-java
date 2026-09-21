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

## 配置项

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `request.budget.default-budget` | `5s` | 每个入口请求获得整体时间预算 |
| `request.budget.termination-grace` | `500ms` | 终止后等待在途工作停止的宽限期（预留给关闭策略） |

## 代码结构

| 类 | 职责 |
| --- | --- |
| `budget/RequestBudget` | 预算与截止时间、终止原因、终止回调、主动截止执行 |
| `budget/BudgetContext` | 线程上下文传播与作用域还原（请求间隔离） |
| `exec/BudgetAwareExecutor` | 跨线程传播、拒绝排队、取消中断在途任务、在途计数 |
| `exec/FanOut` | 部分成功/超时/失败的确定性聚合与重试范围界定 |
| `web/BudgetWebFilter` | 每请求预算分配、异步收尾、台账终结 |
| `observability/BudgetLedger` | 预算流水与终止依据记录（不含敏感内容） |

## 演示端点

- `GET /api/sync?workMs=50` — 同步分阶段工作，阶段间检查预算。
- `GET /api/fanout?workMs=50` — 三个并发子任务（快/慢/失败），返回确定性的
  `outcomes`、`completed` 与 `retryable`。
- `GET /api/stream` — 流式输出，预算耗尽或客户端断开后确定性停止。

## 测试

测试以超时、取消与并发为主：

- `RequestBudgetTest` — 预算只减不增、到期/取消原因可区分、终止回调恰好一次。
- `BudgetAwareExecutorTest` — 耗尽后拒绝排队、取消后无残留任务与中断确认、
  线程池复用下上下文不串扰、嵌套阶段共享同一收缩预算。
- `FanOutTest` — 混合结局的唯一结论、重试范围不含已确认完成的工作。
- `BudgetWebFilterTest` — 每请求独立预算、线程还原、失败路径记为 `ERROR`。
- `BudgetFlowIntegrationTest` — 端到端：超预算同步请求被拒绝、扇出部分完成、
  流式在截止时间确定停止、并发请求预算相互隔离。

运行：`mvn test`
