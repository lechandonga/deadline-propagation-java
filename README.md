# deadline-propagation-java

为 Web 服务提供端到端的**请求时间预算**与**超时/取消传播**能力，使同一请求在
同步调用、异步任务与流式输出等不同执行路径下的超时与取消行为一致、确定且可解释。

## 预算模型

- 每个入口请求由 `BudgetInterceptor` 安装一个 `BudgetContext`，包含：
  - `Deadline`：基于 `System.nanoTime()` 的单调截止时间，**预算只减不增**，
    任何阶段超时后都不会重新获得完整预算；
  - `CancellationToken`：一次性、不可逆的取消标记，携带唯一终止原因；
  - `BudgetLedger`：预算流水，记录各阶段消耗与终止结论。
- 预算跨方法、跨线程、跨嵌套并发调用逐段扣减：所有异步工作统一经过
  `ContextPropagatingExecutor`，任务提交时捕获上下文、执行时安装、结束后恢复。
- 每个阶段通过 `BudgetContext.checkpoint(stage)` 检查预算：
  剩余预算耗尽时抛出 `BudgetException.DeadlineExceeded` 并触发取消，
  **后续工作被拒绝执行而不是继续排队**（拒绝以可区分异常显式暴露，绝不静默失败）。

## 取消语义

- **超时**：截止时间到达 → `TerminationReason.DEADLINE_EXCEEDED`（HTTP 504）。
- **调用方主动取消**：`CancellationToken.cancel(...)` → 已注册回调同步执行，
  执行中的任务线程被中断（`CANCELLED_BY_CALLER`，HTTP 499）。
- **客户端断连**：流式写出失败 → `CLIENT_DISCONNECTED`，上下文被取消。
- **内部错误**：与上述原因可区分（`INTERNAL_ERROR`）。
- 首个取消原因生效，后续取消不改变结论；被取消的工作不得继续占用线程、连接或锁。
- 对无法中断或已经完成的工作，最终状态由 `BudgetLedger.TerminationEntry`
  （`reason` + `workStopped`）确定记录。

## 并发扇出（部分完成）

`FanOut.invokeAll` 在共享请求预算内执行子任务：

- 整体等待时间不超过请求剩余预算；超时即 `cancel(true)` 未完成的子任务；
- 整体结论唯一：`ALL_SUCCEEDED / PARTIAL / ALL_FAILED / TIMED_OUT / CANCELLED`；
- `completedIndexes` 界定已确认完成（副作用已发生）的工作；
- `retryableIndexes(total)` 只包含未完成部分——**重试不会重复已确认完成的工作**。

## 请求间隔离

- 上下文通过 `ThreadLocal` + 显式 `Scope`（try-with-resources）管理，
  close 时恢复前序上下文；线程池任务结束后清理 ThreadLocal、MDC 与中断标记；
- 线程池复用、嵌套异步与并发调用下，前序请求的残留状态不影响后续请求
  （见 `ContextIsolationTest`）。

## 流式输出的确定性收尾

响应已开始输出后超时：写出明确的终止标记行 `TERMINATED:<reason>` 后结束流，
不截断后伪装成功；正常完成以 `DONE` 结尾。

## 可观测性

请求结束时输出预算流水日志（不含敏感请求内容，仅阶段名/耗时/原因）：

```
budget-ledger requestId=... stages=[StageEntry[stage=sync-work, consumedMillis=64, remainingMillis=135], ...] termination=TerminationEntry[reason=DEADLINE_EXCEEDED, workStopped=true]
```

日志含 `requestId`（MDC），可串联同一请求的全部阶段。

## 配置项

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `budget.default-ms` | `2000` | 每个入口请求的默认整体预算（毫秒） |
| `budget.max-ms` | `10000` | 调用方可指定的预算上限 |

请求头：

| 请求头 | 说明 |
| --- | --- |
| `X-Budget-Millis` | 调用方缩短本次预算（只能缩短，超过 `budget.max-ms` 会被截断） |
| `X-Request-Id` | 请求标识；缺省自动生成 UUID |

## 演示端点

| 端点 | 路径 | 说明 |
| --- | --- | --- |
| 同步 | `GET /api/sync?workMs=` | 分片执行，每片前检查预算 |
| 异步 | `GET /api/async?workMs=` | `WebAsyncTask` 共享入口预算，超时回调触发取消 |
| 扇出 | `GET /api/fanout?workMs=&failIndex=` | 3 个并发子任务，演示部分失败结论 |
| 流式 | `GET /api/stream?chunks=&chunkMs=` | 逐块输出，超时后写终止标记 |

## 测试

```
mvn test
```

- `DeadlineTest`：预算只减不增、耗尽后不为负；
- `CancellationTest`：取消传播到执行中任务、线程被释放、预算耗尽后拒绝排队；
- `ContextIsolationTest`：线程池复用下上下文/MDC/中断标记不串扰；
- `FanOutTest`：部分完成结论唯一、重试安全、超时取消剩余任务；
- `WebBudgetTest`：端到端超时（504）、异步超时确定性、流式终止标记、
  前一请求超时不影响后续请求。
