package com.github.lechandonga.deadlinepropagationjava.budget;

/**
 * 请求级预算上下文：截止时间 + 取消标记 + 预算流水。
 *
 * <p>通过 ThreadLocal 在当前线程生效；跨线程时由
 * {@code ContextPropagatingExecutor} 在任务提交时捕获、执行时安装、
 * 结束后恢复，保证线程池复用与嵌套异步场景下请求间严格隔离。
 */
public final class BudgetContext {

    private static final ThreadLocal<BudgetContext> CURRENT = new ThreadLocal<>();

    private final String requestId;
    private final Deadline deadline;
    private final CancellationToken cancellation = new CancellationToken();
    private final BudgetLedger ledger = new BudgetLedger();

    private BudgetContext(String requestId, Deadline deadline) {
        this.requestId = requestId;
        this.deadline = deadline;
    }

    /** 创建请求上下文（不自动安装到当前线程）。 */
    public static BudgetContext start(String requestId, Deadline deadline) {
        return new BudgetContext(requestId, deadline);
    }

    /** 当前线程绑定的上下文；未绑定时返回 null。 */
    public static BudgetContext current() {
        return CURRENT.get();
    }

    /**
     * 将本上下文安装到当前线程，返回作用域句柄。
     * close 时恢复前序上下文（可能为 null），确保不残留、不串扰。
     */
    public Scope install() {
        BudgetContext previous = CURRENT.get();
        CURRENT.set(this);
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        };
    }

    public Deadline deadline() {
        return deadline;
    }

    public CancellationToken cancellation() {
        return cancellation;
    }

    public BudgetLedger ledger() {
        return ledger;
    }

    public String requestId() {
        return requestId;
    }

    /**
     * 阶段检查点：
     * <ol>
     *   <li>已被取消 → 抛出携带原因的异常；</li>
     *   <li>预算耗尽 → 触发取消（原因 DEADLINE_EXCEEDED）、记录终止流水并抛异常，
     *       后续工作不得继续排队；</li>
     *   <li>否则记录本阶段消耗与剩余预算。</li>
     * </ol>
     */
    public void checkpoint(String stage) {
        cancellation.throwIfTerminated();
        if (deadline.isExpired()) {
            cancellation.cancel(TerminationReason.DEADLINE_EXCEEDED);
            ledger.recordTermination(TerminationReason.DEADLINE_EXCEEDED, true);
            throw new BudgetException.DeadlineExceeded(
                    "budget exhausted at stage '" + stage + "'");
        }
        ledger.recordStage(stage, ledger.consumedMillis(), deadline.remainingMillis());
    }

    /** 记录终止结论（仅首个生效）。 */
    public void terminate(TerminationReason reason, boolean workStopped) {
        ledger.recordTermination(reason, workStopped);
    }

    /** 作用域句柄，close 时恢复前序上下文。 */
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
