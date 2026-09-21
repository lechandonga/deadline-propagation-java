package com.github.lechandonga.deadlinepropagationjava.budget;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 取消标记：携带终止原因，支持注册取消回调。
 *
 * <p>取消是一次性、不可逆的状态迁移；回调在取消发生的线程上同步执行，
 * 用于及时释放线程、连接与锁。终止原因在首个取消者处确定，后续取消不改变原因。
 */
public final class CancellationToken {

    private final AtomicReference<TerminationReason> reason =
            new AtomicReference<>(TerminationReason.NONE);
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    public boolean isCancelled() {
        return reason.get() != TerminationReason.NONE;
    }

    /** 终止原因；未取消时为 {@link TerminationReason#NONE}。 */
    public TerminationReason reason() {
        return reason.get();
    }

    /**
     * 触发取消。仅首个非 NONE 原因生效，保证终止原因唯一确定。
     * 已注册的回调按注册顺序同步执行；单个回调异常不影响其余回调。
     */
    public void cancel(TerminationReason r) {
        if (r == null || r == TerminationReason.NONE) {
            throw new IllegalArgumentException("cancel reason must be a real termination reason");
        }
        if (reason.compareAndSet(TerminationReason.NONE, r)) {
            for (Runnable listener : listeners) {
                try {
                    listener.run();
                } catch (RuntimeException ignored) {
                    // 回调失败不阻断取消传播
                }
            }
        }
    }

    /**
     * 注册取消回调；若已取消则立即执行并返回空句柄。
     * 返回的句柄 close 后注销回调（用于避免资源泄漏）。
     */
    public AutoCloseable onCancel(Runnable listener) {
        if (isCancelled()) {
            listener.run();
            return () -> {};
        }
        listeners.add(listener);
        // 竞态：注册与取消同时发生时补执行一次
        if (isCancelled() && listeners.remove(listener)) {
            listener.run();
            return () -> {};
        }
        return () -> listeners.remove(listener);
    }

    /** 已取消时抛出携带对应原因的异常，用于协作式取消检查点。 */
    public void throwIfTerminated() {
        TerminationReason r = reason.get();
        switch (r) {
            case NONE -> {}
            case DEADLINE_EXCEEDED ->
                    throw new BudgetException.DeadlineExceeded("deadline exceeded");
            case REJECTED_BUDGET_EXHAUSTED ->
                    throw new BudgetException.Rejected("budget exhausted, work rejected");
            default ->
                    throw new BudgetException.Cancelled("cancelled: " + r);
        }
    }
}
