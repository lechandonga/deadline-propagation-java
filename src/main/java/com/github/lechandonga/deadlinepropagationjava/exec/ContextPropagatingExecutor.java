package com.github.lechandonga.deadlinepropagationjava.exec;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetException;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * 上下文传播执行器。
 *
 * <p>提交任务时捕获当前 {@link BudgetContext}；任务在线程池中执行时：
 * <ul>
 *   <li>安装被捕获的上下文与 MDC(requestId)，结束后恢复，保证请求间隔离；</li>
 *   <li>注册取消回调：上下文被取消时中断执行线程，传播到下游工作；</li>
 *   <li>执行前检查预算：已终止的上下文不再执行，拒绝发生在任务包装内部，
 *       因此 {@link Future} 会以可区分的异常完成，绝不静默丢弃；</li>
 *   <li>任务结束后清理中断标记与 ThreadLocal，线程可安全复用。</li>
 * </ul>
 */
public final class ContextPropagatingExecutor extends AbstractExecutorService {

    private static final Logger log = LoggerFactory.getLogger(ContextPropagatingExecutor.class);

    private final ExecutorService delegate;

    public ContextPropagatingExecutor(ExecutorService delegate) {
        this.delegate = delegate;
    }

    public static ContextPropagatingExecutor wrap(ExecutorService delegate) {
        return new ContextPropagatingExecutor(delegate);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        BudgetContext captured = BudgetContext.current();
        delegate.execute(() -> {
            try {
                runWithContext(captured, () -> {
                    command.run();
                    return null;
                });
            } catch (BudgetException e) {
                // fire-and-forget 路径：拒绝必须可见，不能静默失败
                log.warn("task rejected before execution: reason={}", e.reason());
            } catch (Exception e) {
                if (e instanceof RuntimeException re) {
                    throw re;
                }
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        BudgetContext captured = BudgetContext.current();
        return delegate.submit(() -> runWithContext(captured, task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        BudgetContext captured = BudgetContext.current();
        return delegate.submit(() -> runWithContext(captured, () -> {
            task.run();
            return result;
        }));
    }

    @Override
    public Future<?> submit(Runnable task) {
        BudgetContext captured = BudgetContext.current();
        return delegate.submit(() -> runWithContext(captured, () -> {
            task.run();
            return null;
        }));
    }

    private <T> T runWithContext(BudgetContext captured, Callable<T> task) throws Exception {
        if (captured == null) {
            return task.call();
        }
        Thread worker = Thread.currentThread();
        String previousMdc = MDC.get("requestId");
        MDC.put("requestId", captured.requestId());
        // 取消传播：上下文取消时中断执行线程
        AutoCloseable interruptOnCancel = captured.cancellation().onCancel(worker::interrupt);
        try (BudgetContext.Scope ignored = captured.install()) {
            // 剩余预算耗尽时拒绝执行，不继续占用线程
            captured.checkpoint("executor-dispatch");
            return task.call();
        } catch (BudgetException e) {
            captured.terminate(e.reason(), true);
            throw e;
        } finally {
            try {
                interruptOnCancel.close();
            } catch (Exception ignoredClose) {
                // 注销回调失败不影响清理
            }
            // 清理由取消引发的中断标记，避免污染复用后的下一个任务
            if (captured.cancellation().isCancelled()) {
                Thread.interrupted();
            }
            if (previousMdc == null) {
                MDC.remove("requestId");
            } else {
                MDC.put("requestId", previousMdc);
            }
        }
    }
}
