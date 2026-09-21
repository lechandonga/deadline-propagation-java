package com.github.lechandonga.deadlinepropagationjava.fanout;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 并发扇出：在共享请求预算内执行一组子任务。
 *
 * <p>语义保证：
 * <ul>
 *   <li>整体等待时间不超过请求剩余预算——预算只减不增；</li>
 *   <li>超时/取消时立即 cancel(true) 未完成的子任务，不继续占用线程；</li>
 *   <li>整体结论唯一确定：ALL_SUCCEEDED / PARTIAL / ALL_FAILED / TIMED_OUT / CANCELLED；</li>
 *   <li>completedIndexes 记录已确认完成（副作用已发生）的下标，
 *       retryableIndexes 只包含未完成部分，重试不会重复已确认工作。</li>
 * </ul>
 */
public final class FanOut {

    private FanOut() {}

    public enum Outcome { ALL_SUCCEEDED, PARTIAL, ALL_FAILED, TIMED_OUT, CANCELLED }

    /** 单个失败子任务的确定性记录（仅记录异常类型名，不含可能敏感的异常消息）。 */
    public record Failure(int index, String reasonType, String terminationReason) {}

    /** 整体结论。 */
    public record FanOutResult<T>(List<T> successes, List<Failure> failures,
                                  List<Integer> completedIndexes, Outcome outcome) {
        /** 可安全重试的下标：不包含任何已确认完成的工作。 */
        public List<Integer> retryableIndexes(int totalTasks) {
            List<Integer> retryable = new ArrayList<>();
            for (int i = 0; i < totalTasks; i++) {
                if (!completedIndexes.contains(i)) {
                    retryable.add(i);
                }
            }
            return retryable;
        }
    }

    public static <T> FanOutResult<T> invokeAll(List<? extends Callable<T>> tasks,
                                                ExecutorService executor) {
        BudgetContext ctx = BudgetContext.current();
        if (ctx != null) {
            try {
                ctx.checkpoint("fanout-start");
            } catch (BudgetException be) {
                // 上下文已终止：不提交任何子任务，返回确定性结论而非抛出
                List<Failure> failures = new ArrayList<>();
                for (int i = 0; i < tasks.size(); i++) {
                    failures.add(new Failure(i, be.getClass().getSimpleName(),
                            be.reason().name()));
                }
                ctx.terminate(be.reason(), true);
                Outcome outcome = be.reason() == TerminationReason.DEADLINE_EXCEEDED
                        ? Outcome.TIMED_OUT : Outcome.CANCELLED;
                return new FanOutResult<>(List.of(), failures, List.of(), outcome);
            }
        }

        List<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(executor.submit(task));
        }

        List<T> successes = new ArrayList<>();
        List<Failure> failures = new ArrayList<>();
        List<Integer> completed = new ArrayList<>();
        boolean timedOut = false;

        for (int i = 0; i < futures.size(); i++) {
            Future<T> future = futures.get(i);
            long waitMillis = ctx != null ? ctx.deadline().remainingMillis() : Long.MAX_VALUE;
            if (waitMillis <= 0) {
                timedOut = true;
                cancelRemaining(futures, i);
                for (int j = i; j < futures.size(); j++) {
                    failures.add(new Failure(j, "TimeoutException",
                            TerminationReason.DEADLINE_EXCEEDED.name()));
                }
                break;
            }
            try {
                T result = future.get(waitMillis, TimeUnit.MILLISECONDS);
                successes.add(result);
                completed.add(i);
            } catch (TimeoutException e) {
                timedOut = true;
                cancelRemaining(futures, i);
                for (int j = i; j < futures.size(); j++) {
                    failures.add(new Failure(j, "TimeoutException",
                            TerminationReason.DEADLINE_EXCEEDED.name()));
                }
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancelRemaining(futures, i);
                failures.add(new Failure(i, "InterruptedException",
                        TerminationReason.CANCELLED_BY_CALLER.name()));
                for (int j = i + 1; j < futures.size(); j++) {
                    failures.add(new Failure(j, "InterruptedException",
                            TerminationReason.CANCELLED_BY_CALLER.name()));
                }
                break;
            } catch (ExecutionException e) {
                failures.add(classify(i, e.getCause()));
            }
        }

        Outcome outcome = determineOutcome(ctx, timedOut, successes.size(), tasks.size());
        if (ctx != null) {
            if (outcome == Outcome.TIMED_OUT) {
                ctx.terminate(TerminationReason.DEADLINE_EXCEEDED, true);
            } else if (outcome == Outcome.CANCELLED) {
                ctx.terminate(ctx.cancellation().reason(), true);
            }
            // 仅记录流水，不再做预算检查（超时场景下结果必须能返回）
            ctx.ledger().recordStage("fanout-end", ctx.ledger().consumedMillis(),
                    ctx.deadline().remainingMillis());
        }
        return new FanOutResult<>(successes, failures, completed, outcome);
    }

    private static void cancelRemaining(List<? extends Future<?>> futures, int from) {
        for (int j = from; j < futures.size(); j++) {
            futures.get(j).cancel(true);
        }
    }

    private static Failure classify(int index, Throwable cause) {
        if (cause instanceof BudgetException be) {
            return new Failure(index, be.getClass().getSimpleName(), be.reason().name());
        }
        if (cause instanceof CancellationException) {
            return new Failure(index, "CancellationException",
                    TerminationReason.CANCELLED_BY_CALLER.name());
        }
        return new Failure(index,
                cause == null ? "unknown" : cause.getClass().getSimpleName(),
                TerminationReason.INTERNAL_ERROR.name());
    }

    private static Outcome determineOutcome(BudgetContext ctx, boolean timedOut,
                                            int succeeded, int total) {
        if (timedOut) {
            return Outcome.TIMED_OUT;
        }
        if (ctx != null && ctx.cancellation().isCancelled()) {
            return Outcome.CANCELLED;
        }
        if (succeeded == total) {
            return Outcome.ALL_SUCCEEDED;
        }
        return succeeded == 0 ? Outcome.ALL_FAILED : Outcome.PARTIAL;
    }
}
