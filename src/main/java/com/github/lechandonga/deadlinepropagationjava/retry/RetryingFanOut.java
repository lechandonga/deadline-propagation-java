package com.github.lechandonga.deadlinepropagationjava.retry;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.BudgetAwareExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Runs named units of work concurrently, then automatically retries only
 * the units that never confirmed completion — strictly within the request's
 * original shrinking budget.
 *
 * <p>Guarantees:
 * <ul>
 *   <li>confirmed success is final: its side effects are never executed again;</li>
 *   <li>retries are capped per {@link RetryPolicy}, separated by backoff,
 *       and never extend the request (no new budget after a timeout);</li>
 *   <li>a retry is never even started when the remaining budget cannot pay
 *       the entrypoint's startup threshold, nor after caller cancellation
 *       or client disconnect;</li>
 *   <li>at the entrypoint concurrency cap, work is degraded immediately
 *       ({@link StopReason#CONCURRENCY_LIMITED}) instead of queued.</li>
 * </ul>
 */
public final class RetryingFanOut {

    private final BudgetAwareExecutor executor;
    private final RetryScheduler scheduler;

    public RetryingFanOut(BudgetAwareExecutor executor, RetryScheduler scheduler) {
        this.executor = executor;
        this.scheduler = scheduler;
    }

    public record NamedWork<T>(String name, Callable<T> work) {
    }

    /**
     * Executes all items and returns their single deterministic
     * {@link DegradedResult}. Never throws for item-level failures or
     * budget termination: the result always carries the three-state
     * classification instead.
     */
    public <T> DegradedResult<T> execute(RequestBudget budget, RetryPolicy policy,
                                         ConcurrencyLimiter limiter,
                                         List<NamedWork<T>> items) {
        List<CompletableFuture<ItemResult<T>>> futures = new ArrayList<>(items.size());
        for (NamedWork<T> item : items) {
            futures.add(runItem(budget, policy, limiter, item, 0, null));
        }

        List<ItemResult<T>> results = new ArrayList<>(futures.size());
        for (CompletableFuture<ItemResult<T>> future : futures) {
            results.add(future.join());
        }
        return DegradedResult.of(results);
    }

    private <T> CompletableFuture<ItemResult<T>> runItem(RequestBudget budget, RetryPolicy policy,
                                                         ConcurrencyLimiter limiter,
                                                         NamedWork<T> item, int attemptsBefore,
                                                         ItemResult.Status lastStatus) {
        // 1. Never start anything for a dead request (timeout / cancel / disconnect).
        StopReason termination = terminationStopReason(budget);
        if (termination != null) {
            return CompletableFuture.completedFuture(
                    ItemResult.failed(item.name(), statusFor(termination, lastStatus),
                            attemptsBefore, termination));
        }

        // 2. Startup-cost floor: if the remaining budget cannot even pay for
        //    another attempt, give up instead of starting a doomed one.
        Duration remaining = budget.remaining();
        if (remaining.compareTo(policy.retryThreshold()) < 0) {
            return CompletableFuture.completedFuture(
                    ItemResult.failed(item.name(),
                            statusFor(StopReason.REMAINING_BELOW_THRESHOLD, lastStatus),
                            attemptsBefore, StopReason.REMAINING_BELOW_THRESHOLD));
        }

        // 3. Concurrency cap: no queuing, degrade immediately with the cause.
        ConcurrencyLimiter.Lease lease = limiter.tryAcquire();
        if (lease == null) {
            return CompletableFuture.completedFuture(
                    ItemResult.failed(item.name(), ItemResult.Status.REJECTED,
                            attemptsBefore, StopReason.CONCURRENCY_LIMITED));
        }

        final CompletableFuture<T> attempt;
        try {
            attempt = executor.submit(item.work(), budget);
        } catch (RuntimeException rejected) {
            lease.close();
            StopReason reason = stopReasonFor(rejected);
            return CompletableFuture.completedFuture(
                    ItemResult.failed(item.name(), statusFor(reason, lastStatus),
                            attemptsBefore, reason));
        }

        final int attemptsStarted = attemptsBefore + 1;
        return attempt.handle((value, error) -> {
            lease.close(); // slot released immediately; backoff does not hold it
            if (error == null) {
                return ItemResult.success(item.name(), value, attemptsStarted);
            }
            StopReason reason = stopReasonFor(error);
            ItemResult.Status status = statusFor(reason, null);

            if (attemptsStarted >= policy.maxAttempts()) {
                return ItemResult.<T>failed(item.name(), status, attemptsStarted,
                        StopReason.RETRY_LIMIT_REACHED);
            }
            // Carry the observed status into the retry-decision stage.
            throw new AttemptFailed(status);
        }).exceptionallyCompose(error -> {
            AttemptFailed failed = unwrapAttemptFailure(error);
            if (failed == null) {
                return CompletableFuture.completedFuture(ItemResult.failed(item.name(),
                        ItemResult.Status.FAILED, attemptsStarted, StopReason.INTERNAL_ERROR));
            }
            // Retry decision is re-made after waiting: cancellation or expiry
            // during backoff means no retry is ever launched afterwards.
            Duration wait = policy.backoff();
            Duration left = budget.remaining();
            if (wait.compareTo(left) > 0) {
                wait = left;
            }
            return scheduler.delay(budget, wait).thenCompose(ignored -> {
                StopReason afterWait = terminationStopReason(budget);
                if (afterWait != null) {
                    return CompletableFuture.completedFuture(
                            ItemResult.failed(item.name(), statusFor(afterWait, failed.status),
                                    attemptsStarted, afterWait));
                }
                if (budget.remaining().compareTo(policy.retryThreshold()) < 0) {
                    return CompletableFuture.completedFuture(
                            ItemResult.failed(item.name(),
                                    statusFor(StopReason.REMAINING_BELOW_THRESHOLD, failed.status),
                                    attemptsStarted, StopReason.REMAINING_BELOW_THRESHOLD));
                }
                return runItem(budget, policy, limiter, item, attemptsStarted, failed.status);
            });
        });
    }

    private static StopReason terminationStopReason(RequestBudget budget) {
        TerminationReason reason = budget.terminationReason();
        return switch (reason) {
            case NONE -> null;
            case CLIENT_CANCELLED -> StopReason.CLIENT_CANCELLED;
            case CANCELLED -> StopReason.CANCELLED;
            case DEADLINE_EXCEEDED, REJECTED_BUDGET_EXHAUSTED -> StopReason.BUDGET_EXHAUSTED;
            case REJECTED_CONCURRENCY_LIMIT -> StopReason.CONCURRENCY_LIMITED;
            case ERROR -> StopReason.INTERNAL_ERROR;
        };
    }

    private static StopReason stopReasonFor(Throwable error) {
        Throwable cause = error;
        while (cause instanceof java.util.concurrent.CompletionException
                || cause instanceof java.util.concurrent.ExecutionException) {
            if (cause.getCause() == null) {
                break;
            }
            cause = cause.getCause();
        }
        if (cause instanceof BudgetExhaustedException) {
            return StopReason.BUDGET_EXHAUSTED;
        }
        if (cause instanceof RequestCancelledException cancelled) {
            return switch (cancelled.reason()) {
                case CLIENT_CANCELLED -> StopReason.CLIENT_CANCELLED;
                case CANCELLED -> StopReason.CANCELLED;
                case REJECTED_CONCURRENCY_LIMIT -> StopReason.CONCURRENCY_LIMITED;
                case REJECTED_BUDGET_EXHAUSTED, DEADLINE_EXCEEDED -> StopReason.BUDGET_EXHAUSTED;
                default -> StopReason.INTERNAL_ERROR;
            };
        }
        return StopReason.INTERNAL_ERROR;
    }

    private static ItemResult.Status statusFor(StopReason reason, ItemResult.Status fallback) {
        ItemResult.Status mapped = switch (reason) {
            case BUDGET_EXHAUSTED, REMAINING_BELOW_THRESHOLD -> ItemResult.Status.TIMEOUT;
            case CLIENT_CANCELLED, CANCELLED -> ItemResult.Status.CANCELLED;
            case CONCURRENCY_LIMITED -> ItemResult.Status.REJECTED;
            default -> ItemResult.Status.FAILED;
        };
        return fallback != null && reason == StopReason.REMAINING_BELOW_THRESHOLD
                ? fallback : mapped;
    }

    private static AttemptFailed unwrapAttemptFailure(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof AttemptFailed failed) {
                return failed;
            }
            cause = cause.getCause();
        }
        return null;
    }

    /** Internal carrier of the last failure between attempt stages. */
    private static final class AttemptFailed extends RuntimeException {
        private final ItemResult.Status status;

        AttemptFailed(ItemResult.Status status) {
            super(status.name(), null, false, false);
            this.status = status;
        }
    }
}
