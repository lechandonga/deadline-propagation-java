package com.github.lechandonga.deadlinepropagationjava.retry;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.degrade.DegradeStatus;
import com.github.lechandonga.deadlinepropagationjava.degrade.DegradedResult;
import com.github.lechandonga.deadlinepropagationjava.degrade.StopReason;
import com.github.lechandonga.deadlinepropagationjava.exec.ConcurrencyLimitRejectedException;
import com.github.lechandonga.deadlinepropagationjava.exec.FanOut;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Runs concurrent child work and retries only what never confirmed
 * completion, spending only the request's original shrinking budget.
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Confirmed work is never invoked again; only TIMEOUT/FAILED work is retried.</li>
 *   <li>Retries are bounded by {@link RetryPolicy#maxAttempts()} with an
 *       interruptible delay between attempts.</li>
 *   <li>No retry starts after deadline expiry or caller cancellation, and
 *       none starts when the remaining time cannot cover the configured
 *       minimum — the request degrades instead of forcing a hopeless attempt.</li>
 *   <li>At the entry concurrency cap, work is rejected immediately (never
 *       queued) and the request degrades as {@link StopReason#CONCURRENCY_LIMITED}.</li>
 *   <li>The conclusion depends only on declared inputs in declaration order,
 *       so identical inputs produce identical degraded results regardless of
 *       which task finishes first.</li>
 * </ul>
 */
public final class RetryingFanOut {

    private final BudgetWaiter waiter;

    public RetryingFanOut(BudgetWaiter waiter) {
        this.waiter = waiter;
    }

    public static RetryingFanOut create() {
        return new RetryingFanOut(BudgetWaiter.system());
    }

    public <T> DegradedResult<T> execute(List<FanOut.NamedTask<T>> tasks,
                                         RequestBudget budget,
                                         RetryPolicy policy,
                                         ConcurrencyLimiter limiter) {
        List<String> order = new ArrayList<>(tasks.size());
        Map<String, FanOut.NamedTask<T>> byName = new LinkedHashMap<>();
        for (FanOut.NamedTask<T> task : tasks) {
            if (byName.putIfAbsent(task.name(), task) != null) {
                throw new IllegalArgumentException("duplicate task name: " + task.name());
            }
            order.add(task.name());
        }

        Map<String, T> completed = new LinkedHashMap<>();
        Map<String, Integer> attempts = new LinkedHashMap<>();
        Map<String, FanOut.Outcome<T>> finals = new LinkedHashMap<>();
        List<String> pending = new ArrayList<>(order);

        StopReason stopReason = StopReason.NONE;

        while (true) {
            // Never start (or retry) anything once the budget is terminated.
            TerminationReason terminatedBeforeRound = budget.terminationReason();
            if (terminatedBeforeRound != TerminationReason.NONE) {
                stopReason = stopReasonForTermination(terminatedBeforeRound);
                break;
            }

            List<RoundAttempt<T>> round = new ArrayList<>(pending.size());
            for (String name : pending) {
                attempts.merge(name, 1, Integer::sum);
                RoundAttempt<T> attempt =
                        startAttempt(byName.get(name), limiter);
                round.add(attempt);
                if (attempt.rejectedByCap()) {
                    // Cap reached: degrade immediately, never queue, never retry.
                    stopReason = StopReason.CONCURRENCY_LIMITED;
                }
            }

            List<String> nextPending = new ArrayList<>();
            for (RoundAttempt<T> attempt : round) {
                FanOut.Outcome<T> outcome =
                        attempt.awaitOutcome(attempts.get(attempt.name()));
                if (outcome.status() == FanOut.Outcome.Status.SUCCESS) {
                    completed.put(attempt.name(), outcome.value());
                    finals.remove(attempt.name());
                    continue;
                }
                finals.put(attempt.name(), outcome);

                boolean terminal = stopReason == StopReason.CONCURRENCY_LIMITED
                        || outcome.status() == FanOut.Outcome.Status.REJECTED
                        || !isRetryable(outcome.status())
                        || attempts.get(attempt.name()) >= policy.maxAttempts()
                        || budget.terminationReason() != TerminationReason.NONE;
                if (!terminal) {
                    nextPending.add(attempt.name());
                }
            }

            if (stopReason == StopReason.CONCURRENCY_LIMITED) {
                pending = nextPending;
                break;
            }

            TerminationReason terminatedAfterRound = budget.terminationReason();
            if (terminatedAfterRound != TerminationReason.NONE) {
                pending = nextPending;
                stopReason = stopReasonForTermination(terminatedAfterRound);
                break;
            }

            if (nextPending.isEmpty()) {
                stopReason = completed.size() == order.size()
                        ? StopReason.NONE : StopReason.RETRIES_EXHAUSTED;
                pending = List.of();
                break;
            }

            // Remaining time must cover the startup cost of at least one more
            // attempt, otherwise give up instead of forcing a doomed retry.
            Duration remaining = budget.remaining();
            if (remaining.compareTo(policy.minRemainingBudget()) < 0) {
                pending = nextPending;
                stopReason = StopReason.INSUFFICIENT_REMAINING;
                break;
            }

            // Inter-attempt delay is spent inside the original budget and is
            // interruptible: expiry/cancellation wakes it and forbids retry.
            try {
                waiter.await(budget, policy.retryInterval());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                pending = nextPending;
                stopReason = stopReasonForTermination(budget.terminationReason());
                break;
            } catch (BudgetExhaustedException e) {
                pending = nextPending;
                stopReason = StopReason.BUDGET_EXHAUSTED;
                break;
            } catch (RequestCancelledException e) {
                pending = nextPending;
                stopReason = StopReason.CANCELLED;
                break;
            }
            pending = nextPending;
        }

        return buildResult(order, completed, finals, attempts, stopReason, budget);
    }

    private static boolean isRetryable(FanOut.Outcome.Status status) {
        return status == FanOut.Outcome.Status.TIMEOUT
                || status == FanOut.Outcome.Status.FAILED;
    }

    private static StopReason stopReasonForTermination(TerminationReason reason) {
        return reason == TerminationReason.DEADLINE_EXCEEDED
                || reason == TerminationReason.REJECTED_BUDGET_EXHAUSTED
                ? StopReason.BUDGET_EXHAUSTED
                : StopReason.CANCELLED;
    }

    private <T> RoundAttempt<T> startAttempt(FanOut.NamedTask<T> task,
                                             ConcurrencyLimiter limiter) {
        ConcurrencyLimiter.Permit permit =
                limiter == null ? null : limiter.tryAcquire();
        if (limiter != null && permit == null) {
            return RoundAttempt.rejected(task.name());
        }
        CompletableFuture<FanOut.Outcome<T>> classified;
        try {
            CompletableFuture<T> future = task.task().get();
            classified = future.handle((value, error) ->
                    classify(task.name(), value, error, 1));
        } catch (Throwable t) {
            if (permit != null) {
                permit.close();
            }
            classified = CompletableFuture.completedFuture(classify(task.name(), null, t, 1));
        }
        if (permit != null) {
            classified.whenComplete((o, e) -> permit.close());
        }
        return new RoundAttempt<>(task.name(), classified, false);
    }

    private static <T> FanOut.Outcome<T> classify(String name, T value, Throwable error,
                                                  int attempts) {
        FanOut.Outcome<T> base;
        if (error == null) {
            base = new FanOut.Outcome<>(name, FanOut.Outcome.Status.SUCCESS, value,
                    TerminationReason.NONE, null);
        } else {
            Throwable cause = unwrap(error);
            if (cause instanceof BudgetExhaustedException) {
                base = new FanOut.Outcome<>(name, FanOut.Outcome.Status.TIMEOUT, null,
                        TerminationReason.DEADLINE_EXCEEDED, cause);
            } else if (cause instanceof ConcurrencyLimitRejectedException) {
                base = new FanOut.Outcome<>(name, FanOut.Outcome.Status.REJECTED, null,
                        TerminationReason.REJECTED_CONCURRENCY_LIMIT, cause);
            } else if (cause instanceof RequestCancelledException cancelled) {
                base = new FanOut.Outcome<>(name, FanOut.Outcome.Status.CANCELLED, null,
                        cancelled.reason(), cause);
            } else {
                base = new FanOut.Outcome<>(name, FanOut.Outcome.Status.FAILED, null,
                        TerminationReason.ERROR, cause);
            }
        }
        return new FanOut.Outcome<>(name, base.status(), base.value(), base.reason(),
                base.error(), attempts);
    }

    private static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    private <T> DegradedResult<T> buildResult(
            List<String> order, Map<String, T> completed,
            Map<String, FanOut.Outcome<T>> finals, Map<String, Integer> attempts,
            StopReason stopReason, RequestBudget budget) {
        if (completed.size() == order.size()) {
            return DegradedResult.allSuccess(completed);
        }

        List<DegradedResult.IncompleteWork> incomplete = new ArrayList<>();
        for (String name : order) {
            if (completed.containsKey(name)) {
                continue;
            }
            FanOut.Outcome<T> last = finals.get(name);
            int count = attempts.getOrDefault(name, 0);
            TerminationReason terminal = budget.terminationReason();
            // Once the budget is terminal, that reason is authoritative for
            // every unfinished unit, even if its last attempt surfaced an
            // internal error: the request stopped because of the budget.
            if (terminal == TerminationReason.DEADLINE_EXCEEDED
                    || terminal == TerminationReason.REJECTED_BUDGET_EXHAUSTED) {
                incomplete.add(new DegradedResult.IncompleteWork(name,
                        FanOut.Outcome.Status.TIMEOUT,
                        TerminationReason.DEADLINE_EXCEEDED, count));
                continue;
            }
            if (terminal != TerminationReason.NONE) {
                incomplete.add(new DegradedResult.IncompleteWork(name,
                        FanOut.Outcome.Status.CANCELLED, terminal, count));
                continue;
            }
            if (last != null) {
                incomplete.add(new DegradedResult.IncompleteWork(
                        name, last.status(), last.reason(), count));
                continue;
            }
            incomplete.add(new DegradedResult.IncompleteWork(name,
                    FanOut.Outcome.Status.FAILED, TerminationReason.ERROR, count));
        }

        StopReason finalStop = stopReason;
        if (finalStop == StopReason.NONE && !incomplete.isEmpty()) {
            finalStop = StopReason.RETRIES_EXHAUSTED;
        }
        return new DegradedResult<>(
                completed.isEmpty() ? DegradeStatus.TOTAL_FAILURE : DegradeStatus.PARTIAL_SUCCESS,
                finalStop,
                java.util.Collections.unmodifiableMap(new LinkedHashMap<>(completed)),
                List.copyOf(incomplete));
    }

    /** One attempt of one named unit of work. */
    private static final class RoundAttempt<T> {
        private final String name;
        private final CompletableFuture<FanOut.Outcome<T>> outcome;
        private final boolean rejectedByCap;

        RoundAttempt(String name, CompletableFuture<FanOut.Outcome<T>> outcome,
                     boolean rejectedByCap) {
            this.name = name;
            this.outcome = outcome;
            this.rejectedByCap = rejectedByCap;
        }

        static <T> RoundAttempt<T> rejected(String name) {
            FanOut.Outcome<T> outcome = new FanOut.Outcome<>(name,
                    FanOut.Outcome.Status.REJECTED, null,
                    TerminationReason.REJECTED_CONCURRENCY_LIMIT,
                    new ConcurrencyLimitRejectedException(
                            "concurrency limit reached; degrading instead of queuing"));
            return new RoundAttempt<>(name,
                    CompletableFuture.completedFuture(outcome), true);
        }

        String name() {
            return name;
        }

        boolean rejectedByCap() {
            return rejectedByCap;
        }

        FanOut.Outcome<T> awaitOutcome(int attemptCount) {
            FanOut.Outcome<T> result = outcome.join();
            return new FanOut.Outcome<>(result.name(), result.status(), result.value(),
                    result.reason(), result.error(), attemptCount);
        }
    }
}
