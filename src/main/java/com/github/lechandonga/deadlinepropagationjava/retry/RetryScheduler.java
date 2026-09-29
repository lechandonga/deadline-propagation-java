package com.github.lechandonga.deadlinepropagationjava.retry;

import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Schedules the delay between attempts. The wait is itself bound to the
 * request budget: when the budget terminates (deadline or cancellation) the
 * returned future completes <em>immediately</em> with the termination
 * signal, so no retry ever starts after a timeout or disconnect, and no new
 * time is ever spent beyond the original budget.
 */
@FunctionalInterface
public interface RetryScheduler {

    /**
     * Completes after {@code delay} or as soon as the budget terminates.
     * The future always completes normally; callers re-check the budget.
     */
    CompletableFuture<Void> delay(RequestBudget budget, Duration delay);

    /** Production scheduler backed by one shared daemon scheduled pool. */
    static RetryScheduler scheduled() {
        return ScheduledRetryBackoff.INSTANCE;
    }

    /** Test scheduler with no real waiting: backoff elapses synchronously. */
    static RetryScheduler immediate() {
        return (budget, delay) -> {
            budget.terminationReason(); // promote expiry without sleeping
            return CompletableFuture.completedFuture(null);
        };
    }
}
