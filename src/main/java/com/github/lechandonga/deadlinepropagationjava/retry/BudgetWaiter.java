package com.github.lechandonga.deadlinepropagationjava.retry;

import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;

import java.time.Duration;

/**
 * Waits the inter-attempt delay while remaining inside the request budget.
 * The wait is interruptible: expiry, caller cancellation or thread
 * interruption wakes it immediately, so a retry never fires after the
 * request is over and retries never extend the overall timeline.
 */
@FunctionalInterface
public interface BudgetWaiter {

    /**
     * Waits up to {@code delay} while the budget is alive. Returns normally
     * if the full delay elapsed with the budget still alive; throws
     * (via {@link RequestBudget#checkAlive()}) if termination occurred.
     */
    void await(RequestBudget budget, Duration delay) throws InterruptedException;

    /**
     * Production waiter: wakes on interruption (incl. the budget enforcer
     * interrupting the thread at the deadline) and re-checks the budget in
     * small slices so cancellation is noticed promptly without busy waiting.
     */
    static BudgetWaiter system() {
        return (budget, delay) -> {
            if (delay.isZero() || delay.isNegative()) {
                budget.checkAlive();
                return;
            }
            long remainingNanos = delay.toNanos();
            long sliceNanos = Duration.ofMillis(10).toNanos();
            while (remainingNanos > 0) {
                budget.checkAlive();
                long sleepNanos = Math.min(sliceNanos, remainingNanos);
                Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
                remainingNanos -= sleepNanos;
            }
            budget.checkAlive();
        };
    }
}
