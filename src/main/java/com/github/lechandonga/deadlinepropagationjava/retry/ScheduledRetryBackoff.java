package com.github.lechandonga.deadlinepropagationjava.retry;

import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Budget-interruptible delay implementation shared across requests. */
final class ScheduledRetryBackoff implements RetryScheduler {

    static final ScheduledRetryBackoff INSTANCE = new ScheduledRetryBackoff();

    private static final ScheduledExecutorService TIMER =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "retry-backoff");
                thread.setDaemon(true);
                return thread;
            });

    private ScheduledRetryBackoff() {
    }

    @Override
    public CompletableFuture<Void> delay(RequestBudget budget, Duration delay) {
        CompletableFuture<Void> ready = new CompletableFuture<>();
        if (delay.isZero() || delay.isNegative()) {
            ready.complete(null);
            return ready;
        }
        // If the budget is already dead (expired/cancelled/disconnected),
        // release immediately so the caller decides not to retry.
        AutoCloseable listener = budget.onTerminate(() -> ready.complete(null));
        TIMER.schedule(() -> {
            try {
                listener.close();
            } catch (Exception ignored) {
            }
            ready.complete(null);
        }, delay.toNanos(), TimeUnit.NANOSECONDS);
        return ready;
    }
}
