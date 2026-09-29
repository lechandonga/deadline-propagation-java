package com.github.lechandonga.deadlinepropagationjava.retry;

import java.time.Duration;

/**
 * Per-entry retry policy. Retries always spend the original request budget:
 * they never extend the deadline and never mint new time after a timeout.
 *
 * @param maxAttempts        total attempts per unit of work including the first (>= 1)
 * @param retryInterval      minimum delay between attempts; interruptible by termination
 * @param minRemainingBudget no retry is started when less than this much budget remains
 */
public record RetryPolicy(int maxAttempts, Duration retryInterval, Duration minRemainingBudget) {

    public static RetryPolicy noRetries() {
        return new RetryPolicy(1, Duration.ZERO, Duration.ZERO);
    }

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1: " + maxAttempts);
        }
        if (retryInterval == null || retryInterval.isNegative()) {
            throw new IllegalArgumentException("retryInterval must not be null or negative");
        }
        if (minRemainingBudget == null || minRemainingBudget.isNegative()) {
            throw new IllegalArgumentException("minRemainingBudget must not be null or negative");
        }
    }

    /** Extra attempts after the first one. */
    public int maxRetries() {
        return maxAttempts - 1;
    }

    public boolean retriesEnabled() {
        return maxAttempts > 1;
    }
}
