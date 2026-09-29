package com.github.lechandonga.deadlinepropagationjava.retry;

import java.time.Duration;

/**
 * Retry policy for one entrypoint. Retries never extend the request: they
 * share the request's original shrinking budget, and a retry is only started
 * when enough of that original budget remains to pay for it.
 *
 * @param maxAttempts    total attempts per unit of work including the first;
 *                       {@code 1} disables retries and preserves legacy behaviour
 * @param backoff        minimum delay between attempts
 * @param retryThreshold attempts are given up when the remaining budget is
 *                       below this amount (startup cost floor)
 */
public record RetryPolicy(int maxAttempts, Duration backoff, Duration retryThreshold) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1: " + maxAttempts);
        }
        if (backoff == null || backoff.isNegative()) {
            throw new IllegalArgumentException("backoff must not be null or negative: " + backoff);
        }
        if (retryThreshold == null || retryThreshold.isNegative()) {
            throw new IllegalArgumentException(
                    "retryThreshold must not be null or negative: " + retryThreshold);
        }
    }

    /** Legacy-compatible default: no retries at all. */
    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, Duration.ZERO);
    }

    public boolean retriesEnabled() {
        return maxAttempts > 1;
    }
}
