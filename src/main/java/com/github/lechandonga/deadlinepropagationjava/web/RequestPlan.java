package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.retry.ConcurrencyLimiter;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryPolicy;

import java.time.Duration;

/**
 * Immutable per-request snapshot of the resolved entry policy. A request
 * reads this snapshot once, so configuration cannot shift underneath it
 * while it runs. Unconfigured entries resolve to the legacy default plan
 * (default budget, retry disabled, no concurrency cap).
 */
public record RequestPlan(String entry,
                          Duration budget,
                          RetryPolicy retryPolicy,
                          ConcurrencyLimiter limiter) {

    public boolean retryEnabled() {
        return retryPolicy.retriesEnabled();
    }
}
