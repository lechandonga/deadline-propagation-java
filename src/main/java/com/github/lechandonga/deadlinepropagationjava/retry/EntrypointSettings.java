package com.github.lechandonga.deadlinepropagationjava.retry;

import java.time.Duration;

/**
 * Immutable, per-request snapshot of one entrypoint's effective settings.
 * A request reads this once on arrival; runtime configuration changes never
 * steer an in-flight request.
 *
 * @param name           entrypoint key (request URI path), or {@code "default"}
 * @param budget         total time budget for the request
 * @param retry          retry policy in effect for the request
 * @param maxConcurrency cap on simultaneously running attempts across the
 *                       entrypoint; {@code <= 0} means unbounded
 */
public record EntrypointSettings(String name, Duration budget, RetryPolicy retry,
                                 int maxConcurrency) {

    public EntrypointSettings {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("entrypoint name must be present");
        }
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw new IllegalArgumentException("budget must be positive: " + budget);
        }
        if (retry == null) {
            throw new IllegalArgumentException("retry policy must be present");
        }
    }
}
