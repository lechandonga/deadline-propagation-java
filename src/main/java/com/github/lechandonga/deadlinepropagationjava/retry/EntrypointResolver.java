package com.github.lechandonga.deadlinepropagationjava.retry;

import com.github.lechandonga.deadlinepropagationjava.web.BudgetProperties;

import java.time.Duration;

/**
 * Resolves the effective {@link EntrypointSettings} for a request.
 * Per-entrypoint overrides win; everything else inherits the defaults, so
 * unconfigured entrypoints behave exactly as before the feature existed.
 *
 * <p>The result is an immutable snapshot: property binding may mutate the
 * underlying {@link BudgetProperties} at runtime, but a running request
 * keeps the settings it started with.
 */
public final class EntrypointResolver {

    public static final String DEFAULT = "default";

    private final BudgetProperties properties;

    public EntrypointResolver(BudgetProperties properties) {
        this.properties = properties;
    }

    public EntrypointSettings resolve(String entrypoint) {
        String key = entrypoint == null || entrypoint.isBlank() ? DEFAULT : entrypoint;
        synchronized (properties) {
            Duration budget = properties.getDefaultBudget();
            RetryPolicy retry = new RetryPolicy(
                    properties.getDefaultMaxAttempts(),
                    properties.getDefaultRetryBackoff(),
                    properties.getDefaultRetryThreshold());
            int maxConcurrency = properties.getMaxConcurrency();

            BudgetProperties.Entrypoint override = properties.getEntrypoints().get(key);
            if (override != null) {
                if (override.getBudget() != null) {
                    budget = override.getBudget();
                }
                retry = new RetryPolicy(
                        override.getMaxAttempts() != null
                                ? override.getMaxAttempts()
                                : properties.getDefaultMaxAttempts(),
                        override.getBackoff() != null
                                ? override.getBackoff()
                                : properties.getDefaultRetryBackoff(),
                        override.getRetryThreshold() != null
                                ? override.getRetryThreshold()
                                : properties.getDefaultRetryThreshold());
                if (override.getMaxConcurrency() != null) {
                    maxConcurrency = override.getMaxConcurrency();
                }
            }
            return new EntrypointSettings(key, budget, retry, maxConcurrency);
        }
    }
}
