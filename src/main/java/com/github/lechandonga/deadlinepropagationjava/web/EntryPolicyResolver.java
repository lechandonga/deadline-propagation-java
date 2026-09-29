package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.retry.ConcurrencyLimiter;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves and validates per-entry policies once at startup, and resolves an
 * immutable {@link RequestPlan} per request.
 *
 * <p>Unconfigured paths are fully backward compatible: they get the default
 * budget, retries disabled and no concurrency cap. Bad configuration fails
 * fast with an explicit message — never a silent fallback.
 */
public final class EntryPolicyResolver {

    private final BudgetProperties properties;
    private final Map<String, ResolvedEntry> byPath = new LinkedHashMap<>();

    public EntryPolicyResolver(BudgetProperties properties) {
        this.properties = properties;
        validate();
    }

    private void validate() {
        Duration defaultBudget = properties.getDefaultBudget();
        if (defaultBudget == null || defaultBudget.isZero() || defaultBudget.isNegative()) {
            throw new IllegalStateException(
                    "request.budget.default-budget must be a positive duration: " + defaultBudget);
        }
        Duration grace = properties.getTerminationGrace();
        if (grace == null || grace.isNegative()) {
            throw new IllegalStateException(
                    "request.budget.termination-grace must be zero or positive: " + grace);
        }

        Map<String, ResolvedEntry> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, BudgetProperties.Entry> e : properties.getEntries().entrySet()) {
            String name = e.getKey();
            BudgetProperties.Entry entry = e.getValue();
            if (entry.getPath() == null || entry.getPath().isBlank()) {
                throw new IllegalStateException(
                        "request.budget.entries." + name + ".path must be set");
            }
            String path = entry.getPath();
            if (resolved.containsKey(path)) {
                throw new IllegalStateException(
                        "duplicate request.budget entry path '" + path + "' (entry " + name + ")");
            }
            Duration budget = entry.getBudget() != null ? entry.getBudget() : defaultBudget;
            if (budget.isZero() || budget.isNegative()) {
                throw new IllegalStateException(
                        "request.budget.entries." + name + ".budget must be positive: " + budget);
            }

            int maxAttempts = entry.getMaxAttempts() != null ? entry.getMaxAttempts() : 1;
            if (maxAttempts < 1) {
                throw new IllegalStateException(
                        "request.budget.entries." + name + ".max-attempts must be >= 1");
            }
            Duration interval = entry.getRetryInterval() != null
                    ? entry.getRetryInterval() : Duration.ZERO;
            if (interval.isNegative()) {
                throw new IllegalStateException(
                        "request.budget.entries." + name + ".retry-interval must not be negative");
            }
            if (maxAttempts > 1 && entry.getRetryInterval() == null) {
                throw new IllegalStateException(
                        "request.budget.entries." + name
                                + ".retry-interval is required when max-attempts > 1");
            }
            Duration minRemaining = entry.getMinRemainingBudget() != null
                    ? entry.getMinRemainingBudget() : Duration.ZERO;
            if (minRemaining.isNegative()) {
                throw new IllegalStateException("request.budget.entries." + name
                        + ".min-remaining-budget must not be negative");
            }
            ConcurrencyLimiter limiter = null;
            if (entry.getMaxConcurrency() != null) {
                if (entry.getMaxConcurrency() < 1) {
                    throw new IllegalStateException("request.budget.entries." + name
                            + ".max-concurrency must be >= 1");
                }
                limiter = new ConcurrencyLimiter(name, entry.getMaxConcurrency());
            }
            resolved.put(path, new ResolvedEntry(name, budget,
                    new RetryPolicy(maxAttempts, interval, minRemaining), limiter));
        }
        this.byPath.putAll(resolved);
    }

    /** Resolves the plan for a request path; unknown paths get legacy defaults. */
    public RequestPlan resolve(String requestPath) {
        ResolvedEntry entry = byPath.get(requestPath);
        if (entry == null) {
            return new RequestPlan("default", properties.getDefaultBudget(),
                    RetryPolicy.noRetries(), null);
        }
        return new RequestPlan(entry.name, entry.budget, entry.retryPolicy, entry.limiter);
    }

    /** Entry names in declaration order (for diagnostics). */
    public List<String> entryNames() {
        return new ArrayList<>(byPath.keySet());
    }

    private record ResolvedEntry(String name, Duration budget, RetryPolicy retryPolicy,
                                 ConcurrencyLimiter limiter) {
    }
}
