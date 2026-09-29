package com.github.lechandonga.deadlinepropagationjava.web;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** Configuration for the request time budget feature. */
@ConfigurationProperties(prefix = "request.budget")
public class BudgetProperties {

    /** Overall time budget granted to each incoming request. */
    private Duration defaultBudget = Duration.ofSeconds(5);

    /** Grace period to wait for in-flight work to stop after termination. */
    private Duration terminationGrace = Duration.ofMillis(500);

    /** Default total attempts per unit of work (including the first); 1 = no retries. */
    private int defaultMaxAttempts = 1;

    /** Default delay between attempts. */
    private Duration defaultRetryBackoff = Duration.ofMillis(100);

    /** Default remaining-budget floor below which another attempt is never started. */
    private Duration defaultRetryThreshold = Duration.ofMillis(50);

    /** Default cap on simultaneously running attempts per entrypoint; 0 or negative = unbounded. */
    private int maxConcurrency = 0;

    /** Per-entrypoint overrides keyed by request URI path; absent keys inherit defaults. */
    private Map<String, Entrypoint> entrypoints = new LinkedHashMap<>();

    public Duration getDefaultBudget() {
        return defaultBudget;
    }

    public void setDefaultBudget(Duration defaultBudget) {
        this.defaultBudget = defaultBudget;
    }

    public Duration getTerminationGrace() {
        return terminationGrace;
    }

    public void setTerminationGrace(Duration terminationGrace) {
        this.terminationGrace = terminationGrace;
    }

    public int getDefaultMaxAttempts() {
        return defaultMaxAttempts;
    }

    public void setDefaultMaxAttempts(int defaultMaxAttempts) {
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    public Duration getDefaultRetryBackoff() {
        return defaultRetryBackoff;
    }

    public void setDefaultRetryBackoff(Duration defaultRetryBackoff) {
        this.defaultRetryBackoff = defaultRetryBackoff;
    }

    public Duration getDefaultRetryThreshold() {
        return defaultRetryThreshold;
    }

    public void setDefaultRetryThreshold(Duration defaultRetryThreshold) {
        this.defaultRetryThreshold = defaultRetryThreshold;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public Map<String, Entrypoint> getEntrypoints() {
        return entrypoints;
    }

    public void setEntrypoints(Map<String, Entrypoint> entrypoints) {
        this.entrypoints = entrypoints == null ? new LinkedHashMap<>() : entrypoints;
    }

    /**
     * Fails fast on invalid configuration instead of silently falling back
     * to defaults. Invoked by Spring after binding.
     */
    @PostConstruct
    public void validate() {
        requirePositive(defaultBudget, "default-budget");
        requireNonNegative(terminationGrace, "termination-grace");
        if (defaultMaxAttempts < 1) {
            throw new IllegalStateException(
                    "request.budget.default-max-attempts must be >= 1: " + defaultMaxAttempts);
        }
        requireNonNegative(defaultRetryBackoff, "default-retry-backoff");
        requireNonNegative(defaultRetryThreshold, "default-retry-threshold");
        if (defaultRetryBackoff.compareTo(defaultBudget) >= 0) {
            throw new IllegalStateException(
                    "request.budget.default-retry-backoff must be smaller than the budget");
        }
        for (Map.Entry<String, Entrypoint> e : entrypoints.entrySet()) {
            e.getValue().validate(e.getKey(), defaultMaxAttempts);
        }
    }

    private static void requirePositive(Duration value, String key) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalStateException("request.budget." + key + " must be positive: " + value);
        }
    }

    private static void requireNonNegative(Duration value, String key) {
        if (value == null || value.isNegative()) {
            throw new IllegalStateException(
                    "request.budget." + key + " must not be negative: " + value);
        }
    }

    /** Per-entrypoint override; every property is optional and inherits the default. */
    public static class Entrypoint {

        private Duration budget;
        private Integer maxAttempts;
        private Duration backoff;
        private Duration retryThreshold;
        private Integer maxConcurrency;

        void validate(String key, int defaultMaxAttempts) {
            if (budget != null && (budget.isZero() || budget.isNegative())) {
                throw new IllegalStateException(
                        "request.budget.entrypoints." + key + ".budget must be positive: " + budget);
            }
            int effectiveMaxAttempts = maxAttempts != null ? maxAttempts : defaultMaxAttempts;
            if (maxAttempts != null && maxAttempts < 1) {
                throw new IllegalStateException("request.budget.entrypoints." + key
                        + ".max-attempts must be >= 1: " + maxAttempts);
            }
            if (backoff != null && backoff.isNegative()) {
                throw new IllegalStateException(
                        "request.budget.entrypoints." + key + ".backoff must not be negative");
            }
            if (retryThreshold != null && retryThreshold.isNegative()) {
                throw new IllegalStateException("request.budget.entrypoints." + key
                        + ".retry-threshold must not be negative");
            }
            if (backoff != null && budget != null && backoff.compareTo(budget) >= 0) {
                throw new IllegalStateException("request.budget.entrypoints." + key
                        + ".backoff must be smaller than its budget");
            }
            if (effectiveMaxAttempts < 1) {
                throw new IllegalStateException("request.budget.entrypoints." + key
                        + ".max-attempts must be >= 1");
            }
        }

        public Duration getBudget() {
            return budget;
        }

        public void setBudget(Duration budget) {
            this.budget = budget;
        }

        public Integer getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(Integer maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Duration getBackoff() {
            return backoff;
        }

        public void setBackoff(Duration backoff) {
            this.backoff = backoff;
        }

        public Duration getRetryThreshold() {
            return retryThreshold;
        }

        public void setRetryThreshold(Duration retryThreshold) {
            this.retryThreshold = retryThreshold;
        }

        public Integer getMaxConcurrency() {
            return maxConcurrency;
        }

        public void setMaxConcurrency(Integer maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
        }
    }
}
