package com.github.lechandonga.deadlinepropagationjava.web;

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

    /**
     * Per-entry overrides keyed by entry name. Entries not listed here keep
     * the legacy behavior exactly (default budget, no retries, no cap).
     */
    private Map<String, Entry> entries = new LinkedHashMap<>();

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

    public Map<String, Entry> getEntries() {
        return entries;
    }

    public void setEntries(Map<String, Entry> entries) {
        this.entries = entries;
    }

    /**
     * Per-entry policy. Any illegal value fails application startup with a
     * clear message instead of silently falling back to defaults.
     */
    public static class Entry {
        /** Request path this entry matches, e.g. {@code /api/degrade}. */
        private String path;
        /** Total budget for this entry; must be positive. */
        private Duration budget;
        /** Total attempts per unit of work including the first; {@code 1} disables retry. */
        private Integer maxAttempts;
        /** Minimum delay between attempts; must be non-null/non-negative when retry is enabled. */
        private Duration retryInterval;
        /** Skip retry when remaining budget is below this. */
        private Duration minRemainingBudget;
        /** Max simultaneously running/retrying work; {@code null} means uncapped. */
        private Integer maxConcurrency;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
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

        public Duration getRetryInterval() {
            return retryInterval;
        }

        public void setRetryInterval(Duration retryInterval) {
            this.retryInterval = retryInterval;
        }

        public Duration getMinRemainingBudget() {
            return minRemainingBudget;
        }

        public void setMinRemainingBudget(Duration minRemainingBudget) {
            this.minRemainingBudget = minRemainingBudget;
        }

        public Integer getMaxConcurrency() {
            return maxConcurrency;
        }

        public void setMaxConcurrency(Integer maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
        }
    }
}
