package com.github.lechandonga.deadlinepropagationjava.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Configuration for the request time budget feature. */
@ConfigurationProperties(prefix = "request.budget")
public class BudgetProperties {

    /** Overall time budget granted to each incoming request. */
    private Duration defaultBudget = Duration.ofSeconds(5);

    /** Grace period to wait for in-flight work to stop after termination. */
    private Duration terminationGrace = Duration.ofMillis(500);

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
}
