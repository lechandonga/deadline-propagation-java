package com.github.lechandonga.deadlinepropagationjava.tests.web;

import com.github.lechandonga.deadlinepropagationjava.retry.RetryPolicy;
import com.github.lechandonga.deadlinepropagationjava.web.BudgetProperties;
import com.github.lechandonga.deadlinepropagationjava.web.EntryPolicyResolver;
import com.github.lechandonga.deadlinepropagationjava.web.RequestPlan;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class EntryPolicyResolverTest {

    private static BudgetProperties defaults() {
        BudgetProperties p = new BudgetProperties();
        p.setDefaultBudget(Duration.ofSeconds(5));
        return p;
    }

    private static BudgetProperties.Entry entry(String path, Duration budget,
                                                Integer maxAttempts, Duration interval,
                                                Duration minRemaining, Integer maxConcurrency) {
        BudgetProperties.Entry e = new BudgetProperties.Entry();
        e.setPath(path);
        e.setBudget(budget);
        e.setMaxAttempts(maxAttempts);
        e.setRetryInterval(interval);
        e.setMinRemainingBudget(minRemaining);
        e.setMaxConcurrency(maxConcurrency);
        return e;
    }

    @Test
    void unconfiguredEntryKeepsLegacyDefaults() {
        EntryPolicyResolver resolver = new EntryPolicyResolver(defaults());
        RequestPlan plan = resolver.resolve("/api/anything");

        assertEquals("default", plan.entry());
        assertEquals(Duration.ofSeconds(5), plan.budget());
        assertFalse(plan.retryEnabled());
        assertEquals(RetryPolicy.noRetries(), plan.retryPolicy());
        assertNull(plan.limiter(), "no concurrency cap unless configured");
    }

    @Test
    void configuredEntryIsIsolatedFromOthers() {
        BudgetProperties props = defaults();
        props.getEntries().put("degrade", entry("/api/degrade", Duration.ofSeconds(2),
                4, Duration.ofMillis(20), Duration.ofMillis(60), 3));
        EntryPolicyResolver resolver = new EntryPolicyResolver(props);

        RequestPlan degrade = resolver.resolve("/api/degrade");
        assertEquals("degrade", degrade.entry());
        assertEquals(Duration.ofSeconds(2), degrade.budget());
        assertEquals(4, degrade.retryPolicy().maxAttempts());
        assertEquals(Duration.ofMillis(20), degrade.retryPolicy().retryInterval());
        assertEquals(Duration.ofMillis(60), degrade.retryPolicy().minRemainingBudget());
        assertNotNull(degrade.limiter());

        RequestPlan other = resolver.resolve("/api/fanout");
        assertEquals("default", other.entry());
        assertEquals(Duration.ofSeconds(5), other.budget());
        assertFalse(other.retryEnabled());
        assertNull(other.limiter());
    }

    @Test
    void thresholdLargerThanBudgetIsValidAndDisablesAllRetries() {
        // Legitimate way to say "never retry here": the remaining time can
        // never cover the startup cost.
        BudgetProperties props = defaults();
        props.getEntries().put("deadline", entry("/api/deadline", Duration.ofMillis(300),
                3, Duration.ofMillis(10), Duration.ofMillis(400), null));
        EntryPolicyResolver resolver = new EntryPolicyResolver(props);

        assertEquals(Duration.ofMillis(400),
                resolver.resolve("/api/deadline").retryPolicy().minRemainingBudget());
    }

    @Test
    void invalidBudgetFailsLoudly() {
        BudgetProperties props = defaults();
        props.setDefaultBudget(Duration.ZERO);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new EntryPolicyResolver(props));
        assertTrue(e.getMessage().contains("default-budget"));
    }

    @Test
    void retryEnabledWithoutIntervalFailsLoudly() {
        BudgetProperties props = defaults();
        props.getEntries().put("bad", entry("/api/bad", Duration.ofSeconds(1),
                3, null, Duration.ZERO, null));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new EntryPolicyResolver(props));
        assertTrue(e.getMessage().contains("retry-interval"));
    }

    @Test
    void nonPositiveAttemptsAndConcurrencyFailLoudly() {
        BudgetProperties props = defaults();
        props.getEntries().put("bad", entry("/api/bad", Duration.ofSeconds(1),
                0, Duration.ZERO, Duration.ZERO, null));
        assertThrows(IllegalStateException.class, () -> new EntryPolicyResolver(props));

        BudgetProperties props2 = defaults();
        props2.getEntries().put("bad2", entry("/api/bad2", Duration.ofSeconds(1),
                1, null, null, 0));
        assertThrows(IllegalStateException.class, () -> new EntryPolicyResolver(props2));
    }

    @Test
    void missingPathAndDuplicatePathsFailLoudly() {
        BudgetProperties props = defaults();
        props.getEntries().put("bad", entry(null, Duration.ofSeconds(1),
                1, null, null, null));
        assertThrows(IllegalStateException.class, () -> new EntryPolicyResolver(props));

        BudgetProperties props2 = defaults();
        props2.getEntries().put("a", entry("/api/same", Duration.ofSeconds(1),
                1, null, null, null));
        props2.getEntries().put("b", entry("/api/same", Duration.ofSeconds(1),
                1, null, null, null));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new EntryPolicyResolver(props2));
        assertTrue(e.getMessage().contains("duplicate"));
    }
}
