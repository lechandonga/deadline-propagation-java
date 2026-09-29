package com.github.lechandonga.deadlinepropagationjava.tests.retry;

import com.github.lechandonga.deadlinepropagationjava.retry.EntrypointResolver;
import com.github.lechandonga.deadlinepropagationjava.retry.EntrypointSettings;
import com.github.lechandonga.deadlinepropagationjava.web.BudgetProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class EntrypointResolverTest {

    private BudgetProperties propertiesWithDefaults() {
        BudgetProperties properties = new BudgetProperties();
        properties.setDefaultBudget(Duration.ofSeconds(5));
        properties.setDefaultMaxAttempts(1);
        properties.setDefaultRetryBackoff(Duration.ofMillis(100));
        properties.setDefaultRetryThreshold(Duration.ofMillis(50));
        return properties;
    }

    @Test
    void unconfiguredEntrypointInheritsLegacyDefaults() {
        EntrypointSettings settings =
                new EntrypointResolver(propertiesWithDefaults()).resolve("/api/anything");

        assertEquals(Duration.ofSeconds(5), settings.budget());
        assertEquals(1, settings.retry().maxAttempts(), "retries stay off by default");
        assertEquals(Duration.ofMillis(100), settings.retry().backoff());
        assertEquals(Duration.ofMillis(50), settings.retry().retryThreshold());
        assertEquals(0, settings.maxConcurrency(), "unbounded by default");
    }

    @Test
    void perEntrypointOverridesAreIsolated() {
        BudgetProperties properties = propertiesWithDefaults();
        BudgetProperties.Entrypoint override = new BudgetProperties.Entrypoint();
        override.setBudget(Duration.ofMillis(300));
        override.setMaxAttempts(4);
        override.setBackoff(Duration.ofMillis(10));
        override.setRetryThreshold(Duration.ofMillis(30));
        override.setMaxConcurrency(2);
        properties.getEntrypoints().put("/api/retry/fanout", override);
        EntrypointResolver resolver = new EntrypointResolver(properties);

        EntrypointSettings configured = resolver.resolve("/api/retry/fanout");
        assertEquals(Duration.ofMillis(300), configured.budget());
        assertEquals(4, configured.retry().maxAttempts());
        assertEquals(Duration.ofMillis(10), configured.retry().backoff());
        assertEquals(Duration.ofMillis(30), configured.retry().retryThreshold());
        assertEquals(2, configured.maxConcurrency());

        // A different entrypoint is completely unaffected.
        EntrypointSettings other = resolver.resolve("/api/sync");
        assertEquals(Duration.ofSeconds(5), other.budget());
        assertEquals(1, other.retry().maxAttempts());
        assertEquals(0, other.maxConcurrency());
    }

    @Test
    void partialOverrideKeepsUnsetDefaults() {
        BudgetProperties properties = propertiesWithDefaults();
        BudgetProperties.Entrypoint override = new BudgetProperties.Entrypoint();
        override.setBudget(Duration.ofSeconds(2));
        properties.getEntrypoints().put("/api/x", override);

        EntrypointSettings settings = new EntrypointResolver(properties).resolve("/api/x");
        assertEquals(Duration.ofSeconds(2), settings.budget());
        assertEquals(1, settings.retry().maxAttempts(), "unset retry fields inherit defaults");
        assertEquals(Duration.ofMillis(100), settings.retry().backoff());
    }

    @Test
    void snapshotIsStableAcrossRuntimeChanges() {
        BudgetProperties properties = propertiesWithDefaults();
        EntrypointResolver resolver = new EntrypointResolver(properties);
        EntrypointSettings snapshot = resolver.resolve("/api/x");

        properties.setDefaultBudget(Duration.ofSeconds(99));
        properties.setDefaultMaxAttempts(8);

        assertEquals(Duration.ofSeconds(5), snapshot.budget(),
                "an in-flight request keeps the settings it started with");
        assertEquals(1, snapshot.retry().maxAttempts());
    }

    @Test
    void invalidConfigurationFailsLoudly() {
        BudgetProperties badBudget = propertiesWithDefaults();
        badBudget.setDefaultBudget(Duration.ZERO);
        assertThrows(IllegalStateException.class, badBudget::validate);

        BudgetProperties badAttempts = propertiesWithDefaults();
        badAttempts.setDefaultMaxAttempts(0);
        assertThrows(IllegalStateException.class, badAttempts::validate);

        BudgetProperties badBackoff = propertiesWithDefaults();
        badBackoff.setDefaultRetryBackoff(Duration.ofSeconds(10));
        assertThrows(IllegalStateException.class, badBackoff::validate,
                "backoff >= budget must be rejected, not silently defaulted");

        BudgetProperties badEntry = propertiesWithDefaults();
        BudgetProperties.Entrypoint override = new BudgetProperties.Entrypoint();
        override.setMaxAttempts(-3);
        badEntry.getEntrypoints().put("/api/x", override);
        assertThrows(IllegalStateException.class, badEntry::validate);
    }
}
