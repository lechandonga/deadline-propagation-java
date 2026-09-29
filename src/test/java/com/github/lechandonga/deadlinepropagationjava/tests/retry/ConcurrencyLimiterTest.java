package com.github.lechandonga.deadlinepropagationjava.tests.retry;

import com.github.lechandonga.deadlinepropagationjava.retry.ConcurrencyLimiter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyLimiterTest {

    @Test
    void boundedLimiterRejectsWithoutQueuingAndReleasesOnClose() {
        ConcurrencyLimiter limiter = ConcurrencyLimiter.bounded("e", 2);
        try (ConcurrencyLimiter.Lease first = limiter.tryAcquire();
             ConcurrencyLimiter.Lease second = limiter.tryAcquire()) {
            assertNotNull(first);
            assertNotNull(second);
            assertNull(limiter.tryAcquire(), "at the cap: degrade now, never queue");
        }
        assertNotNull(limiter.tryAcquire(), "released permits are reusable");
    }

    @Test
    void unboundedLimiterAlwaysGrants() {
        ConcurrencyLimiter limiter = ConcurrencyLimiter.bounded("e", 0);
        for (int i = 0; i < 100; i++) {
            assertNotNull(limiter.tryAcquire());
        }
    }
}
