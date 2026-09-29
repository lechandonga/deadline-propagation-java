package com.github.lechandonga.deadlinepropagationjava.retry;

import java.util.concurrent.Semaphore;

/**
 * Per-entry cap on work that is genuinely running or retrying. When the cap
 * is reached, work is rejected immediately (never queued) so retries cannot
 * pile up or hammer a downstream. Permits are fair, so acquisition order is
 * deterministic; rejection does not block.
 */
public final class ConcurrencyLimiter {

    private final String entry;
    private final Semaphore permits;

    public ConcurrencyLimiter(String entry, int maxConcurrent) {
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("maxConcurrent must be >= 1: " + maxConcurrent);
        }
        this.entry = entry;
        this.permits = new Semaphore(maxConcurrent, true);
    }

    public String entry() {
        return entry;
    }

    /** Acquires a permit immediately, or returns {@code null} when at the cap. */
    public Permit tryAcquire() {
        return permits.tryAcquire() ? new Permit() : null;
    }

    public int availablePermits() {
        return permits.availablePermits();
    }

    /** A held slot; must be closed exactly once when the attempt finishes. */
    public final class Permit implements AutoCloseable {
        private boolean closed;

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            permits.release();
        }
    }
}
