package com.github.lechandonga.deadlinepropagationjava.retry;

import java.util.concurrent.Semaphore;

/**
 * Entrypoint-scoped cap on work that is actually running or retrying.
 * Acquisition is non-blocking: at the cap, callers are degraded
 * immediately with {@link StopReason#CONCURRENCY_LIMITED} instead of being
 * queued and piling pressure on the downstream.
 *
 * <p>One instance is shared by all requests of one entrypoint; permits are
 * held only while an attempt is in flight (not during backoff).
 */
public final class ConcurrencyLimiter {

    private final String entrypoint;
    private final Semaphore permits;

    private ConcurrencyLimiter(String entrypoint, int permits) {
        this.entrypoint = entrypoint;
        this.permits = permits <= 0 ? null : new Semaphore(permits, true);
    }

    public static ConcurrencyLimiter bounded(String entrypoint, int maxConcurrency) {
        return new ConcurrencyLimiter(entrypoint, maxConcurrency);
    }

    public String entrypoint() {
        return entrypoint;
    }

    /** Attempt handle; {@code null} when the cap is reached (no queuing). */
    public Lease tryAcquire() {
        if (permits == null) {
            return Lease.UNBOUNDED;
        }
        if (permits.tryAcquire()) {
            return new Lease(permits);
        }
        return null;
    }

    public static final class Lease implements AutoCloseable {
        static final Lease UNBOUNDED = new Lease(null);
        private final Semaphore source;

        private Lease(Semaphore source) {
            this.source = source;
        }

        @Override
        public void close() {
            if (source != null) {
                source.release();
            }
        }
    }
}
