package com.github.lechandonga.deadlinepropagationjava.retry;

/**
 * Why processing stopped for a request that could not fully succeed.
 * Distinguishes budget exhaustion, cancellation and internal errors as
 * required, plus the deterministic retry give-up causes.
 */
public enum StopReason {
    /** Every unit of work confirmed completion. */
    NONE,
    /** The shared request budget ran out; retries could no longer be afforded. */
    BUDGET_EXHAUSTED,
    /** The caller (e.g. HTTP client disconnect) cancelled the request. */
    CLIENT_CANCELLED,
    /** Explicit cancellation by the application. */
    CANCELLED,
    /** The remaining budget was below the entrypoint's retry startup threshold. */
    REMAINING_BELOW_THRESHOLD,
    /** The entrypoint's attempt limit was reached for the unfinished work. */
    RETRY_LIMIT_REACHED,
    /** Work was rejected up front because the concurrency cap was reached. */
    CONCURRENCY_LIMITED,
    /** Unfinished work stopped because of an internal error. */
    INTERNAL_ERROR
}
