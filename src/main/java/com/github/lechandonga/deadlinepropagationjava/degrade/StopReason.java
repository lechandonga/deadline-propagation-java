package com.github.lechandonga.deadlinepropagationjava.degrade;

/**
 * Why degraded work stopped. Budget exhaustion, caller cancellation and
 * internal errors are always distinguishable; budget exhaustion is further
 * split between a hard deadline and giving up because the remaining time
 * could not cover even the startup cost of one more attempt.
 */
public enum StopReason {
    /** Every unit of work confirmed completion; nothing was retried in vain. */
    NONE,
    /** The request deadline was reached while work (or a retry) was still unconfirmed. */
    BUDGET_EXHAUSTED,
    /** Remaining budget was below the configured retry threshold; no retry was attempted. */
    INSUFFICIENT_REMAINING,
    /** The caller disconnected or cancelled; no retry is allowed afterwards. */
    CANCELLED,
    /** Work kept failing internally until the attempt limit was reached. */
    RETRIES_EXHAUSTED,
    /** The entry concurrency cap was reached; work was rejected instead of queued. */
    CONCURRENCY_LIMITED,
    /** An internal error occurred that was not safe to keep retrying. */
    INTERNAL_ERROR
}
