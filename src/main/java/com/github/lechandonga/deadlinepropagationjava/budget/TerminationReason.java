package com.github.lechandonga.deadlinepropagationjava.budget;

/**
 * Why a unit of work terminated. Distinguishes deadline expiry, caller
 * cancellation and internal errors so nothing is silently misclassified.
 */
public enum TerminationReason {
    /** Work is still running or completed normally. */
    NONE,
    /** The request time budget was exhausted before the work finished. */
    DEADLINE_EXCEEDED,
    /** The caller (e.g. HTTP client disconnect) cancelled the request. */
    CLIENT_CANCELLED,
    /** Explicit cancellation by the application. */
    CANCELLED,
    /** Work was rejected because no budget remained; it never ran. */
    REJECTED_BUDGET_EXHAUSTED,
    /** Work failed with an internal error. */
    ERROR
}
