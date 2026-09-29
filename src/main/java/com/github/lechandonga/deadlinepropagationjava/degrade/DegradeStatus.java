package com.github.lechandonga.deadlinepropagationjava.degrade;

/**
 * The three states a client can distinguish at a glance:
 * everything confirmed, partial degraded result, or nothing usable.
 */
public enum DegradeStatus {
    /** All units of work confirmed completion. */
    ALL_SUCCESS,
    /** Some work confirmed completion; the rest is returned as degraded. */
    PARTIAL_SUCCESS,
    /** No work confirmed completion; the whole request failed. */
    TOTAL_FAILURE
}
