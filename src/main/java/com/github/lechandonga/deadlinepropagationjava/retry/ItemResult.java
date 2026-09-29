package com.github.lechandonga.deadlinepropagationjava.retry;

/**
 * Final per-item conclusion inside a {@link DegradedResult}. The status set
 * is a strict superset of the original FanOut classification so callers can
 * still tell success, timeout, failure and cancellation apart, plus the new
 * up-front concurrency rejection.
 *
 * @param name       logical unit-of-work name (stable across attempts)
 * @param status     final status after all affordable attempts
 * @param value      confirmed result value when {@link Status#SUCCESS}
 * @param attempts   attempts actually started (0 when rejected up front)
 * @param stopReason why this item gave up; {@link StopReason#NONE} on success
 */
public record ItemResult<T>(String name, Status status, T value, int attempts,
                            StopReason stopReason) {

    public enum Status {
        SUCCESS, TIMEOUT, FAILED, CANCELLED, REJECTED
    }

    public boolean completed() {
        return status == Status.SUCCESS;
    }

    public static <T> ItemResult<T> success(String name, T value, int attempts) {
        return new ItemResult<>(name, Status.SUCCESS, value, attempts, StopReason.NONE);
    }

    public static <T> ItemResult<T> failed(String name, Status status, int attempts,
                                           StopReason reason) {
        return new ItemResult<>(name, status, null, attempts, reason);
    }
}
