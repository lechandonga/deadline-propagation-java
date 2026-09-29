package com.github.lechandonga.deadlinepropagationjava.degrade;

import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.FanOut;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Meaningful result of a request after bounded retries. Never throws for
 * partial failure: callers get exactly one of three states and can see which
 * work is confirmed (and therefore unsafe to retry), which never confirmed,
 * and why everything stopped.
 *
 * <p>Determinism: contents are derived solely from the declared inputs in
 * declaration order, never from which task happened to finish first, so the
 * same inputs always yield the same degraded conclusion.
 */
public record DegradedResult<T>(
        DegradeStatus status,
        StopReason stopReason,
        Map<String, T> completed,
        List<IncompleteWork> incomplete) {

    /** Work that never confirmed completion and is therefore still safe to retry elsewhere. */
    public record IncompleteWork(String name, FanOut.Outcome.Status finalStatus,
                                 TerminationReason reason, int attempts) {
    }

    public static <T> DegradedResult<T> allSuccess(Map<String, T> completed) {
        return new DegradedResult<>(DegradeStatus.ALL_SUCCESS, StopReason.NONE,
                ordered(completed), List.of());
    }

    public static <T> DegradedResult<T> of(StopReason stopReason,
                                           Map<String, T> completed,
                                           List<IncompleteWork> incomplete) {
        DegradeStatus status = completed.isEmpty()
                ? DegradeStatus.TOTAL_FAILURE
                : DegradeStatus.PARTIAL_SUCCESS;
        return new DegradedResult<>(status, stopReason,
                ordered(completed), List.copyOf(incomplete));
    }

    private static <T> Map<String, T> ordered(Map<String, T> source) {
        // Keep declaration order so identical inputs serialize identically;
        // Map.copyOf does not promise encounter order.
        return java.util.Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    /** Confirmed work names in declaration order. */
    public List<String> completedNames() {
        return new ArrayList<>(completed.keySet());
    }

    /** Unconfirmed work names in declaration order. */
    public List<String> incompleteNames() {
        List<String> names = new ArrayList<>(incomplete.size());
        for (IncompleteWork work : incomplete) {
            names.add(work.name());
        }
        return names;
    }

    /** View helper stable under JSON serialization (insertion-ordered). */
    public Map<String, Object> toResponseBody() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status.name());
        body.put("stopReason", stopReason.name());
        body.put("completed", completedNames());
        List<Map<String, Object>> details = new ArrayList<>(incomplete.size());
        for (IncompleteWork work : incomplete) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("name", work.name());
            detail.put("status", work.finalStatus().name());
            detail.put("reason", work.reason().name());
            detail.put("attempts", work.attempts());
            details.add(detail);
        }
        body.put("incomplete", details);
        return body;
    }
}
