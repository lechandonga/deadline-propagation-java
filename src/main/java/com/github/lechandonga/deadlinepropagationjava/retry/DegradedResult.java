package com.github.lechandonga.deadlinepropagationjava.retry;

import java.util.ArrayList;
import java.util.List;

/**
 * The one, deterministic answer of a budget-bound retried fan-out. It never
 * throws away confirmed work: clients can tell the three global states
 * ({@link State#ALL_SUCCESS}, {@link State#PARTIAL_SUCCESS},
 * {@link State#FAILED}) apart at a glance, see exactly which items confirmed
 * completion (and therefore must never be retried), which did not, and why
 * processing stopped.
 *
 * <p>The aggregate is order-independent: it is computed from the item set
 * only, so the same inputs yield the same conclusion regardless of which
 * task finishes first.
 *
 * @param state        ALL_SUCCESS / PARTIAL_SUCCESS / FAILED
 * @param stopReason   why the request could not fully succeed;
 *                     {@link StopReason#NONE} when everything succeeded
 * @param items        per-item conclusions in the original submission order
 */
public record DegradedResult<T>(State state, StopReason stopReason, List<ItemResult<T>> items) {

    public enum State {
        ALL_SUCCESS, PARTIAL_SUCCESS, FAILED
    }

    public DegradedResult {
        items = List.copyOf(items);
    }

    public static <T> DegradedResult<T> of(List<ItemResult<T>> items) {
        boolean anySuccess = items.stream().anyMatch(ItemResult::completed);
        boolean allSuccess = !items.isEmpty() && items.stream().allMatch(ItemResult::completed);

        State state;
        if (allSuccess) {
            state = State.ALL_SUCCESS;
        } else if (anySuccess) {
            state = State.PARTIAL_SUCCESS;
        } else {
            state = State.FAILED;
        }
        return new DegradedResult<>(state, aggregateStopReason(state, items), items);
    }

    /** Confirmed-completion item names; retrying these would duplicate side effects. */
    public List<String> completed() {
        return collect(ItemResult::completed);
    }

    /** Item names that never confirmed completion; only these could be retried later. */
    public List<String> unfinished() {
        return collect(item -> !item.completed());
    }

    private List<String> collect(java.util.function.Predicate<ItemResult<T>> predicate) {
        List<String> names = new ArrayList<>();
        for (ItemResult<T> item : items) {
            if (predicate.test(item)) {
                names.add(item.name());
            }
        }
        return List.copyOf(names);
    }

    /**
     * Set-only aggregation with a fixed priority, so the conclusion is a
     * function of the item set and never of completion ordering. Cancellation
     * outranks budget, which outranks threshold/limit causes, then errors.
     */
    private static <T> StopReason aggregateStopReason(State state, List<ItemResult<T>> items) {
        if (state == State.ALL_SUCCESS) {
            return StopReason.NONE;
        }
        StopReason[] priority = {
                StopReason.CLIENT_CANCELLED,
                StopReason.CANCELLED,
                StopReason.BUDGET_EXHAUSTED,
                StopReason.REMAINING_BELOW_THRESHOLD,
                StopReason.CONCURRENCY_LIMITED,
                StopReason.RETRY_LIMIT_REACHED,
                StopReason.INTERNAL_ERROR
        };
        for (StopReason candidate : priority) {
            for (ItemResult<T> item : items) {
                if (!item.completed() && item.stopReason() == candidate) {
                    return candidate;
                }
            }
        }
        return StopReason.INTERNAL_ERROR;
    }
}
