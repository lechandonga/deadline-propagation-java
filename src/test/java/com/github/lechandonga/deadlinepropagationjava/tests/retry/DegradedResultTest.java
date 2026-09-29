package com.github.lechandonga.deadlinepropagationjava.tests.retry;

import com.github.lechandonga.deadlinepropagationjava.retry.DegradedResult;
import com.github.lechandonga.deadlinepropagationjava.retry.ItemResult;
import com.github.lechandonga.deadlinepropagationjava.retry.StopReason;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DegradedResultTest {

    private static ItemResult<String> success(String name) {
        return ItemResult.success(name, name + "-v", 1);
    }

    private static ItemResult<String> failed(String name, StopReason reason,
                                             ItemResult.Status status) {
        return ItemResult.failed(name, status, 2, reason);
    }

    @Test
    void allSuccessPartialAndFailedAreDistinct() {
        DegradedResult<String> all = DegradedResult.of(List.of(success("a"), success("b")));
        assertEquals(DegradedResult.State.ALL_SUCCESS, all.state());
        assertEquals(StopReason.NONE, all.stopReason());

        DegradedResult<String> partial = DegradedResult.of(
                List.of(success("a"), failed("b", StopReason.BUDGET_EXHAUSTED,
                        ItemResult.Status.TIMEOUT)));
        assertEquals(DegradedResult.State.PARTIAL_SUCCESS, partial.state());

        DegradedResult<String> none = DegradedResult.of(
                List.of(failed("a", StopReason.INTERNAL_ERROR, ItemResult.Status.FAILED)));
        assertEquals(DegradedResult.State.FAILED, none.state());
    }

    @Test
    void completedAndUnfinishedAreClassifiedExactlyOnce() {
        DegradedResult<String> result = DegradedResult.of(List.of(
                success("charged"),
                failed("email", StopReason.BUDGET_EXHAUSTED, ItemResult.Status.TIMEOUT),
                failed("audit", StopReason.INTERNAL_ERROR, ItemResult.Status.FAILED)));

        assertEquals(List.of("charged"), result.completed());
        assertEquals(List.of("email", "audit"), result.unfinished());
        assertTrue(CollectionsDisjoint.disjoint(result.completed(), result.unfinished()));
    }

    @Test
    void stopReasonAggregationIsOrderIndependent() {
        // Same item set, different list order: the priority of reasons is fixed.
        List<ItemResult<String>> orderOne = List.of(
                success("ok"),
                failed("timeout", StopReason.BUDGET_EXHAUSTED, ItemResult.Status.TIMEOUT),
                failed("cancel", StopReason.CLIENT_CANCELLED, ItemResult.Status.CANCELLED));
        List<ItemResult<String>> orderTwo = List.of(
                failed("cancel", StopReason.CLIENT_CANCELLED, ItemResult.Status.CANCELLED),
                success("ok"),
                failed("timeout", StopReason.BUDGET_EXHAUSTED, ItemResult.Status.TIMEOUT));

        assertEquals(DegradedResult.of(orderOne).stopReason(),
                DegradedResult.of(orderTwo).stopReason());
        assertEquals(StopReason.CLIENT_CANCELLED, DegradedResult.of(orderOne).stopReason(),
                "cancellation outranks budget exhaustion");
    }

    @Test
    void budgetOutranksInternalErrorAndThreshold() {
        DegradedResult<String> result = DegradedResult.of(List.of(
                failed("err", StopReason.INTERNAL_ERROR, ItemResult.Status.FAILED),
                failed("low", StopReason.REMAINING_BELOW_THRESHOLD, ItemResult.Status.TIMEOUT),
                failed("slow", StopReason.BUDGET_EXHAUSTED, ItemResult.Status.TIMEOUT)));
        assertEquals(StopReason.BUDGET_EXHAUSTED, result.stopReason());
    }

    private static final class CollectionsDisjoint {
        static boolean disjoint(List<String> a, List<String> b) {
            return a.stream().noneMatch(b::contains);
        }
    }
}
