package com.github.lechandonga.deadlinepropagationjava.tests.exec;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.FanOut;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FanOutTest {

    @Test
    void mixedOutcomesProduceOneDeterministicResult() throws Exception {
        FanOut.FanOutResult<String> result = FanOut.gather(List.of(
                new FanOut.NamedTask<>("ok", () -> CompletableFuture.completedFuture("done")),
                new FanOut.NamedTask<>("slow", () -> CompletableFuture.failedFuture(
                        new BudgetExhaustedException("budget exhausted"))),
                new FanOut.NamedTask<>("bad", () -> CompletableFuture.failedFuture(
                        new IllegalStateException("boom"))),
                new FanOut.NamedTask<>("cancelled", () -> CompletableFuture.failedFuture(
                        new RequestCancelledException("client gone", TerminationReason.CLIENT_CANCELLED)))
        )).get(2, TimeUnit.SECONDS);

        assertEquals(4, result.outcomes().size());
        assertEquals(FanOut.Outcome.Status.SUCCESS, result.outcomes().get(0).status());
        assertEquals(FanOut.Outcome.Status.TIMEOUT, result.outcomes().get(1).status());
        assertEquals(FanOut.Outcome.Status.FAILED, result.outcomes().get(2).status());
        assertEquals(FanOut.Outcome.Status.CANCELLED, result.outcomes().get(3).status());
        assertFalse(result.allSucceeded());
    }

    @Test
    void retryScopeNeverIncludesConfirmedWork() throws Exception {
        FanOut.FanOutResult<String> result = FanOut.gather(List.of(
                new FanOut.NamedTask<>("charged-card", () -> CompletableFuture.completedFuture("txn-1")),
                new FanOut.NamedTask<>("send-email", () -> CompletableFuture.failedFuture(
                        new BudgetExhaustedException("timed out")))
        )).get(2, TimeUnit.SECONDS);

        // Confirmed side effect is final and excluded from retries.
        assertEquals(List.of("charged-card"), result.completed());
        assertEquals(List.of("send-email"), result.retryable());
        assertFalse(result.retryable().contains("charged-card"),
                "retrying confirmed work would duplicate side effects");
    }

    @Test
    void timeoutIsDistinguishedFromFailureAndCancellation() throws Exception {
        FanOut.FanOutResult<String> result = FanOut.<String>gather(List.of(
                new FanOut.NamedTask<>("t", () -> CompletableFuture.failedFuture(
                        new BudgetExhaustedException("x"))),
                new FanOut.NamedTask<>("f", () -> CompletableFuture.failedFuture(
                        new RuntimeException("x"))),
                new FanOut.NamedTask<>("c", () -> CompletableFuture.failedFuture(
                        new RequestCancelledException("x", TerminationReason.CANCELLED)))
        )).get(2, TimeUnit.SECONDS);

        assertEquals(TerminationReason.DEADLINE_EXCEEDED, result.outcomes().get(0).reason());
        assertEquals(TerminationReason.ERROR, result.outcomes().get(1).reason());
        assertEquals(TerminationReason.CANCELLED, result.outcomes().get(2).reason());
    }

    @Test
    void allSuccessIsRetryFree() throws Exception {
        FanOut.FanOutResult<String> result = FanOut.gather(List.of(
                new FanOut.NamedTask<>("a", () -> CompletableFuture.completedFuture("1")),
                new FanOut.NamedTask<>("b", () -> CompletableFuture.completedFuture("2"))
        )).get(2, TimeUnit.SECONDS);

        assertTrue(result.allSucceeded());
        assertTrue(result.retryable().isEmpty());
        assertEquals(List.of("a", "b"), result.completed());
    }
}
