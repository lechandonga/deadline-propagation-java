package com.github.lechandonga.deadlinepropagationjava.tests.retry;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.degrade.DegradeStatus;
import com.github.lechandonga.deadlinepropagationjava.degrade.DegradedResult;
import com.github.lechandonga.deadlinepropagationjava.degrade.StopReason;
import com.github.lechandonga.deadlinepropagationjava.exec.FanOut;
import com.github.lechandonga.deadlinepropagationjava.retry.BudgetWaiter;
import com.github.lechandonga.deadlinepropagationjava.retry.ConcurrencyLimiter;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryPolicy;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryingFanOut;
import com.github.lechandonga.deadlinepropagationjava.tests.support.ImmediateExecutor;
import com.github.lechandonga.deadlinepropagationjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryingFanOutTest {

    private static final RetryPolicy POLICY =
            new RetryPolicy(3, Duration.ofMillis(10), Duration.ofMillis(50));

    private static RetryingFanOut engine() {
        // No real sleeping: the delay is acknowledged instantly but still
        // consults the budget, so termination is honored deterministically.
        BudgetWaiter waiter = (budget, delay) -> budget.checkAlive();
        return new RetryingFanOut(waiter);
    }

    private static <T> FanOut.NamedTask<T> task(String name, ImmediateExecutor executor,
                                                RequestBudget budget,
                                                java.util.concurrent.Callable<T> body) {
        return new FanOut.NamedTask<>(name, () -> {
            try (BudgetContext.Scope ignored = BudgetContext.enter(budget)) {
                return executor.submit(body, budget);
            }
        });
    }

    @Test
    void retriesUnconfirmedWorkUntilItConfirms() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        ImmediateExecutor executor = ImmediateExecutor.create();
        AtomicInteger attempts = new AtomicInteger();

        DegradedResult<String> result = engine().execute(List.of(
                task("flaky", executor, budget, () -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("transient");
                    }
                    return "recovered";
                })), budget, POLICY, null);

        assertEquals(DegradeStatus.ALL_SUCCESS, result.status());
        assertEquals(StopReason.NONE, result.stopReason());
        assertEquals("recovered", result.completed().get("flaky"));
        assertEquals(3, attempts.get(), "work was retried until it confirmed");
        assertTrue(result.incomplete().isEmpty());
    }

    @Test
    void retriesAreBoundedByMaxAttemptsAndDegrade() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        ImmediateExecutor executor = ImmediateExecutor.create();
        AtomicInteger attempts = new AtomicInteger();

        DegradedResult<String> result = engine().execute(List.of(
                task("ok", executor, budget, () -> "done"),
                task("broken", executor, budget, () -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("always");
                })), budget, POLICY, null);

        assertEquals(DegradeStatus.PARTIAL_SUCCESS, result.status());
        assertEquals(StopReason.RETRIES_EXHAUSTED, result.stopReason());
        assertEquals(List.of("ok"), result.completedNames());
        assertEquals(List.of("broken"), result.incompleteNames());
        assertEquals(3, attempts.get(), "attempts are bounded: 1 initial + 2 retries");
        assertEquals(3, result.incomplete().get(0).attempts());
        assertEquals(TerminationReason.ERROR, result.incomplete().get(0).reason());
    }

    @Test
    void nothingConfirmedMeansTotalFailure() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        ImmediateExecutor executor = ImmediateExecutor.create();

        DegradedResult<String> result = engine().execute(List.of(
                task("a", executor, budget, () -> {
                    throw new IllegalStateException("x");
                })), budget, new RetryPolicy(1, Duration.ZERO, Duration.ZERO), null);

        assertEquals(DegradeStatus.TOTAL_FAILURE, result.status());
        assertEquals(StopReason.RETRIES_EXHAUSTED, result.stopReason());
        assertTrue(result.completed().isEmpty());
    }

    @Test
    void confirmedWorkIsNeverReinvoked() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        ImmediateExecutor executor = ImmediateExecutor.create();
        AtomicInteger confirmedCalls = new AtomicInteger();
        AtomicInteger brokenCalls = new AtomicInteger();

        engine().execute(List.of(
                task("charged", executor, budget, () -> {
                    confirmedCalls.incrementAndGet();
                    return "txn";
                }),
                task("broken", executor, budget, () -> {
                    brokenCalls.incrementAndGet();
                    throw new IllegalStateException("x");
                })), budget, POLICY, null);

        assertEquals(1, confirmedCalls.get(), "confirmed side effect must run exactly once");
        assertEquals(3, brokenCalls.get());
    }

    @Test
    void noRetryWhenRemainingBelowThreshold() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        ImmediateExecutor executor = ImmediateExecutor.create();
        AtomicInteger calls = new AtomicInteger();

        clock.advance(Duration.ofSeconds(9).plusMillis(900)); // 100ms left
        DegradedResult<String> result = engine().execute(List.of(
                task("slow", executor, budget, () -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("x");
                })), budget,
                new RetryPolicy(3, Duration.ofMillis(10), Duration.ofMillis(200)), null);

        assertEquals(StopReason.INSUFFICIENT_REMAINING, result.stopReason());
        assertEquals(1, calls.get(), "no retry when remaining time cannot cover startup cost");
        assertEquals(DegradeStatus.TOTAL_FAILURE, result.status());
    }

    @Test
    void deadlineExceededBeforeRoundStopsRetrying() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        ImmediateExecutor executor = ImmediateExecutor.create();
        AtomicInteger calls = new AtomicInteger();

        // Waiter simulates time crossing the deadline during the inter-attempt delay.
        RetryingFanOut retrying = new RetryingFanOut((b, delay) -> {
            clock.advance(Duration.ofSeconds(11));
            b.checkAlive();
        });
        DegradedResult<String> result = retrying.execute(List.of(
                task("slow", executor, budget, () -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("x");
                })), budget, POLICY, null);

        assertEquals(StopReason.BUDGET_EXHAUSTED, result.stopReason());
        assertEquals(1, calls.get(), "timeout must not mint a new budget / retry");
        assertEquals(TerminationReason.DEADLINE_EXCEEDED,
                result.incomplete().get(0).reason());
    }

    @Test
    void callerCancellationForbidsFurtherRetries() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        ImmediateExecutor executor = ImmediateExecutor.create();
        AtomicInteger calls = new AtomicInteger();

        RetryingFanOut retrying = new RetryingFanOut((b, delay) -> {
            b.cancel(TerminationReason.CLIENT_CANCELLED);
            throw new com.github.lechandonga.deadlinepropagationjava.budget
                    .RequestCancelledException("client gone", TerminationReason.CLIENT_CANCELLED);
        });
        // The first failure already recorded cancellation on the budget.
        DegradedResult<String> result = retrying.execute(List.of(
                task("gone", executor, budget, () -> {
                    calls.incrementAndGet();
                    budget.cancel(TerminationReason.CLIENT_CANCELLED);
                    throw new com.github.lechandonga.deadlinepropagationjava.budget
                            .RequestCancelledException("client gone",
                            TerminationReason.CLIENT_CANCELLED);
                })), budget, POLICY, null);

        assertEquals(StopReason.CANCELLED, result.stopReason());
        assertEquals(1, calls.get(), "no retry after caller cancellation");
    }

    @Test
    void alreadyDeadBudgetStartsNothing() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        clock.advance(Duration.ofSeconds(11));
        ImmediateExecutor executor = ImmediateExecutor.create();
        AtomicInteger calls = new AtomicInteger();

        DegradedResult<String> result = engine().execute(List.of(
                task("a", executor, budget, () -> {
                    calls.incrementAndGet();
                    return "x";
                })), budget, POLICY, null);

        assertEquals(0, calls.get(), "dead budget must not start work");
        assertEquals(StopReason.BUDGET_EXHAUSTED, result.stopReason());
        assertEquals(TerminationReason.DEADLINE_EXCEEDED,
                result.incomplete().get(0).reason());
    }

    @Test
    void concurrencyCapRejectsImmediatelyAndDegradesWithoutQueueing() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        ImmediateExecutor executor = ImmediateExecutor.create();
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("degrade", 1);

        // Hold the single permit outside, then every attempt must be rejected.
        ConcurrencyLimiter.Permit held = limiter.tryAcquire();
        assertNotNull(held);
        AtomicInteger calls = new AtomicInteger();
        DegradedResult<String> result = engine().execute(List.of(
                task("a", executor, budget, () -> {
                    calls.incrementAndGet();
                    return "1";
                }),
                task("b", executor, budget, () -> {
                    calls.incrementAndGet();
                    return "2";
                })), budget, POLICY, limiter);
        held.close();

        assertEquals(StopReason.CONCURRENCY_LIMITED, result.stopReason());
        assertEquals(DegradeStatus.TOTAL_FAILURE, result.status());
        assertEquals(0, calls.get(), "capped work never runs or queues");
        assertEquals(TerminationReason.REJECTED_CONCURRENCY_LIMIT,
                result.incomplete().get(0).reason());
    }

    @Test
    void sameInputsYieldSameConclusionRegardlessOfOrder() {
        // The immediate executor runs in submission order, but the assertion
        // is on declaration-order determinism of the aggregate.
        List<DegradedResult<String>> runs = new ArrayList<>();
        for (int run = 0; run < 5; run++) {
            MutableClock clock = new MutableClock();
            RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
            ImmediateExecutor executor = ImmediateExecutor.create();
            AtomicInteger flaky = new AtomicInteger();
            runs.add(engine().execute(List.of(
                    task("z-ok", executor, budget, () -> "z"),
                    task("a-bad", executor, budget, () -> {
                        throw new IllegalStateException("x");
                    }),
                    task("m-flaky", executor, budget, () -> {
                        if (flaky.incrementAndGet() < 2) {
                            throw new IllegalStateException("transient");
                        }
                        return "m";
                    })), budget, POLICY, null));
        }
        for (DegradedResult<String> r : runs) {
            assertEquals(DegradeStatus.PARTIAL_SUCCESS, r.status());
            assertEquals(List.of("z-ok", "m-flaky"), r.completedNames(),
                    "completed order is declaration order");
            assertEquals(List.of("a-bad"), r.incompleteNames());
            assertEquals(StopReason.RETRIES_EXHAUSTED, r.stopReason());
        }
        assertEquals(runs.get(0).toResponseBody(), runs.get(1).toResponseBody());
        assertEquals(runs.get(0).toResponseBody(), runs.get(4).toResponseBody());
    }
}
