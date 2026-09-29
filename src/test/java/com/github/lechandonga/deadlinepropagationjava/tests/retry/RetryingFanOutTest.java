package com.github.lechandonga.deadlinepropagationjava.tests.retry;

import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.BudgetAwareExecutor;
import com.github.lechandonga.deadlinepropagationjava.retry.ConcurrencyLimiter;
import com.github.lechandonga.deadlinepropagationjava.retry.DegradedResult;
import com.github.lechandonga.deadlinepropagationjava.retry.ItemResult;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryPolicy;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryScheduler;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryingFanOut;
import com.github.lechandonga.deadlinepropagationjava.retry.StopReason;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryingFanOutTest {

    private BudgetAwareExecutor executor;
    private RetryingFanOut fanOut;

    @BeforeEach
    void setUp() {
        executor = BudgetAwareExecutor.wrap(Executors.newFixedThreadPool(4));
        fanOut = new RetryingFanOut(executor, RetryScheduler.immediate());
    }

    @AfterEach
    void tearDown() {
        executor.close();
    }

    static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-09-29T00:00:00Z");

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static RetryPolicy policy(int maxAttempts) {
        return new RetryPolicy(maxAttempts, Duration.ZERO, Duration.ofMillis(1));
    }

    @Test
    void succeedsOnRetryAndNeverRunsConfirmedWorkAgain() {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(30));
        AtomicInteger confirmedRuns = new AtomicInteger();
        AtomicInteger flakyRuns = new AtomicInteger();

        DegradedResult<String> result = fanOut.execute(budget, policy(3),
                ConcurrencyLimiter.bounded("e", 0), List.of(
                        new RetryingFanOut.NamedWork<>("confirmed", () -> {
                            confirmedRuns.incrementAndGet();
                            return "ok";
                        }),
                        new RetryingFanOut.NamedWork<>("flaky", () -> {
                            if (flakyRuns.incrementAndGet() == 1) {
                                throw new IllegalStateException("transient");
                            }
                            return "recovered";
                        })));

        assertEquals(DegradedResult.State.ALL_SUCCESS, result.state());
        assertEquals(1, confirmedRuns.get(), "confirmed side effects run exactly once");
        assertEquals(2, flakyRuns.get());
        assertEquals(StopReason.NONE, result.stopReason());
        assertEquals(1, result.items().get(0).attempts());
        assertEquals(2, result.items().get(1).attempts());
        assertTrue(result.unfinished().isEmpty());
    }

    @Test
    void retriesAreBoundedByMaxAttempts() {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(30));
        AtomicInteger runs = new AtomicInteger();

        DegradedResult<String> result = fanOut.execute(budget, policy(3),
                ConcurrencyLimiter.bounded("e", 0), List.of(
                        new RetryingFanOut.NamedWork<>("always-bad", () -> {
                            runs.incrementAndGet();
                            throw new IllegalStateException("boom");
                        })));

        assertEquals(3, runs.get(), "attempts are capped");
        assertEquals(DegradedResult.State.FAILED, result.state());
        assertEquals(StopReason.RETRY_LIMIT_REACHED, result.stopReason());
        ItemResult<String> item = result.items().get(0);
        assertEquals(ItemResult.Status.FAILED, item.status());
        assertEquals(List.of("always-bad"), result.unfinished());
    }

    @Test
    void partialSuccessDegradesInsteadOfFailingTheRequest() {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(30));

        DegradedResult<String> result = fanOut.execute(budget, policy(2),
                ConcurrencyLimiter.bounded("e", 0), List.of(
                        new RetryingFanOut.NamedWork<>("done", () -> "value"),
                        new RetryingFanOut.NamedWork<>("broken", () -> {
                            throw new IllegalStateException("boom");
                        })));

        assertEquals(DegradedResult.State.PARTIAL_SUCCESS, result.state());
        assertEquals(List.of("done"), result.completed(),
                "degraded answer keeps confirmed work and marks it unsafe to retry");
        assertEquals(List.of("broken"), result.unfinished());
        assertEquals(StopReason.RETRY_LIMIT_REACHED, result.stopReason());
    }

    @Test
    void threeStatesAreDistinguishable() {
        RequestBudget allOkBudget = RequestBudget.newBudget(Duration.ofSeconds(30));
        DegradedResult<String> allOk = fanOut.execute(allOkBudget, policy(1),
                ConcurrencyLimiter.bounded("e", 0),
                List.of(new RetryingFanOut.NamedWork<>("a", () -> "1")));
        assertEquals(DegradedResult.State.ALL_SUCCESS, allOk.state());

        RequestBudget partialBudget = RequestBudget.newBudget(Duration.ofSeconds(30));
        DegradedResult<String> partial = fanOut.execute(partialBudget, policy(1),
                ConcurrencyLimiter.bounded("e", 0), List.of(
                        new RetryingFanOut.NamedWork<>("a", () -> "1"),
                        new RetryingFanOut.NamedWork<>("b", () -> {
                            throw new IllegalStateException("x");
                        })));
        assertEquals(DegradedResult.State.PARTIAL_SUCCESS, partial.state());

        RequestBudget failedBudget = RequestBudget.newBudget(Duration.ofSeconds(30));
        DegradedResult<String> failed = fanOut.execute(failedBudget, policy(1),
                ConcurrencyLimiter.bounded("e", 0),
                List.of(new RetryingFanOut.NamedWork<>("a", () -> {
                    throw new IllegalStateException("x");
                })));
        assertEquals(DegradedResult.State.FAILED, failed.state());
    }

    @Test
    void noRetryAfterCancellationEvenWithAttemptsLeft() throws Exception {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(30));
        CountDownLatch firstAttemptStarted = new CountDownLatch(1);
        AtomicInteger runs = new AtomicInteger();

        // Backoff completes only when the budget terminates: the retry is
        // parked in waiting when cancellation arrives, so no later attempt
        // can possibly start.
        RetryScheduler gateScheduler = (b, delay) -> {
            CompletableFuture<Void> ready = new CompletableFuture<>();
            b.onTerminate(() -> ready.complete(null));
            return ready;
        };
        RetryingFanOut gatedFanOut = new RetryingFanOut(executor, gateScheduler);
        CompletableFuture<DegradedResult<String>> future = CompletableFuture.supplyAsync(() ->
                gatedFanOut.execute(budget, policy(5), ConcurrencyLimiter.bounded("e", 0),
                        List.of(new RetryingFanOut.NamedWork<>("w", () -> {
                            runs.incrementAndGet();
                            firstAttemptStarted.countDown();
                            throw new IllegalStateException("boom");
                        }))));

        assertTrue(firstAttemptStarted.await(2, TimeUnit.SECONDS));
        Thread.sleep(50); // let the failed attempt settle into backoff waiting
        budget.cancel(TerminationReason.CLIENT_CANCELLED);

        DegradedResult<String> result = future.get(2, TimeUnit.SECONDS);
        assertEquals(1, runs.get(), "no retry ever starts after caller cancellation");
        assertEquals(StopReason.CLIENT_CANCELLED, result.stopReason());
        assertEquals(ItemResult.Status.CANCELLED, result.items().get(0).status());
        assertEquals(DegradedResult.State.FAILED, result.state());
    }

    @Test
    void exhaustedBudgetStopsRetriesAndClassifiesTimeout() {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofMillis(30));

        DegradedResult<String> result = fanOut.execute(budget, policy(5),
                ConcurrencyLimiter.bounded("e", 0), List.of(
                        new RetryingFanOut.NamedWork<>("slow", () -> {
                            Thread.sleep(150);
                            return "never";
                        })));

        assertEquals(StopReason.BUDGET_EXHAUSTED, result.stopReason());
        assertEquals(ItemResult.Status.TIMEOUT, result.items().get(0).status());
    }

    @Test
    void remainingBelowThresholdSkipsTheAttempt() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofMillis(100), clock);
        clock.advance(Duration.ofMillis(90)); // 10ms left

        RetryPolicy policy = new RetryPolicy(3, Duration.ZERO, Duration.ofMillis(50));
        AtomicInteger runs = new AtomicInteger();
        DegradedResult<String> result = fanOut.execute(budget, policy,
                ConcurrencyLimiter.bounded("e", 0), List.of(
                        new RetryingFanOut.NamedWork<>("w", () -> {
                            runs.incrementAndGet();
                            return "x";
                        })));

        assertEquals(0, runs.get(), "an attempt that cannot pay startup cost must not start");
        assertEquals(StopReason.REMAINING_BELOW_THRESHOLD, result.stopReason());
        assertEquals(ItemResult.Status.TIMEOUT, result.items().get(0).status());
    }

    @Test
    void concurrencyCapDegradesImmediatelyInsteadOfQueuing() throws InterruptedException {
        // One permit total; the second item is rejected up front, never queued.
        ConcurrencyLimiter limiter = ConcurrencyLimiter.bounded("e", 1);
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        CountDownLatch accepted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        RetryingFanOut gatingFanOut = new RetryingFanOut(executor, RetryScheduler.immediate());

        CompletableFuture<DegradedResult<String>> future = CompletableFuture.supplyAsync(() ->
                gatingFanOut.execute(RequestBudget.newBudget(Duration.ofSeconds(30)), policy(1),
                        limiter, List.of(
                                new RetryingFanOut.NamedWork<>("a", () -> {
                                    accepted.countDown();
                                    release.await();
                                    return "a";
                                }),
                                new RetryingFanOut.NamedWork<>("b", () -> {
                                    try {
                                        observeConcurrency(concurrent, maxObserved);
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return "b";
                                }),
                                new RetryingFanOut.NamedWork<>("c", () -> {
                                    try {
                                        observeConcurrency(concurrent, maxObserved);
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return "c";
                                }))));

        assertTrue(accepted.await(2, TimeUnit.SECONDS), "first item acquired the single permit");
        // While the permit is held, the cap decision is already final; let it finish.
        release.countDown();
        DegradedResult<String> result = future.join();
        assertTrue(maxObserved.get() <= 1, "downstream never sees more than the cap");
        assertTrue(result.unfinished().size() >= 1,
                "at least one item degraded at the cap");
        assertEquals(StopReason.CONCURRENCY_LIMITED, result.stopReason());
        ItemResult<String> rejected = result.items().stream()
                .filter(i -> i.status() == ItemResult.Status.REJECTED).findFirst().orElseThrow();
        assertEquals(0, rejected.attempts(), "rejected work never ran");
    }

    private static void observeConcurrency(AtomicInteger concurrent, AtomicInteger maxObserved)
            throws InterruptedException {
        int now = concurrent.incrementAndGet();
        maxObserved.accumulateAndGet(now, Math::max);
        Thread.sleep(50);
        concurrent.decrementAndGet();
    }

    @Test
    void sameInputsYieldSameConclusionRegardlessOfTiming() throws Exception {
        // Run many times with varied scheduling; set-only aggregation must
        // always produce the same state, stop reason and item classification.
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(30));
        DegradedResult<String> first = fanOut.execute(budget, policy(1),
                ConcurrencyLimiter.bounded("e", 0), List.of(
                        new RetryingFanOut.NamedWork<>("ok", () -> "v"),
                        new RetryingFanOut.NamedWork<>("bad", () -> {
                            throw new IllegalStateException("x");
                        })));

        for (int run = 0; run < 20; run++) {
            final int scheduleSeed = run;
            DegradedResult<String> other = fanOut.execute(
                    RequestBudget.newBudget(Duration.ofSeconds(30)), policy(1),
                    ConcurrencyLimiter.bounded("e", 0), List.of(
                            new RetryingFanOut.NamedWork<>("ok", () -> {
                                Thread.sleep(scheduleSeed % 5);
                                return "v";
                            }),
                            new RetryingFanOut.NamedWork<>("bad", () -> {
                                Thread.sleep((scheduleSeed * 3) % 7);
                                throw new IllegalStateException("x");
                            })));
            assertEquals(first.state(), other.state());
            assertEquals(first.stopReason(), other.stopReason());
            assertEquals(first.completed(), other.completed());
            assertEquals(first.unfinished(), other.unfinished());
        }
    }
}
