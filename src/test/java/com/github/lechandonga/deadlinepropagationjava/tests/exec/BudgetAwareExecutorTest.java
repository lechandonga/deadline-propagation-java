package com.github.lechandonga.deadlinepropagationjava.tests.exec;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.BudgetAwareExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BudgetAwareExecutorTest {

    private BudgetAwareExecutor executor;

    @BeforeEach
    void setUp() {
        executor = BudgetAwareExecutor.wrap(Executors.newFixedThreadPool(2));
    }

    @AfterEach
    void tearDown() {
        executor.close();
    }

    @Test
    void rejectsSubmissionAfterBudgetExhausted() throws Exception {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofMillis(50));
        Thread.sleep(120); // let it expire
        assertEquals(TerminationReason.DEADLINE_EXCEEDED, budget.terminationReason());

        assertThrows(BudgetExhaustedException.class,
                () -> executor.submit(() -> "never runs", budget));
        assertEquals(0, executor.inFlightCount(), "rejected work must not queue");
    }

    @Test
    void rejectsSubmissionAfterCancellation() {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(30));
        budget.cancel(TerminationReason.CLIENT_CANCELLED);

        RequestCancelledException e = assertThrows(RequestCancelledException.class,
                () -> executor.submit(() -> "never runs", budget));
        assertEquals(TerminationReason.CLIENT_CANCELLED, e.reason());
        assertEquals(0, executor.inFlightCount());
    }

    @Test
    void cancellationInterruptsInFlightWorkAndLeavesNoResidue() throws Exception {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(30));
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();

        CompletableFuture<String> future = executor.submit(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000); // stands in for uninterruptible-looking work
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            }
            return "should never complete";
        }, budget);

        assertTrue(started.await(2, TimeUnit.SECONDS));
        budget.cancel(TerminationReason.CLIENT_CANCELLED);

        ExecutionException e = assertThrows(ExecutionException.class,
                () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(RequestCancelledException.class, e.getCause());
        assertTrue(interrupted.get(), "in-flight work must be interrupted, not left running");

        // No residual tasks remain tracked after termination.
        for (int i = 0; i < 50 && executor.inFlightCount() != 0; i++) {
            Thread.sleep(20);
        }
        assertEquals(0, executor.inFlightCount(), "no residual tasks after cancellation");
    }

    @Test
    void pooledThreadsDoNotLeakContextBetweenRequests() throws Exception {
        // Request A runs on the pool with budget A.
        RequestBudget budgetA = RequestBudget.newBudget(Duration.ofSeconds(30));
        AtomicReference<RequestBudget> seenByA = new AtomicReference<>();
        executor.submit(() -> {
            seenByA.set(BudgetContext.current());
            return null;
        }, budgetA).get(2, TimeUnit.SECONDS);
        assertSame(budgetA, seenByA.get());

        // Hammer the same pool with context-free tasks: every one must see null.
        for (int i = 0; i < 20; i++) {
            CompletableFuture<RequestBudget> probe = new CompletableFuture<>();
            executor.execute(() -> probe.complete(BudgetContext.current()));
            assertNull(probe.get(2, TimeUnit.SECONDS),
                    "pooled thread leaked budget A into an unrelated task");
        }

        // Request B must see exactly its own budget, never A's.
        RequestBudget budgetB = RequestBudget.newBudget(Duration.ofSeconds(30));
        AtomicReference<RequestBudget> seenByB = new AtomicReference<>();
        executor.submit(() -> {
            seenByB.set(BudgetContext.current());
            return null;
        }, budgetB).get(2, TimeUnit.SECONDS);
        assertSame(budgetB, seenByB.get());
        assertNotSame(budgetA, seenByB.get());
    }

    @Test
    void nestedStagesShareOneShrinkingBudget() throws Exception {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofMillis(500));
        executor.submit(() -> {
            Duration outer = BudgetContext.requireCurrent().remaining();
            Thread.sleep(200);
            // Nested async hop on another thread: same budget, strictly less left.
            return executor.submit(() -> {
                Duration inner = BudgetContext.requireCurrent().remaining();
                assertTrue(inner.compareTo(outer) < 0,
                        "nested stage must see a reduced budget");
                return inner;
            }, budget).get();
        }, budget).get(2, TimeUnit.SECONDS);
    }
}
