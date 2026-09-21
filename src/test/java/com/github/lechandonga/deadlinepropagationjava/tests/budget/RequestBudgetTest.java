package com.github.lechandonga.deadlinepropagationjava.tests.budget;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RequestBudgetTest {

    /** Deterministic clock for budget tests. */
    static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-09-21T00:00:00Z");

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

    @Test
    void budgetOnlyDecreasesAndNeverRegains() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10), clock);

        Duration previous = budget.remaining();
        assertEquals(Duration.ofSeconds(10), previous);

        for (int i = 0; i < 10; i++) {
            clock.advance(Duration.ofSeconds(1));
            Duration current = budget.remaining();
            assertTrue(current.compareTo(previous) < 0,
                    "remaining must strictly decrease: " + previous + " -> " + current);
            previous = current;
        }
        // Fully consumed: stays at zero, never negative, never resets.
        assertEquals(Duration.ZERO, budget.remaining());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(Duration.ZERO, budget.remaining());
        assertTrue(budget.consumed().compareTo(budget.total()) <= 0);
    }

    @Test
    void expiryTerminatesWithDeadlineExceeded() {
        MutableClock clock = new MutableClock();
        RequestBudget budget = RequestBudget.newBudget(Duration.ofMillis(100), clock);
        AtomicInteger terminations = new AtomicInteger();
        budget.onTerminate(terminations::incrementAndGet);

        clock.advance(Duration.ofMillis(150));
        assertTrue(budget.isExpired());
        assertEquals(TerminationReason.DEADLINE_EXCEEDED, budget.terminationReason());
        assertEquals(1, terminations.get(), "termination listeners fire exactly once");
        assertThrows(BudgetExhaustedException.class, budget::checkAlive);
    }

    @Test
    void cancellationIsTerminalAndFirstReasonWins() {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(30));
        budget.cancel(TerminationReason.CLIENT_CANCELLED);
        budget.cancel(TerminationReason.CANCELLED); // ignored: first reason wins

        assertTrue(budget.isCancelled());
        assertEquals(TerminationReason.CLIENT_CANCELLED, budget.terminationReason());

        RequestCancelledException e = assertThrows(RequestCancelledException.class, budget::checkAlive);
        assertEquals(TerminationReason.CLIENT_CANCELLED, e.reason());
    }

    @Test
    void deadlineAndCancellationAreDistinguishable() {
        MutableClock clock = new MutableClock();
        RequestBudget expired = RequestBudget.newBudget(Duration.ofMillis(10), clock);
        clock.advance(Duration.ofSeconds(1));
        assertThrows(BudgetExhaustedException.class, expired::checkAlive);

        RequestBudget cancelled = RequestBudget.newBudget(Duration.ofSeconds(10), clock);
        cancelled.cancel(TerminationReason.CANCELLED);
        assertThrows(RequestCancelledException.class, cancelled::checkAlive);
    }

    @Test
    void listenerRegisteredAfterTerminationRunsImmediately() {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10));
        budget.cancel(TerminationReason.CANCELLED);
        AtomicInteger calls = new AtomicInteger();
        budget.onTerminate(calls::incrementAndGet);
        assertEquals(1, calls.get());
    }
}
