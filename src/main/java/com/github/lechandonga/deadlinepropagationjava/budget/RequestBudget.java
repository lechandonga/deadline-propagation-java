package com.github.lechandonga.deadlinepropagationjava.budget;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-request time budget. The budget only ever decreases: it is anchored to
 * an absolute deadline, so nested stages and child tasks can never regain a
 * full budget. Cancellation is terminal and propagates to registered work.
 *
 * <p>Thread-safe; instances are shared across the threads serving one request
 * and must never be reused across requests.
 */
public final class RequestBudget {

    /**
     * Actively promotes wall-clock expiry to {@link TerminationReason#DEADLINE_EXCEEDED}
     * at the deadline, so sleeping or blocked work is interrupted even when no
     * stage happens to call {@link #checkAlive()} at that moment.
     */
    private static final ScheduledExecutorService ENFORCER = Executors.newSingleThreadScheduledExecutor(
            runnable -> {
                Thread thread = new Thread(runnable, "budget-deadline-enforcer");
                thread.setDaemon(true);
                return thread;
            });

    private final Clock clock;
    private final Instant createdAt;
    private final Instant deadline;
    private final Duration total;
    private final AtomicReference<TerminationReason> termination =
            new AtomicReference<>(TerminationReason.NONE);
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    private RequestBudget(Clock clock, Duration budget) {
        if (budget.isNegative() || budget.isZero()) {
            throw new IllegalArgumentException("budget must be positive: " + budget);
        }
        this.clock = clock;
        this.createdAt = clock.instant();
        this.deadline = createdAt.plus(budget);
        this.total = budget;
    }

    public static RequestBudget newBudget(Duration budget) {
        RequestBudget requestBudget = new RequestBudget(Clock.systemUTC(), budget);
        // Active enforcement: expiry fires even if no one is checking.
        ENFORCER.schedule(() -> requestBudget.terminate(TerminationReason.DEADLINE_EXCEEDED),
                budget.toNanos(), TimeUnit.NANOSECONDS);
        return requestBudget;
    }

    /**
     * Test hook: budget driven by an explicit clock. Expiry is promoted
     * lazily (on observation) only; no wall-clock enforcer is attached.
     */
    public static RequestBudget newBudget(Duration budget, Clock clock) {
        return new RequestBudget(clock, budget);
    }

    /** Remaining time, clamped at zero. Monotonically non-increasing. */
    public Duration remaining() {
        Duration left = Duration.between(clock.instant(), deadline);
        if (left.isNegative()) {
            left = Duration.ZERO;
        }
        if (left.isZero()) {
            terminate(TerminationReason.DEADLINE_EXCEEDED);
        }
        return left;
    }

    public Instant deadline() {
        return deadline;
    }

    public Duration total() {
        return total;
    }

    /** Budget actually consumed so far; never exceeds {@link #total()}. */
    public Duration consumed() {
        Duration used = Duration.between(createdAt, clock.instant());
        return used.compareTo(total) > 0 ? total : used;
    }

    public boolean isExpired() {
        return remaining().isZero();
    }

    public boolean isCancelled() {
        TerminationReason r = termination.get();
        return r != TerminationReason.NONE && r != TerminationReason.DEADLINE_EXCEEDED;
    }

    /** Terminal reason, or {@link TerminationReason#NONE} while still alive. */
    public TerminationReason terminationReason() {
        remaining(); // lazily promote wall-clock expiry to DEADLINE_EXCEEDED
        return termination.get();
    }

    /**
     * Throws if the budget is expired or cancelled; otherwise no-op.
     * Call at stage boundaries so stale work stops instead of continuing.
     */
    public void checkAlive() {
        TerminationReason r = terminationReason();
        switch (r) {
            case NONE -> {
                return;
            }
            case DEADLINE_EXCEEDED ->
                    throw new BudgetExhaustedException("request budget exhausted at " + deadline);
            default ->
                    throw new RequestCancelledException("request terminated: " + r, r);
        }
    }

    /**
     * Terminates the budget with the given reason. First reason wins so the
     * outcome is deterministic; later calls are ignored.
     */
    public void cancel(TerminationReason reason) {
        if (reason == TerminationReason.NONE || reason == TerminationReason.DEADLINE_EXCEEDED) {
            throw new IllegalArgumentException("not a cancellation reason: " + reason);
        }
        terminate(reason);
    }

    private void terminate(TerminationReason reason) {
        if (termination.compareAndSet(TerminationReason.NONE, reason)) {
            for (Runnable listener : listeners) {
                try {
                    listener.run();
                } catch (Throwable ignored) {
                    // A misbehaving listener must not block termination of others.
                }
            }
        }
    }

    /**
     * Registers a callback invoked at most once when the budget terminates
     * (expiry or cancellation). If already terminated, runs immediately.
     * Closing the returned handle unregisters the callback.
     */
    public AutoCloseable onTerminate(Runnable callback) {
        listeners.add(callback);
        if (terminationReason() != TerminationReason.NONE && listeners.remove(callback)) {
            callback.run();
            return () -> {
            };
        }
        return () -> listeners.remove(callback);
    }
}
