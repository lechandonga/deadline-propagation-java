package com.github.lechandonga.deadlinepropagationjava.exec;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executor wrapper that propagates the request budget across threads,
 * refuses to queue work when no budget remains, interrupts in-flight work on
 * termination, and always restores the pooled thread's previous context.
 *
 * <p>Rejection is loud: submissions after termination fail immediately with
 * {@link BudgetExhaustedException} / {@link RequestCancelledException} instead
 * of silently queuing.
 */
public final class BudgetAwareExecutor implements Executor, AutoCloseable {

    private final ExecutorService delegate;
    private final Set<TrackedTask<?>> inFlight = ConcurrentHashMap.newKeySet();

    private BudgetAwareExecutor(ExecutorService delegate) {
        this.delegate = delegate;
    }

    public static BudgetAwareExecutor wrap(ExecutorService delegate) {
        return new BudgetAwareExecutor(delegate);
    }

    /** Number of tasks currently running or queued; must return to 0 after a request ends. */
    public int inFlightCount() {
        return inFlight.size();
    }

    @Override
    public void execute(Runnable command) {
        RequestBudget budget = BudgetContext.current();
        if (budget != null) {
            budget.checkAlive(); // never queue work for a dead request
        }
        delegate.execute(wrap(command, budget));
    }

    /**
     * Submits a task bound to the given budget. The task sees the budget via
     * {@link BudgetContext}; on termination its future is cancelled with
     * interruption so blocked work stops holding threads, connections or locks.
     */
    public <T> CompletableFuture<T> submit(Callable<T> task, RequestBudget budget) {
        // Loud rejection: no silent queuing once the request is over.
        TerminationReason reason = budget.terminationReason();
        if (reason == TerminationReason.DEADLINE_EXCEEDED) {
            throw new BudgetExhaustedException("refusing to queue work: budget exhausted");
        }
        if (reason != TerminationReason.NONE) {
            throw new RequestCancelledException("refusing to queue work: " + reason, reason);
        }

        TrackedTask<T> tracked = new TrackedTask<>(task, budget);
        inFlight.add(tracked);
        try {
            tracked.future = delegate.submit(tracked);
        } catch (RuntimeException e) {
            tracked.unregister();
            inFlight.remove(tracked);
            throw e;
        }
        tracked.result.whenComplete((v, err) -> {
            tracked.unregister();
            inFlight.remove(tracked);
        });
        return tracked.result;
    }

    private Runnable wrap(Runnable command, RequestBudget budget) {
        return () -> {
            if (budget == null) {
                command.run();
                return;
            }
            try (BudgetContext.Scope ignored = BudgetContext.enter(budget)) {
                command.run();
            }
        };
    }

    @Override
    public void close() {
        delegate.shutdown();
    }

    private final class TrackedTask<T> implements Callable<T> {
        private final Callable<T> task;
        private final RequestBudget budget;
        private final CompletableFuture<T> result = new CompletableFuture<>();
        private final AtomicBoolean done = new AtomicBoolean();
        private volatile Future<?> future;
        private volatile AutoCloseable listener;

        TrackedTask(Callable<T> task, RequestBudget budget) {
            this.task = task;
            this.budget = budget;
            // Termination interrupts the in-flight task and completes the
            // result with the real reason so callers can distinguish causes.
            this.listener = budget.onTerminate(() -> {
                if (done.compareAndSet(false, true)) {
                    Future<?> f = future;
                    if (f != null) {
                        f.cancel(true);
                    }
                    TerminationReason r = budget.terminationReason();
                    if (r == TerminationReason.DEADLINE_EXCEEDED) {
                        result.completeExceptionally(
                                new BudgetExhaustedException("budget exhausted while running"));
                    } else {
                        result.completeExceptionally(
                                new RequestCancelledException("cancelled while running: " + r, r));
                    }
                }
            });
        }

        void unregister() {
            AutoCloseable l = listener;
            if (l != null) {
                try {
                    l.close();
                } catch (Exception ignored) {
                }
            }
        }

        @Override
        public T call() throws Exception {
            try (BudgetContext.Scope ignored = BudgetContext.enter(budget)) {
                budget.checkAlive();
                T value = task.call();
                if (done.compareAndSet(false, true)) {
                    result.complete(value);
                }
                return value;
            } catch (Throwable t) {
                if (done.compareAndSet(false, true)) {
                    result.completeExceptionally(t);
                }
                throw t;
            }
            // Scope.close() restores the pooled thread's previous context here.
        }
    }
}
