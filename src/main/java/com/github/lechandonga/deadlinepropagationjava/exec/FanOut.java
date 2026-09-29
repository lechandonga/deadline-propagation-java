package com.github.lechandonga.deadlinepropagationjava.exec;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Combines concurrent child tasks into one deterministic outcome when some
 * succeed, some time out and some fail. Completed work is recorded so retries
 * never redo confirmed side effects.
 */
public final class FanOut {

    private FanOut() {
    }

    /**
     * Starts all tasks and completes when every one of them has reached a
     * terminal state. The result never fails: each child is classified
     * exactly once, so the aggregate conclusion is unique and deterministic.
     */
    public static <T> CompletableFuture<FanOutResult<T>> gather(List<NamedTask<T>> tasks) {
        List<CompletableFuture<Outcome<T>>> futures = new ArrayList<>(tasks.size());
        for (NamedTask<T> named : tasks) {
            CompletableFuture<T> future;
            try {
                future = named.task().get();
            } catch (Throwable t) {
                future = CompletableFuture.failedFuture(t);
            }
            futures.add(future.handle((value, error) -> classify(named.name(), value, error)));
        }
        return CompletableFuture
                .allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> {
                    List<Outcome<T>> outcomes = new ArrayList<>(futures.size());
                    for (CompletableFuture<Outcome<T>> f : futures) {
                        outcomes.add(f.join());
                    }
                    return new FanOutResult<>(List.copyOf(outcomes));
                });
    }

    private static <T> Outcome<T> classify(String name, T value, Throwable error) {
        if (error == null) {
            return new Outcome<>(name, Outcome.Status.SUCCESS, value, TerminationReason.NONE, null);
        }
        Throwable cause = unwrap(error);
        if (cause instanceof BudgetExhaustedException) {
            return new Outcome<>(name, Outcome.Status.TIMEOUT, null,
                    TerminationReason.DEADLINE_EXCEEDED, cause);
        }
        if (cause instanceof ConcurrencyLimitRejectedException) {
            return new Outcome<>(name, Outcome.Status.REJECTED, null,
                    TerminationReason.REJECTED_CONCURRENCY_LIMIT, cause);
        }
        if (cause instanceof RequestCancelledException cancelled) {
            return new Outcome<>(name, Outcome.Status.CANCELLED, null,
                    cancelled.reason(), cause);
        }
        return new Outcome<>(name, Outcome.Status.FAILED, null, TerminationReason.ERROR, cause);
    }

    private static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    public record NamedTask<T>(String name, Supplier<CompletableFuture<T>> task) {
    }

    public record Outcome<T>(String name, Status status, T value, TerminationReason reason,
                             Throwable error, int attempts) {
        public enum Status {SUCCESS, TIMEOUT, FAILED, CANCELLED, REJECTED}

        public Outcome(String name, Status status, T value, TerminationReason reason,
                       Throwable error) {
            this(name, status, value, reason, error, 1);
        }
    }

    /**
     * Deterministic aggregate: {@link #completed()} holds confirmed side
     * effects that must never be retried; {@link #retryable()} holds only
     * work that did not confirm completion.
     */
    public record FanOutResult<T>(List<Outcome<T>> outcomes) {
        /** Tasks that did not confirm completion; only these may be retried. */
        public List<String> retryable() {
            // REJECTED work never ran due to the concurrency cap and must
            // degrade immediately rather than be retried or queued.
            return namesWhere(s -> s != Outcome.Status.SUCCESS
                    && s != Outcome.Status.REJECTED);
        }

        /** Tasks confirmed complete; retrying these would duplicate side effects. */
        public List<String> completed() {
            return namesWhere(s -> s == Outcome.Status.SUCCESS);
        }

        public boolean allSucceeded() {
            return outcomes.stream().allMatch(o -> o.status() == Outcome.Status.SUCCESS);
        }

        private List<String> namesWhere(java.util.function.Predicate<Outcome.Status> predicate) {
            List<String> names = new ArrayList<>();
            for (Outcome<T> outcome : outcomes) {
                if (predicate.test(outcome.status())) {
                    names.add(outcome.name());
                }
            }
            return List.copyOf(names);
        }
    }
}
