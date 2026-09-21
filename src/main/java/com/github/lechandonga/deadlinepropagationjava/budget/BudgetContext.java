package com.github.lechandonga.deadlinepropagationjava.budget;

/**
 * Thread-local carrier for the current request's budget. Scopes must be
 * closed (try-with-resources) so pooled threads are restored to their exact
 * prior state and requests never observe each other's budgets.
 */
public final class BudgetContext {

    private static final ThreadLocal<RequestBudget> CURRENT = new ThreadLocal<>();

    private BudgetContext() {
    }

    /** The budget bound to this thread, or {@code null} when outside a request. */
    public static RequestBudget current() {
        return CURRENT.get();
    }

    /** The budget bound to this thread; throws when absent. */
    public static RequestBudget requireCurrent() {
        RequestBudget budget = CURRENT.get();
        if (budget == null) {
            throw new IllegalStateException("no request budget bound to thread "
                    + Thread.currentThread().getName());
        }
        return budget;
    }

    /**
     * Installs the budget on the current thread. Closing the returned scope
     * restores the previous binding (or clears it), never merely clearing.
     */
    public static Scope enter(RequestBudget budget) {
        RequestBudget previous = CURRENT.get();
        CURRENT.set(budget);
        return new Scope(previous);
    }

    public static final class Scope implements AutoCloseable {
        private final RequestBudget previous;
        private boolean closed;

        private Scope(RequestBudget previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }
}
