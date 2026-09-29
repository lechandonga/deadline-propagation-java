package com.github.lechandonga.deadlinepropagationjava.tests.support;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.exec.BudgetAwareExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * Runs every submission synchronously on the calling thread, so retry tests
 * stay deterministic without real waits, networking or external services.
 */
public final class ImmediateExecutor extends BudgetAwareExecutor {

    private ImmediateExecutor() {
        super(Executors.newCachedThreadPool());
    }

    public static ImmediateExecutor create() {
        return new ImmediateExecutor();
    }

    @Override
    public void close() {
        // No real pool is used.
    }

    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task, RequestBudget budget) {
        try (BudgetContext.Scope ignored = BudgetContext.enter(budget)) {
            return CompletableFuture.completedFuture(task.call());
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }
}
