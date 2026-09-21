package com.github.lechandonga.deadlinepropagationjava.budget;

/** Thrown when work is attempted after the request budget is exhausted. */
public class BudgetExhaustedException extends RuntimeException {
    public BudgetExhaustedException(String message) {
        super(message);
    }
}
