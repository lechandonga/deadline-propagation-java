package com.github.lechandonga.deadlinepropagationjava.budget;

/** Thrown when work is attempted on a cancelled request. */
public class RequestCancelledException extends RuntimeException {
    private final TerminationReason reason;

    public RequestCancelledException(String message, TerminationReason reason) {
        super(message);
        this.reason = reason;
    }

    public TerminationReason reason() {
        return reason;
    }
}
