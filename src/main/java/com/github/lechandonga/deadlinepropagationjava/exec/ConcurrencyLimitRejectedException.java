package com.github.lechandonga.deadlinepropagationjava.exec;

import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;

/**
 * Thrown when work is rejected because the entry's in-flight concurrency cap
 * was reached. The work never ran and never queued; callers degrade with
 * {@link TerminationReason#REJECTED_CONCURRENCY_LIMIT} instead of retrying.
 */
public class ConcurrencyLimitRejectedException extends RuntimeException {
    public ConcurrencyLimitRejectedException(String message) {
        super(message);
    }
}
