package com.github.lechandonga.deadlinepropagationjava.stream;

import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Deterministic head of a budgeted stream. Bytes already written are never
 * repeated and no new content appears after termination; the stream ends
 * with exactly one fixed terminator carrying the real stop reason.
 *
 * <p>Once {@link #finish()} ran (or the client disconnected), every further
 * {@link #writeLine(String)} is a no-op, so callers cannot accidentally
 * double-write the terminator or append content after it.
 */
public final class BudgetedStreamEmitter {

    public static final String TERMINATOR_PREFIX = "X-STREAM-TERMINATED:";

    private final OutputStream output;
    private final RequestBudget budget;
    private final AtomicBoolean finished = new AtomicBoolean();
    private volatile TerminationReason finishReason = TerminationReason.NONE;

    public BudgetedStreamEmitter(OutputStream output, RequestBudget budget) {
        this.output = output;
        this.budget = budget;
    }

    /** Writes one line if the stream is still open; returns false after termination. */
    public boolean writeLine(String line) throws IOException {
        if (finished.get() || budget.terminationReason() != TerminationReason.NONE) {
            return false;
        }
        output.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        output.flush();
        return true;
    }

    /**
     * Emits the single terminator exactly once and marks the stream closed.
     * A client disconnect ({@link IOException}) marks the stream closed
     * without inventing bytes the client will never receive.
     *
     * @return true if the terminator was written by this call
     */
    public boolean finish(TerminationReason reason) throws IOException {
        if (!finished.compareAndSet(false, true)) {
            return false;
        }
        finishReason = reason;
        try {
            output.write((TERMINATOR_PREFIX + reason.name() + "\n")
                    .getBytes(StandardCharsets.UTF_8));
            output.flush();
            return true;
        } catch (IOException clientGone) {
            if (budget.terminationReason() == TerminationReason.NONE) {
                budget.cancel(TerminationReason.CLIENT_CANCELLED);
            }
            finishReason = budget.terminationReason();
            throw clientGone;
        }
    }

    public boolean isFinished() {
        return finished.get();
    }

    public TerminationReason finishReason() {
        return finishReason;
    }
}
