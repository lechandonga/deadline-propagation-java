package com.github.lechandonga.deadlinepropagationjava.observability;

import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Per-request observability record: budget consumed per stage, termination
 * reason and whether work actually stopped. Records only stage names and
 * durations — never request payloads or other sensitive content.
 */
public final class BudgetLedger {

    private static final Logger log = LoggerFactory.getLogger(BudgetLedger.class);

    private final List<StageEntry> stages = new CopyOnWriteArrayList<>();
    private volatile TerminationReason terminationReason = TerminationReason.NONE;
    private volatile Boolean workStopped;

    private BudgetLedger() {
    }

    public static BudgetLedger create() {
        return new BudgetLedger();
    }

    /** Records budget consumed by one stage; safe to call from worker threads. */
    public void recordStage(String stage, Duration consumed) {
        stages.add(new StageEntry(stage, consumed));
        log.info("budget stage={} consumedMs={}", stage, consumed.toMillis());
    }

    /** Records the final termination reason and whether work truly stopped. */
    public void recordTermination(TerminationReason reason, boolean workStopped) {
        this.terminationReason = reason;
        this.workStopped = workStopped;
        log.info("budget terminated reason={} workStopped={}", reason, workStopped);
    }

    public TerminationReason terminationReason() {
        return terminationReason;
    }

    /** True only when termination was recorded with confirmed stop. */
    public boolean workStoppedConfirmed() {
        return Boolean.TRUE.equals(workStopped);
    }

    public List<StageEntry> stages() {
        return List.copyOf(stages);
    }

    /** One-line, payload-free summary of the budget flow for this request. */
    public String summary() {
        StringBuilder sb = new StringBuilder("budget-ledger[");
        for (StageEntry stage : stages) {
            sb.append(stage.name()).append('=').append(stage.consumed().toMillis()).append("ms ");
        }
        sb.append("terminated=").append(terminationReason);
        sb.append(" workStopped=").append(workStopped);
        return sb.append(']').toString();
    }

    public record StageEntry(String name, Duration consumed) {
    }
}
