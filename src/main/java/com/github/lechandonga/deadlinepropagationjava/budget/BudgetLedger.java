package com.github.lechandonga.deadlinepropagationjava.budget;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 预算流水：记录各阶段消耗的预算、终止原因、工作是否真正停止。
 *
 * <p>仅记录阶段名与耗时等元数据，不记录任何敏感请求内容。
 * 线程安全：扇出并发场景下多个子任务可同时写入。
 */
public final class BudgetLedger {

    public record StageEntry(String stage, long consumedMillis, long remainingMillis) {}

    public record TerminationEntry(TerminationReason reason, boolean workStopped) {}

    private final long startNanos = System.nanoTime();
    private final List<StageEntry> stages = new CopyOnWriteArrayList<>();
    private final AtomicReference<TerminationEntry> termination = new AtomicReference<>();

    /** 记录一个阶段的累计消耗与当时的剩余预算。 */
    public void recordStage(String stage, long consumedMillis, long remainingMillis) {
        stages.add(new StageEntry(stage, consumedMillis, remainingMillis));
    }

    /** 记录终止结论；仅首个记录生效，保证终止依据唯一。 */
    public void recordTermination(TerminationReason reason, boolean workStopped) {
        termination.compareAndSet(null, new TerminationEntry(reason, workStopped));
    }

    public List<StageEntry> stageEntries() {
        return List.copyOf(stages);
    }

    public TerminationEntry termination() {
        return termination.get();
    }

    /** 自上下文创建起累计消耗的毫秒数。 */
    public long consumedMillis() {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
