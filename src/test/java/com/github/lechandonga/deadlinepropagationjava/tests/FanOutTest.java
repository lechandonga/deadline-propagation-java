package com.github.lechandonga.deadlinepropagationjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.Deadline;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.ContextPropagatingExecutor;
import com.github.lechandonga.deadlinepropagationjava.fanout.FanOut;
import com.github.lechandonga.deadlinepropagationjava.fanout.FanOut.FanOutResult;
import com.github.lechandonga.deadlinepropagationjava.fanout.FanOut.Outcome;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FanOutTest {

    private ContextPropagatingExecutor executor;

    @BeforeEach
    void setUp() {
        executor = ContextPropagatingExecutor.wrap(Executors.newFixedThreadPool(4));
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void allSucceeded() {
        BudgetContext ctx = BudgetContext.start("req-fanout-ok", Deadline.after(Duration.ofSeconds(10)));
        try (BudgetContext.Scope ignored = ctx.install()) {
            FanOutResult<String> result = FanOut.invokeAll(
                    List.of(() -> "a", () -> "b", () -> "c"), executor);
            assertThat(result.outcome()).isEqualTo(Outcome.ALL_SUCCEEDED);
            assertThat(result.successes()).containsExactlyInAnyOrder("a", "b", "c");
            assertThat(result.completedIndexes()).containsExactlyInAnyOrder(0, 1, 2);
            assertThat(result.retryableIndexes(3)).isEmpty();
        }
    }

    @Test
    void partialFailureGivesDeterministicOutcomeAndSafeRetrySet() {
        BudgetContext ctx = BudgetContext.start("req-fanout-partial", Deadline.after(Duration.ofSeconds(10)));
        try (BudgetContext.Scope ignored = ctx.install()) {
            List<Callable<String>> tasks = List.of(
                    () -> "ok-0",
                    () -> { throw new IllegalStateException("boom"); },
                    () -> "ok-2");
            FanOutResult<String> result = FanOut.invokeAll(tasks, executor);

            // 整体结论唯一确定
            assertThat(result.outcome()).isEqualTo(Outcome.PARTIAL);
            assertThat(result.successes()).containsExactlyInAnyOrder("ok-0", "ok-2");
            assertThat(result.failures()).hasSize(1);
            assertThat(result.failures().get(0).index()).isEqualTo(1);
            assertThat(result.failures().get(0).reasonType())
                    .isEqualTo("IllegalStateException");
            assertThat(result.failures().get(0).terminationReason())
                    .isEqualTo(TerminationReason.INTERNAL_ERROR.name());

            // 重试安全：只包含未确认完成的下标
            assertThat(result.completedIndexes()).containsExactlyInAnyOrder(0, 2);
            assertThat(result.retryableIndexes(3)).containsExactly(1);
        }
    }

    @Test
    void timeoutCancelsRemainingTasksAndCompletedWorkIsNotRetried() throws Exception {
        AtomicInteger started = new AtomicInteger();
        AtomicInteger interrupted = new AtomicInteger();
        BudgetContext ctx = BudgetContext.start("req-fanout-timeout",
                Deadline.after(Duration.ofMillis(300)));
        try (BudgetContext.Scope ignored = ctx.install()) {
            List<Callable<String>> tasks = List.of(
                    () -> { started.incrementAndGet(); return "fast"; },
                    () -> {
                        started.incrementAndGet();
                        try {
                            Thread.sleep(10_000);
                        } catch (InterruptedException e) {
                            interrupted.incrementAndGet();
                            throw e;
                        }
                        return "slow";
                    });
            FanOutResult<String> result = FanOut.invokeAll(tasks, executor);

            // 预算耗尽：整体结论为 TIMED_OUT，唯一确定
            assertThat(result.outcome()).isEqualTo(Outcome.TIMED_OUT);
            // 已完成的副作用被明确界定，不在重试范围内
            assertThat(result.completedIndexes()).containsExactly(0);
            assertThat(result.retryableIndexes(2)).containsExactly(1);
            // 被取消的工作收到中断，不继续占用线程（中断送达是异步的，轮询等待）
            long waitUntil = System.currentTimeMillis() + 3000;
            while (interrupted.get() != 1 && System.currentTimeMillis() < waitUntil) {
                Thread.sleep(20);
            }
            assertThat(interrupted.get()).isEqualTo(1);
            // 终止原因已记录
            assertThat(ctx.ledger().termination()).isNotNull();
            assertThat(ctx.ledger().termination().reason())
                    .isEqualTo(TerminationReason.DEADLINE_EXCEEDED);
        }
    }

    @Test
    void callerCancelPropagatesToSubTasks() {
        BudgetContext ctx = BudgetContext.start("req-fanout-cancel",
                Deadline.after(Duration.ofSeconds(30)));
        try (BudgetContext.Scope ignored = ctx.install()) {
            ctx.cancellation().cancel(TerminationReason.CANCELLED_BY_CALLER);
            FanOutResult<String> result = FanOut.invokeAll(
                    List.of(() -> "x", () -> "y"), executor);
            // 已取消上下文中的子任务被拒绝执行，结论为 CANCELLED
            assertThat(result.outcome()).isEqualTo(Outcome.CANCELLED);
            assertThat(result.successes()).isEmpty();
            assertThat(result.failures()).hasSize(2);
            assertThat(result.failures().get(0).terminationReason())
                    .isEqualTo(TerminationReason.CANCELLED_BY_CALLER.name());
        }
    }
}
