package com.github.lechandonga.deadlinepropagationjava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetException;
import com.github.lechandonga.deadlinepropagationjava.budget.CancellationToken;
import com.github.lechandonga.deadlinepropagationjava.budget.Deadline;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.ContextPropagatingExecutor;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CancellationTest {

    @Test
    void firstCancelReasonWins() {
        CancellationToken token = new CancellationToken();
        token.cancel(TerminationReason.CANCELLED_BY_CALLER);
        token.cancel(TerminationReason.DEADLINE_EXCEEDED);
        assertThat(token.isCancelled()).isTrue();
        assertThat(token.reason()).isEqualTo(TerminationReason.CANCELLED_BY_CALLER);
    }

    @Test
    void throwIfTerminatedDistinguishesReasons() {
        CancellationToken timeout = new CancellationToken();
        timeout.cancel(TerminationReason.DEADLINE_EXCEEDED);
        assertThatThrownBy(timeout::throwIfTerminated)
                .isInstanceOf(BudgetException.DeadlineExceeded.class);

        CancellationToken cancelled = new CancellationToken();
        cancelled.cancel(TerminationReason.CANCELLED_BY_CALLER);
        assertThatThrownBy(cancelled::throwIfTerminated)
                .isInstanceOf(BudgetException.Cancelled.class);
    }

    @Test
    void cancelListenerRunsImmediatelyIfAlreadyCancelled() {
        CancellationToken token = new CancellationToken();
        token.cancel(TerminationReason.CANCELLED_BY_CALLER);
        AtomicBoolean ran = new AtomicBoolean();
        token.onCancel(() -> ran.set(true));
        assertThat(ran).isTrue();
    }

    @Test
    void cancelPropagatesToRunningTaskAndFreesThread() throws Exception {
        ContextPropagatingExecutor executor = ContextPropagatingExecutor.wrap(
                Executors.newFixedThreadPool(1));
        try {
            BudgetContext ctx = BudgetContext.start("req-cancel",
                    Deadline.after(Duration.ofSeconds(30)));
            CountDownLatch started = new CountDownLatch(1);
            AtomicBoolean stoppedByInterrupt = new AtomicBoolean();
            AtomicBoolean keptRunning = new AtomicBoolean();

            Future<?> future;
            try (BudgetContext.Scope ignored = ctx.install()) {
                future = executor.submit(() -> {
                    started.countDown();
                    try {
                        Thread.sleep(30_000);
                        keptRunning.set(true); // 不应到达：被取消的工作不得继续
                    } catch (InterruptedException e) {
                        stoppedByInterrupt.set(true);
                    }
                });
            }

            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            ctx.cancellation().cancel(TerminationReason.CANCELLED_BY_CALLER);

            future.get(5, TimeUnit.SECONDS); // 任务必须真正结束
            assertThat(stoppedByInterrupt).isTrue();
            assertThat(keptRunning).isFalse();

            // 线程已被释放：同一线程池可立即执行新任务
            Future<String> next = executor.submit(() -> "free");
            assertThat(next.get(2, TimeUnit.SECONDS)).isEqualTo("free");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void exhaustedBudgetRejectsQueuedWork() throws Exception {
        ContextPropagatingExecutor executor = ContextPropagatingExecutor.wrap(
                Executors.newFixedThreadPool(1));
        try {
            BudgetContext ctx = BudgetContext.start("req-exhausted",
                    Deadline.after(Duration.ofMillis(50)));
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Future<?> future;
            try (BudgetContext.Scope ignored = ctx.install()) {
                future = executor.submit(() -> {
                    // 任务体不会执行：dispatch 检查点应先拒绝
                });
                Thread.sleep(150); // 等预算耗尽后再提交第二个任务
                Future<?> late = executor.submit(() -> {});
                try {
                    late.get(2, TimeUnit.SECONDS);
                } catch (Exception e) {
                    failure.set(e.getCause());
                }
            }
            future.get(2, TimeUnit.SECONDS);
            // 预算耗尽后提交的工作被显式拒绝（异常可区分），而非静默丢弃
            assertThat(failure.get()).isInstanceOf(BudgetException.DeadlineExceeded.class);
            assertThat(ctx.ledger().termination()).isNotNull();
            assertThat(ctx.ledger().termination().reason())
                    .isEqualTo(TerminationReason.DEADLINE_EXCEEDED);
        } finally {
            executor.shutdownNow();
        }
    }
}
