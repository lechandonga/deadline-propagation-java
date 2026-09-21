package com.github.lechandonga.deadlinepropagationjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.Deadline;
import com.github.lechandonga.deadlinepropagationjava.exec.ContextPropagatingExecutor;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class ContextIsolationTest {

    @Test
    void nestedScopeRestoresPreviousContext() {
        BudgetContext outer = BudgetContext.start("outer", Deadline.after(Duration.ofSeconds(5)));
        BudgetContext inner = BudgetContext.start("inner", Deadline.after(Duration.ofSeconds(5)));
        assertThat(BudgetContext.current()).isNull();
        try (BudgetContext.Scope s1 = outer.install()) {
            assertThat(BudgetContext.current()).isSameAs(outer);
            try (BudgetContext.Scope s2 = inner.install()) {
                assertThat(BudgetContext.current()).isSameAs(inner);
            }
            assertThat(BudgetContext.current()).isSameAs(outer);
        }
        assertThat(BudgetContext.current()).isNull();
    }

    @Test
    void reusedPoolThreadDoesNotLeakContextAcrossRequests() throws Exception {
        // 单线程池：强制线程复用，验证前序请求上下文不残留
        ContextPropagatingExecutor executor = ContextPropagatingExecutor.wrap(
                Executors.newFixedThreadPool(1));
        try {
            BudgetContext ctxA = BudgetContext.start("req-A", Deadline.after(Duration.ofSeconds(5)));
            BudgetContext ctxB = BudgetContext.start("req-B", Deadline.after(Duration.ofSeconds(5)));

            Future<String> fa;
            Future<String> fb;
            try (BudgetContext.Scope ignored = ctxA.install()) {
                fa = executor.submit(() -> BudgetContext.current().requestId());
            }
            try (BudgetContext.Scope ignored = ctxB.install()) {
                fb = executor.submit(() -> BudgetContext.current().requestId());
            }
            // 无线程上下文时提交的任务必须看到 null（无残留）
            Future<String> fc = executor.submit(() -> {
                BudgetContext current = BudgetContext.current();
                return current == null ? "clean" : "leaked:" + current.requestId();
            });
            Future<String> fmdc = executor.submit(() -> {
                String rid = MDC.get("requestId");
                return rid == null ? "mdc-clean" : "mdc-leaked:" + rid;
            });

            assertThat(fa.get()).isEqualTo("req-A");
            assertThat(fb.get()).isEqualTo("req-B");
            assertThat(fc.get()).isEqualTo("clean");
            assertThat(fmdc.get()).isEqualTo("mdc-clean");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void interruptFlagDoesNotLeakToNextTaskOnReusedThread() throws Exception {
        ContextPropagatingExecutor executor = ContextPropagatingExecutor.wrap(
                Executors.newFixedThreadPool(1));
        try {
            BudgetContext ctx = BudgetContext.start("req-int", Deadline.after(Duration.ofSeconds(30)));
            java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
            Future<?> cancelled;
            try (BudgetContext.Scope ignored = ctx.install()) {
                cancelled = executor.submit(() -> {
                    started.countDown();
                    try {
                        Thread.sleep(30_000);
                    } catch (InterruptedException ie) {
                        // 被取消中断
                    }
                });
            }
            started.await();
            ctx.cancellation().cancel(
                    com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason.CANCELLED_BY_CALLER);
            cancelled.get();

            // 同一复用线程上的下一个任务不得继承中断标记
            Future<Boolean> next = executor.submit(() -> Thread.currentThread().isInterrupted());
            assertThat(next.get()).isFalse();
        } finally {
            executor.shutdownNow();
        }
    }
}
