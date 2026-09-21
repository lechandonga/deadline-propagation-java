package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetException;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.ContextPropagatingExecutor;
import com.github.lechandonga.deadlinepropagationjava.fanout.FanOut;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.WebAsyncTask;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * 演示端点：同步、异步、并发扇出、流式四条路径共享同一请求预算。
 * 所有路径的超时/取消行为一致：预算耗尽即终止后续工作，终止原因可区分。
 */
@RestController
@RequestMapping("/api")
public class DemoController {

    private final ContextPropagatingExecutor executor;
    private final AsyncTaskExecutor asyncExecutor;

    public DemoController(ContextPropagatingExecutor executor) {
        this.executor = executor;
        this.asyncExecutor = executor::execute;
    }

    /** 同步路径：分片执行，每片前做预算检查点。 */
    @GetMapping("/sync")
    public Map<String, Object> sync(@RequestParam(defaultValue = "100") long workMs) {
        BudgetContext ctx = BudgetContext.current();
        doWork(ctx, workMs, "sync-work");
        return Map.of("status", "ok",
                "remainingMs", ctx.deadline().remainingMillis());
    }

    /** 异步路径：异步任务共享入口预算；超时回调触发取消，结果确定。 */
    @GetMapping("/async")
    public WebAsyncTask<ResponseEntity<Map<String, Object>>> async(
            @RequestParam(defaultValue = "100") long workMs) {
        BudgetContext ctx = BudgetContext.current();
        long timeoutMs = Math.max(1, ctx.deadline().remainingMillis());
        Callable<ResponseEntity<Map<String, Object>>> job = () -> {
            doWork(ctx, workMs, "async-work");
            return ResponseEntity.ok(Map.<String, Object>of(
                    "status", "ok", "remainingMs", ctx.deadline().remainingMillis()));
        };
        WebAsyncTask<ResponseEntity<Map<String, Object>>> task =
                new WebAsyncTask<>(timeoutMs, asyncExecutor, job);
        task.onTimeout(() -> {
            ctx.cancellation().cancel(TerminationReason.DEADLINE_EXCEEDED);
            ctx.terminate(TerminationReason.DEADLINE_EXCEEDED, true);
            return ResponseEntity.status(504).body(Map.<String, Object>of(
                    "status", "timeout", "reason", TerminationReason.DEADLINE_EXCEEDED.name()));
        });
        return task;
    }

    /** 并发扇出：部分成功/失败/超时时整体结论唯一，retryable 不含已完成工作。 */
    @GetMapping("/fanout")
    public Map<String, Object> fanout(@RequestParam(defaultValue = "50") long workMs,
                                      @RequestParam(defaultValue = "-1") int failIndex) {
        BudgetContext ctx = BudgetContext.current();
        List<Callable<String>> tasks = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final int index = i;
            tasks.add(() -> {
                doWork(ctx, workMs, "fanout-task-" + index);
                if (index == failIndex) {
                    throw new IllegalStateException("simulated failure");
                }
                return "task-" + index;
            });
        }
        FanOut.FanOutResult<String> result = FanOut.invokeAll(tasks, executor);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("outcome", result.outcome().name());
        body.put("successes", result.successes());
        body.put("failures", result.failures());
        body.put("completedIndexes", result.completedIndexes());
        body.put("retryableIndexes", result.retryableIndexes(tasks.size()));
        return body;
    }

    /**
     * 流式路径：响应已开始输出后超时，收尾必须确定——
     * 写出明确的终止标记后结束流；客户端断连则取消上下文。
     */
    @GetMapping("/stream")
    public StreamingResponseBody stream(@RequestParam(defaultValue = "10") int chunks,
                                        @RequestParam(defaultValue = "100") long chunkMs) {
        BudgetContext ctx = BudgetContext.current();
        return out -> {
            try (BudgetContext.Scope ignored = ctx.install()) {
                for (int i = 0; i < chunks; i++) {
                    ctx.checkpoint("stream-chunk-" + i);
                    out.write(("chunk-" + i + "\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    Thread.sleep(chunkMs);
                }
                out.write("DONE\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
            } catch (BudgetException be) {
                // 预算耗尽/被取消：写确定性的终止标记后收尾
                ctx.terminate(be.reason(), true);
                try {
                    out.write(("TERMINATED:" + be.reason() + "\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                } catch (IOException ignoredIo) {
                    // 客户端已断开，无需再写
                }
            } catch (IOException ioe) {
                ctx.cancellation().cancel(TerminationReason.CLIENT_DISCONNECTED);
                ctx.terminate(TerminationReason.CLIENT_DISCONNECTED, true);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                ctx.cancellation().cancel(TerminationReason.CANCELLED_BY_CALLER);
                ctx.terminate(TerminationReason.CANCELLED_BY_CALLER, true);
            }
        };
    }

    /** 分片执行工作，每片前检查预算；被中断时按取消语义退出。 */
    private void doWork(BudgetContext ctx, long workMs, String stage) {
        long deadlineNanos = System.nanoTime() + workMs * 1_000_000L;
        while (System.nanoTime() < deadlineNanos) {
            ctx.checkpoint(stage);
            long sliceMs = Math.min(25, (deadlineNanos - System.nanoTime()) / 1_000_000L + 1);
            try {
                Thread.sleep(sliceMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // 中断来自取消传播：以取消语义终止，而非伪装成功
                ctx.cancellation().throwIfTerminated();
                throw new BudgetException.Cancelled("work interrupted at stage '" + stage + "'");
            }
        }
        ctx.checkpoint(stage + "-done");
    }
}
