package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.degrade.DegradedResult;
import com.github.lechandonga.deadlinepropagationjava.exec.BudgetAwareExecutor;
import com.github.lechandonga.deadlinepropagationjava.exec.FanOut;
import com.github.lechandonga.deadlinepropagationjava.observability.BudgetLedger;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryingFanOut;
import com.github.lechandonga.deadlinepropagationjava.stream.BudgetedStreamEmitter;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demo endpoints exercising the sync, async fan-out and streaming paths.
 * Every stage checks the shared budget, so no path can outlive the request's
 * deadline or regain budget after a timeout.
 */
@RestController
public class DemoController {

    private final BudgetAwareExecutor executor;
    private final RetryingFanOut retryingFanOut;

    public DemoController(BudgetAwareExecutor executor, RetryingFanOut retryingFanOut) {
        this.executor = executor;
        this.retryingFanOut = retryingFanOut;
    }

    /** Sync path: staged work with budget checks between stages. */
    @GetMapping("/api/sync")
    public Map<String, Object> sync(@RequestParam(defaultValue = "50") long workMs,
                                    HttpServletRequest request) throws InterruptedException {
        RequestBudget budget = BudgetContext.requireCurrent();
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);
        int stages = 3;
        for (int i = 0; i < stages; i++) {
            budget.checkAlive();
            Thread.sleep(workMs);
            ledger.recordStage("sync-stage-" + i, budget.consumed());
        }
        return Map.of(
                "status", "ok",
                "remainingMs", budget.remaining().toMillis());
    }

    /** Async path: concurrent subtasks with one deterministic aggregate. */
    @GetMapping("/api/fanout")
    public Map<String, Object> fanout(@RequestParam(defaultValue = "50") long workMs,
                                      HttpServletRequest request) throws Exception {
        RequestBudget budget = BudgetContext.requireCurrent();
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);

        List<FanOut.NamedTask<String>> tasks = List.of(
                new FanOut.NamedTask<>("fast",
                        () -> executor.submit(() -> sleepAndReturn(workMs, "fast-done"), budget)),
                new FanOut.NamedTask<>("slow",
                        () -> executor.submit(() -> sleepAndReturn(workMs * 4, "slow-done"), budget)),
                new FanOut.NamedTask<>("failing",
                        () -> executor.submit(() -> {
                            throw new IllegalStateException("boom");
                        }, budget)));

        FanOut.FanOutResult<String> result = FanOut.gather(tasks).join();
        ledger.recordStage("fanout", budget.consumed());

        Map<String, String> outcomes = new LinkedHashMap<>();
        result.outcomes().forEach(o -> outcomes.put(o.name(), o.status().name()));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("outcomes", outcomes);
        body.put("completed", result.completed());
        body.put("retryable", result.retryable());
        body.put("remainingMs", budget.remaining().toMillis());
        return body;
    }

    /**
     * Streaming path: emits ticks until the budget expires or the client
     * disconnects. Termination is deterministic: once terminated, no further
     * bytes are written and exactly one terminator line closes the stream.
     */
    @GetMapping(path = "/api/stream", produces = MediaType.TEXT_PLAIN_VALUE)
    public StreamingResponseBody stream(HttpServletRequest request) {
        RequestBudget budget = BudgetContext.requireCurrent();
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);
        return outputStream -> {
            BudgetedStreamEmitter emitter = new BudgetedStreamEmitter(outputStream, budget);
            try {
                int tick = 0;
                while (emitter.writeLine("tick " + tick++)) {
                    budget.checkAlive();
                    Thread.sleep(100);
                }
                // writeLine refused because the budget terminated: finish once.
                emitter.finish(budget.terminationReason());
            } catch (BudgetExhaustedException | com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException e) {
                // Deterministic finish: stop writing, record the real reason.
                emitter.finish(budget.terminationReason());
                ledger.recordStage("stream-terminated", budget.consumed());
            } catch (IOException clientGone) {
                emitter.finish(TerminationReason.CLIENT_CANCELLED);
                ledger.recordStage("stream-client-cancelled", budget.consumed());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                emitter.finish(budget.terminationReason());
                ledger.recordStage("stream-interrupted", budget.consumed());
            }
        };
    }

    /**
     * Retry + degrade path. Three named units: one confirms immediately, one
     * fails until attempt N (then confirms, demonstrating that retries rescue
     * unconfirmed work), one always fails (demonstrating the bounded cap and
     * a meaningful partial result).
     */
    @GetMapping("/api/degrade")
    public Map<String, Object> degrade(@RequestParam(defaultValue = "3") int succeedOnAttempt,
                                       HttpServletRequest request) {
        RequestBudget budget = BudgetContext.requireCurrent();
        RequestPlan plan = BudgetWebFilter.currentPlan(request);
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);

        AtomicInteger flakyAttempts = new AtomicInteger();
        List<FanOut.NamedTask<String>> tasks = List.of(
                new FanOut.NamedTask<>("confirmed",
                        () -> executor.submit(() -> "done", budget)),
                new FanOut.NamedTask<>("flaky",
                        () -> executor.submit(() -> {
                            if (flakyAttempts.incrementAndGet() < succeedOnAttempt) {
                                throw new IllegalStateException("transient failure");
                            }
                            return "recovered";
                        }, budget)),
                new FanOut.NamedTask<>("always-failing",
                        () -> executor.submit(() -> {
                            throw new IllegalStateException("permanent failure");
                        }, budget)));

        DegradedResult<String> result = retryingFanOut.execute(
                tasks, budget, plan.retryPolicy(), plan.limiter());
        ledger.recordStage("degrade", budget.consumed());

        Map<String, Object> body = result.toResponseBody();
        body.put("remainingMs", budget.remaining().toMillis());
        return body;
    }

    /**
     * Deadline-driven degrade path: a quick unit confirms while a heavy unit
     * runs into the deadline. The remaining budget cannot cover the retry
     * threshold, so no retry is forced and the request degrades with
     * {@code INSUFFICIENT_REMAINING}.
     */
    @GetMapping("/api/deadline")
    public Map<String, Object> deadline(@RequestParam(defaultValue = "220") long heavyMs,
                                        @RequestParam(defaultValue = "false") boolean failFirst,
                                        HttpServletRequest request) {
        RequestBudget budget = BudgetContext.requireCurrent();
        RequestPlan plan = BudgetWebFilter.currentPlan(request);
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);

        List<FanOut.NamedTask<String>> tasks = List.of(
                new FanOut.NamedTask<>("quick",
                        () -> executor.submit(() -> "done", budget)),
                new FanOut.NamedTask<>("heavy",
                        () -> executor.submit(() -> {
                            if (failFirst) {
                                // Fails fast on attempt 1; the retry gate then
                                // observes too little remaining budget and
                                // declines to force another attempt.
                                throw new IllegalStateException("first attempt failed");
                            }
                            return sleepAndReturn(heavyMs, "heavy-done");
                        }, budget)));

        DegradedResult<String> result = retryingFanOut.execute(
                tasks, budget, plan.retryPolicy(), plan.limiter());
        ledger.recordStage("deadline-degrade", budget.consumed());

        Map<String, Object> body = result.toResponseBody();
        body.put("remainingMs", budget.remaining().toMillis());
        return body;
    }

    private static String sleepAndReturn(long ms, String value) throws InterruptedException {
        Thread.sleep(ms);
        return value;
    }
}
