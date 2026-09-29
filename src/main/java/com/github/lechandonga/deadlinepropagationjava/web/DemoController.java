package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.BudgetAwareExecutor;
import com.github.lechandonga.deadlinepropagationjava.exec.FanOut;
import com.github.lechandonga.deadlinepropagationjava.observability.BudgetLedger;
import com.github.lechandonga.deadlinepropagationjava.retry.ConcurrencyLimiter;
import com.github.lechandonga.deadlinepropagationjava.retry.DegradedResult;
import com.github.lechandonga.deadlinepropagationjava.retry.EntrypointSettings;
import com.github.lechandonga.deadlinepropagationjava.retry.ItemResult;
import com.github.lechandonga.deadlinepropagationjava.retry.RetryingFanOut;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
     * Retry-aware fan-out: unfinished items are retried within the original
     * budget; the response is always 200 with a three-state {@code state}
     * (ALL_SUCCESS / PARTIAL_SUCCESS / FAILED), the confirmed-vs-unfinished
     * split and the stop reason.
     */
    @GetMapping("/api/retry/fanout")
    public Map<String, Object> retryFanout(@RequestParam(defaultValue = "80") long workMs,
                                           HttpServletRequest request) {
        RequestBudget budget = BudgetContext.requireCurrent();
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);
        EntrypointSettings settings = BudgetWebFilter.currentSettings(request);

        // "slow" always exceeds this entrypoint's budget; "flaky" fails exactly
        // its first attempt and confirms on the retry; "fast" always succeeds.
        AtomicInteger flakyAttempts = new AtomicInteger();
        List<RetryingFanOut.NamedWork<String>> items = List.of(
                new RetryingFanOut.NamedWork<>("fast",
                        () -> sleepAndReturn(workMs, "fast-done")),
                new RetryingFanOut.NamedWork<>("flaky",
                        () -> {
                            if (flakyAttempts.incrementAndGet() == 1) {
                                throw new IllegalStateException("transient");
                            }
                            return sleepAndReturn(workMs, "flaky-done");
                        }),
                new RetryingFanOut.NamedWork<>("slow",
                        () -> sleepAndReturn(workMs * 20, "slow-done")));

        ConcurrencyLimiter limiter =
                ConcurrencyLimiter.bounded(settings.name(), settings.maxConcurrency());
        DegradedResult<String> result =
                retryingFanOut.execute(budget, settings.retry(), limiter, items);
        ledger.recordStage("retry-fanout", budget.consumed());

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("state", result.state().name());
        body.put("stopReason", result.stopReason().name());
        body.put("completed", result.completed());
        body.put("unfinished", result.unfinished());
        List<Map<String, Object>> itemDetails = new java.util.ArrayList<>();
        for (ItemResult<String> item : result.items()) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("name", item.name());
            detail.put("status", item.status().name());
            detail.put("attempts", item.attempts());
            detail.put("stopReason", item.stopReason().name());
            if (item.value() != null) {
                detail.put("value", item.value());
            }
            itemDetails.add(detail);
        }
        body.put("items", itemDetails);
        body.put("remainingMs", budget.remaining().toMillis());
        return body;
    }

    /**
     * Retry-aware streaming path. Each emitted tick is confirmed exactly once:
     * a tick whose work fails before confirmation is retried within budget;
     * already-written bytes are never repeated and no extra bytes appear.
     * The stream always ends with one deterministic termination marker.
     */
    @GetMapping(path = "/api/stream-retry", produces = MediaType.TEXT_PLAIN_VALUE)
    public StreamingResponseBody streamRetry(@RequestParam(defaultValue = "-1") int flakyTick,
                                             @RequestParam(defaultValue = "40") long workMs,
                                             HttpServletRequest request) {
        RequestBudget budget = BudgetContext.requireCurrent();
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);
        EntrypointSettings settings = BudgetWebFilter.currentSettings(request);
        return outputStream -> {
            String stopReason = "COMPLETE";
            int ticks = 3;
            boolean finishedAllTicks = false;
            try {
                for (int tick = 0; tick < ticks; tick++) {
                    stopReason = emitTickWithRetry(tick, flakyTick == tick, outputStream,
                            budget, settings, workMs);
                    if (!"CONTINUE".equals(stopReason)) {
                        break;
                    }
                    if (tick == ticks - 1) {
                        finishedAllTicks = true;
                    }
                }
                if (finishedAllTicks) {
                    stopReason = "COMPLETE";
                }
            } catch (IOException clientGone) {
                budget.cancel(TerminationReason.CLIENT_CANCELLED);
                stopReason = "CLIENT_CANCELLED";
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                stopReason = "INTERRUPTED";
            } finally {
                ledger.recordStage("stream-retry-finished", budget.consumed());
                // Exactly one deterministic terminator; never written twice.
                try {
                    outputStream.write(("END:" + stopReason + "\n").getBytes(StandardCharsets.UTF_8));
                    outputStream.flush();
                } catch (IOException ignored) {
                    // Client already gone: nothing more can or should be sent.
                }
            }
        };
    }

    /**
     * Emits one tick, retrying only while the tick has not been confirmed
     * (i.e. before its bytes are written). Returns CONTINUE on confirmation
     * or the stop reason when the budget forced the stream to end.
     */
    private String emitTickWithRetry(int tick, boolean flakyFirstAttempt,
                                     java.io.OutputStream stream,
                                     RequestBudget budget, EntrypointSettings settings,
                                     long workMs)
            throws IOException, InterruptedException {
        int maxAttempts = settings.retry().maxAttempts();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (budget.remaining().compareTo(settings.retry().retryThreshold()) < 0) {
                return "BUDGET_EXHAUSTED";
            }
            try {
                budget.checkAlive();
                if (flakyFirstAttempt && attempt == 1) {
                    throw new IllegalStateException("transient tick failure");
                }
                Thread.sleep(workMs);
                budget.checkAlive();
                // Confirmation point: bytes leave the process exactly once.
                stream.write(("tick " + tick + "\n").getBytes(StandardCharsets.UTF_8));
                stream.flush();
                return "CONTINUE";
            } catch (BudgetExhaustedException e) {
                return "BUDGET_EXHAUSTED";
            } catch (com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException e) {
                return e.reason() == TerminationReason.CLIENT_CANCELLED
                        ? "CLIENT_CANCELLED" : "CANCELLED";
            } catch (IllegalStateException transientFailure) {
                if (attempt >= maxAttempts) {
                    return "FAILED";
                }
                // Budget-bound backoff; a dead budget stops the retry.
                Duration wait = settings.retry().backoff();
                Thread.sleep(Math.min(wait.toMillis(), Math.max(budget.remaining().toMillis(), 0)));
            }
        }
        return "FAILED";
    }

    /**
     * Streaming path: emits ticks until the budget expires or the client
     * disconnects. Termination is deterministic: once terminated, no further
     * bytes are written and the ledger records why.
     */
    @GetMapping(path = "/api/stream", produces = MediaType.TEXT_PLAIN_VALUE)
    public StreamingResponseBody stream(HttpServletRequest request) {
        RequestBudget budget = BudgetContext.requireCurrent();
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);
        return outputStream -> {
            try {
                int tick = 0;
                while (true) {
                    budget.checkAlive();
                    outputStream.write(("tick " + tick++ + "\n").getBytes(StandardCharsets.UTF_8));
                    outputStream.flush();
                    Thread.sleep(100);
                }
            } catch (BudgetExhaustedException | com.github.lechandonga.deadlinepropagationjava.budget.RequestCancelledException e) {
                // Deterministic finish: stop writing, record the real reason.
                ledger.recordStage("stream-terminated", budget.consumed());
            } catch (IOException clientGone) {
                budget.cancel(TerminationReason.CLIENT_CANCELLED);
                ledger.recordStage("stream-client-cancelled", budget.consumed());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                ledger.recordStage("stream-interrupted", budget.consumed());
            }
        };
    }

    private static String sleepAndReturn(long ms, String value) throws InterruptedException {
        Thread.sleep(ms);
        return value;
    }
}
