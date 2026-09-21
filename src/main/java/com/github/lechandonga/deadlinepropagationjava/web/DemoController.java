package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetExhaustedException;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.exec.BudgetAwareExecutor;
import com.github.lechandonga.deadlinepropagationjava.exec.FanOut;
import com.github.lechandonga.deadlinepropagationjava.observability.BudgetLedger;
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

/**
 * Demo endpoints exercising the sync, async fan-out and streaming paths.
 * Every stage checks the shared budget, so no path can outlive the request's
 * deadline or regain budget after a timeout.
 */
@RestController
public class DemoController {

    private final BudgetAwareExecutor executor;

    public DemoController(BudgetAwareExecutor executor) {
        this.executor = executor;
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
