package com.github.lechandonga.deadlinepropagationjava.tests.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Budget-exhaustion path with the concurrency cap lifted, so retries run
 * until the single shrinking deadline forces a PARTIAL_SUCCESS answer.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "request.budget.default-budget=400ms",
        "request.budget.entrypoints[/api/retry/fanout].max-concurrency=0"
})
class RetryBudgetFlowIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @Test
    void retriesConsumeOnlyTheOriginalBudgetAndDegradeAtDeadline() {
        long start = System.nanoTime();
        ResponseEntity<Map> response =
                rest.getForEntity("/api/retry/fanout?workMs=80", Map.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(HttpStatus.OK, response.getStatusCode(),
                "partial success is delivered as a degraded result, not a 500");
        Map body = response.getBody();
        assertEquals("PARTIAL_SUCCESS", body.get("state"));
        assertEquals("BUDGET_EXHAUSTED", body.get("stopReason"));

        List<String> completed = (List<String>) body.get("completed");
        assertTrue(completed.contains("fast"));
        assertTrue(completed.contains("flaky"), "recovered via retry inside the same budget");
        assertEquals(List.of("slow"), body.get("unfinished"));

        // 400ms budget + grace; retries never buy extra wall-clock time.
        assertTrue(elapsedMs < 1_200,
                "retries must not extend the request past its deadline: " + elapsedMs);
        assertEquals(0L, ((Number) body.get("remainingMs")).longValue());
    }
}
