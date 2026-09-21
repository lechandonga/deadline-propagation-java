package com.github.lechandonga.deadlinepropagationjava.tests.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end: one shrinking budget across sync, fan-out and streaming paths,
 * with concurrent requests isolated from each other.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "request.budget.default-budget=400ms")
class BudgetFlowIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @Test
    void syncWorkBeyondBudgetIsTerminatedNotQueued() {
        // 3 stages x 200ms = 600ms > 400ms budget: must fail, loudly.
        ResponseEntity<String> response =
                rest.getForEntity("/api/sync?workMs=200", String.class);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    }

    @Test
    void syncWorkWithinBudgetSucceeds() {
        ResponseEntity<Map> response = rest.getForEntity("/api/sync?workMs=50", Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Number remaining = (Number) response.getBody().get("remainingMs");
        assertTrue(remaining.longValue() > 0, "budget remains but is partially consumed");
        assertTrue(remaining.longValue() < 400, "budget only decreases, never regains");
    }

    @Test
    void fanoutYieldsDeterministicPartialOutcome() {
        // fast=100ms ok, slow=400ms hits the 400ms budget, failing=error.
        ResponseEntity<Map> response = rest.getForEntity("/api/fanout?workMs=100", Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        Map<String, String> outcomes = (Map<String, String>) response.getBody().get("outcomes");
        assertEquals("SUCCESS", outcomes.get("fast"));
        assertEquals("TIMEOUT", outcomes.get("slow"));
        assertEquals("FAILED", outcomes.get("failing"));

        List<String> completed = (List<String>) response.getBody().get("completed");
        List<String> retryable = (List<String>) response.getBody().get("retryable");
        assertEquals(List.of("fast"), completed, "only confirmed work is complete");
        assertFalse(retryable.contains("fast"), "retry must never redo confirmed work");
        assertTrue(retryable.containsAll(List.of("slow", "failing")));
    }

    @Test
    void streamTerminatesDeterministicallyAtDeadline() {
        long start = System.nanoTime();
        String body = rest.getForObject("/api/stream", String.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertNotNull(body);
        assertTrue(body.contains("tick 0"), "stream started before termination");
        long ticks = body.lines().count();
        assertTrue(ticks < 20, "stream must stop at the deadline, got " + ticks + " ticks");
        assertTrue(elapsedMs < 5_000, "stream must not hang after budget expiry");
    }

    @Test
    void concurrentRequestsKeepIsolatedBudgets() throws Exception {
        int parallelism = 8;
        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        try {
            List<Callable<Map>> calls = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                calls.add(() -> rest.getForObject("/api/sync?workMs=30", Map.class));
            }
            List<Future<Map>> futures = new ArrayList<>();
            for (Callable<Map> call : calls) {
                futures.add(pool.submit(call));
            }
            for (Future<Map> future : futures) {
                Map body = future.get();
                assertEquals("ok", body.get("status"));
                Number remaining = (Number) body.get("remainingMs");
                assertTrue(remaining.longValue() > 0 && remaining.longValue() < 400,
                        "each request got its own fresh budget, remaining=" + remaining);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
