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
 * End-to-end retry/degrade behavior using the per-entry configuration in
 * application.properties. Fast and deterministic: no external services,
 * real network, or long sleeps.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RetryDegradeIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @Test
    @SuppressWarnings("unchecked")
    void retriesRescueUnconfirmedWorkAndBoundedFailureDegrades() {
        ResponseEntity<Map> response =
                rest.getForEntity("/api/degrade?succeedOnAttempt=3", Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();

        // Three distinguishable states are reported explicitly.
        assertEquals("PARTIAL_SUCCESS", body.get("status"));
        assertEquals("RETRIES_EXHAUSTED", body.get("stopReason"));
        assertEquals(List.of("confirmed", "flaky"), body.get("completed"),
                "flaky was rescued by retrying; confirmed work ran once");

        List<Map<String, Object>> incomplete = (List<Map<String, Object>>) body.get("incomplete");
        assertEquals(1, incomplete.size());
        assertEquals("always-failing", incomplete.get(0).get("name"));
        assertEquals("ERROR", incomplete.get(0).get("reason"));
        assertEquals(4, ((Number) incomplete.get(0).get("attempts")).intValue(),
                "retries are bounded by max-attempts=4");
    }

    @Test
    void allSuccessReportsAllSuccessState() {
        ResponseEntity<Map> all = rest.getForEntity("/api/deadline?heavyMs=1", Map.class);
        assertEquals("ALL_SUCCESS", all.getBody().get("status"));
        assertEquals("NONE", all.getBody().get("stopReason"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void insufficientRemainingBudgetSkipsRetryAndDegrades() {
        // deadline entry: 300ms budget, heavy task sleeps 220ms while quick
        // completes; remaining < 400ms threshold, so no retry is attempted.
        long start = System.nanoTime();
        ResponseEntity<Map> response =
        rest.getForEntity("/api/deadline?failFirst=true", Map.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(HttpStatus.OK, response.getStatusCode());

        Map<String, Object> body = response.getBody();
        assertEquals("PARTIAL_SUCCESS", body.get("status"));
        assertEquals("INSUFFICIENT_REMAINING", body.get("stopReason"),
                "remaining time below startup threshold: give up, don't force a retry");
        assertEquals(List.of("quick"), body.get("completed"));

        List<Map<String, Object>> incomplete = (List<Map<String, Object>>) body.get("incomplete");
        assertEquals("heavy", incomplete.get(0).get("name"));
        assertEquals(1, ((Number) incomplete.get(0).get("attempts")).intValue(),
                "not a single retry fired");
        assertTrue(elapsedMs < 1_500, "retries never extend the overall timeline");
    }

    @Test
    void streamClosesWithOneDeterministicTerminatorAndNoDupes() {
        String body = rest.getForObject("/api/stream", String.class);
        assertNotNull(body);

        long terminators = body.lines()
                .filter(l -> l.startsWith("X-STREAM-TERMINATED:"))
                .count();
        assertEquals(1, terminators, "exactly one terminator line");
        assertTrue(body.endsWith("X-STREAM-TERMINATED:DEADLINE_EXCEEDED\n"),
                "the terminator is the final byte content");
        long tickLines = body.lines().filter(l -> l.startsWith("tick ")).count();
        long uniqueTicks = body.lines().filter(l -> l.startsWith("tick ")).distinct().count();
        assertEquals(uniqueTicks, tickLines, "already-emitted ticks are never repeated");
    }

    @Test
    void unconfiguredEntryStillUsesLegacyDefaultBudget() {
        // /api/sync is not a configured entry: keeps 5s default budget and
        // succeeds just like before the feature was added.
        ResponseEntity<Map> response = rest.getForEntity("/api/sync?workMs=10", Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("ok", response.getBody().get("status"));
    }
}
