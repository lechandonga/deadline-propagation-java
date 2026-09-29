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
 * End-to-end retry/degradation behaviour: partial success is returned as a
 * degraded 200 instead of failing the request, the stream emits each tick at
 * most once and ends with exactly one deterministic marker, and the
 * per-entrypoint concurrency cap degrades rather than queues.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "request.budget.default-budget=400ms")
class RetryFlowIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @Test
    void fanoutDegradesConcurrencyLimitedItem() {
        // max-concurrency=2 for three concurrently submitted items: the
        // rejected item is reported up front with attempts=0.
        ResponseEntity<Map> response =
                rest.getForEntity("/api/retry/fanout?workMs=80", Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        Map body = response.getBody();
        assertEquals("PARTIAL_SUCCESS", body.get("state"));
        assertEquals("CONCURRENCY_LIMITED", body.get("stopReason"));
        assertTrue(((List<String>) body.get("completed")).contains("fast"));
        assertTrue(((List<String>) body.get("completed")).contains("flaky"),
                "flaky work recovered via an in-budget retry before degrading slow");
        List<Map<String, Object>> items = (List<Map<String, Object>>) body.get("items");
        boolean rejectedSeen = false;
        for (Map<String, Object> item : items) {
            if ("REJECTED".equals(item.get("status"))) {
                rejectedSeen = true;
                assertEquals(0, item.get("attempts"), "rejected work never ran");
                assertEquals("CONCURRENCY_LIMITED", item.get("stopReason"));
            }
        }
        assertTrue(rejectedSeen, "the cap must surface as a degraded item, not a queue");
    }

    @Test
    void retriedStreamEmitsEachTickOnceAndEndsWithOneMarker() {
        // tick 1 fails its first attempt before any byte is confirmed: it is
        // retried and must appear exactly once.
        String body = rest.getForObject("/api/stream-retry?flakyTick=1", String.class);

        assertNotNull(body);
        for (int tick = 0; tick < 3; tick++) {
            final int tickIndex = tick;
            long occurrences = body.lines()
                    .filter(line -> line.equals("tick " + tickIndex)).count();
            assertEquals(1, occurrences, "tick " + tick + " duplicated or invented: " + body);
        }
        long endMarkers = body.lines().filter(line -> line.startsWith("END:")).count();
        assertEquals(1, endMarkers, "exactly one deterministic terminator");
        assertTrue(body.trim().lines().reduce((a, b) -> b).orElseThrow().startsWith("END:"),
                "the terminator is the last byte group");
        assertTrue(body.contains("END:COMPLETE"));
    }

    @Test
    void streamUnderBudgetPressureEndsWithBudgetMarkerAndNoExtraTicks() {
        // 200ms of work per tick cannot finish 3 ticks inside the 600ms budget
        // together with backoff; it must stop with the budget marker while
        // keeping every earlier tick exactly once.
        String body = rest.getForObject(
                "/api/stream-retry?flakyTick=0&workMs=200", String.class);
        assertNotNull(body);
        long endMarkers = body.lines().filter(line -> line.startsWith("END:")).count();
        assertEquals(1, endMarkers);
        String last = body.trim().lines().reduce((a, b) -> b).orElseThrow();
        assertEquals("END:BUDGET_EXHAUSTED", last);
    }

    @Test
    void unconfiguredEntrypointKeepsLegacyFailureBehaviour() {
        // /api/sync has no retry config: an over-budget request still fails
        // loudly exactly as before the feature existed.
        ResponseEntity<String> response = rest.getForEntity("/api/sync?workMs=200", String.class);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    }
}
