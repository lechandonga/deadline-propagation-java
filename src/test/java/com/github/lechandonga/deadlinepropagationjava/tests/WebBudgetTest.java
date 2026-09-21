package com.github.lechandonga.deadlinepropagationjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebBudgetTest {

    @Autowired
    private TestRestTemplate rest;

    private static HttpEntity<Void> budgetHeader(long millis) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Budget-Millis", String.valueOf(millis));
        return new HttpEntity<>(headers);
    }

    @Test
    void syncWithinBudgetSucceeds() {
        ResponseEntity<Map> response = rest.exchange(
                "/api/sync?workMs=100", HttpMethod.GET, budgetHeader(2000), Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().get("status")).isEqualTo("ok");
        long remaining = ((Number) response.getBody().get("remainingMs")).longValue();
        // 预算只减不增：剩余必须小于入口预算
        assertThat(remaining).isLessThan(2000).isPositive();
    }

    @Test
    void syncExceedingBudgetReturns504WithReason() {
        ResponseEntity<Map> response = rest.exchange(
                "/api/sync?workMs=3000", HttpMethod.GET, budgetHeader(300), Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
        assertThat(response.getBody().get("reason")).isEqualTo("DEADLINE_EXCEEDED");
        assertThat(response.getBody().get("status")).isEqualTo("terminated");
    }

    @Test
    void asyncTimeoutIsDeterministic() {
        ResponseEntity<Map> response = rest.exchange(
                "/api/async?workMs=3000", HttpMethod.GET, budgetHeader(300), Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
        assertThat(response.getBody().get("reason")).isEqualTo("DEADLINE_EXCEEDED");
    }

    @Test
    void asyncWithinBudgetSucceeds() {
        ResponseEntity<Map> response = rest.exchange(
                "/api/async?workMs=100", HttpMethod.GET, budgetHeader(2000), Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().get("status")).isEqualTo("ok");
    }

    @Test
    void fanoutPartialFailureIsDeterministic() {
        ResponseEntity<Map> response = rest.exchange(
                "/api/fanout?workMs=50&failIndex=1", HttpMethod.GET, budgetHeader(5000),
                Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().get("outcome")).isEqualTo("PARTIAL");
        assertThat(response.getBody().get("retryableIndexes").toString()).isEqualTo("[1]");
        assertThat(response.getBody().get("completedIndexes").toString()).contains("0", "2");
    }

    @Test
    void streamTerminatesDeterministicallyAfterDeadline() {
        ResponseEntity<String> response = rest.exchange(
                "/api/stream?chunks=50&chunkMs=100", HttpMethod.GET, budgetHeader(400),
                String.class);
        // 响应已开始输出：收尾必须确定——带明确终止标记，而非截断后伪装成功
        assertThat(response.getBody()).contains("TERMINATED:DEADLINE_EXCEEDED");
        assertThat(response.getBody()).doesNotContain("DONE");
    }

    @Test
    void streamCompletesWithinBudget() {
        ResponseEntity<String> response = rest.exchange(
                "/api/stream?chunks=3&chunkMs=20", HttpMethod.GET, budgetHeader(5000),
                String.class);
        assertThat(response.getBody()).contains("DONE");
        assertThat(response.getBody()).doesNotContain("TERMINATED");
    }

    @Test
    void timedOutRequestDoesNotAffectNextRequest() {
        // 前一个请求超时后，后续请求必须获得完整的新预算（隔离性）
        ResponseEntity<Map> first = rest.exchange(
                "/api/sync?workMs=2000", HttpMethod.GET, budgetHeader(200), Map.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);

        ResponseEntity<Map> second = rest.exchange(
                "/api/sync?workMs=50", HttpMethod.GET, budgetHeader(2000), Map.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        long remaining = ((Number) second.getBody().get("remainingMs")).longValue();
        assertThat(remaining).isGreaterThan(1500);
    }
}
