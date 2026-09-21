package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.BudgetException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 预算异常 → HTTP 响应的统一映射。
 * 超时(504)、预算耗尽拒绝(503)、调用方取消(499) 可区分，绝不伪装成成功。
 * 响应体只含终止原因与请求标识，不含敏感请求内容。
 */
@RestControllerAdvice
public class BudgetExceptionHandler {

    @ExceptionHandler(BudgetException.DeadlineExceeded.class)
    public ResponseEntity<Map<String, Object>> deadlineExceeded(BudgetException.DeadlineExceeded e) {
        return build(HttpStatus.GATEWAY_TIMEOUT, e);
    }

    @ExceptionHandler(BudgetException.Rejected.class)
    public ResponseEntity<Map<String, Object>> rejected(BudgetException.Rejected e) {
        return build(HttpStatus.SERVICE_UNAVAILABLE, e);
    }

    @ExceptionHandler(BudgetException.Cancelled.class)
    public ResponseEntity<Map<String, Object>> cancelled(BudgetException.Cancelled e) {
        return build(HttpStatus.valueOf(499), e);
    }

    private ResponseEntity<Map<String, Object>> build(HttpStatus status, BudgetException e) {
        BudgetContext ctx = BudgetContext.current();
        if (ctx != null) {
            ctx.terminate(e.reason(), true);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "terminated");
        body.put("reason", e.reason().name());
        body.put("requestId", ctx != null ? ctx.requestId() : "unknown");
        return ResponseEntity.status(status).body(body);
    }
}
