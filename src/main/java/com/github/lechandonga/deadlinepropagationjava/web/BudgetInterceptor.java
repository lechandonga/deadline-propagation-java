package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.Deadline;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * 入口拦截器：为每个请求安装整体时间预算上下文。
 *
 * <p>预算来源：请求头 {@code X-Budget-Millis}（有上限约束），缺省用配置值。
 * 异步请求会多次进入 preHandle（初次分发 + 异步再分发），同一请求复用同一上下文，
 * 保证预算只减不增。请求结束时输出预算流水日志并彻底清理 ThreadLocal/MDC。
 */
public class BudgetInterceptor implements HandlerInterceptor {

    public static final String ATTR_CONTEXT = BudgetInterceptor.class.getName() + ".CONTEXT";
    public static final String ATTR_SCOPE = BudgetInterceptor.class.getName() + ".SCOPE";

    private static final Logger log = LoggerFactory.getLogger(BudgetInterceptor.class);

    private final long defaultBudgetMillis;
    private final long maxBudgetMillis;

    public BudgetInterceptor(long defaultBudgetMillis, long maxBudgetMillis) {
        this.defaultBudgetMillis = defaultBudgetMillis;
        this.maxBudgetMillis = maxBudgetMillis;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
                             Object handler) {
        BudgetContext ctx = (BudgetContext) request.getAttribute(ATTR_CONTEXT);
        if (ctx == null) {
            // 初次分发：创建请求级预算上下文
            long budgetMs = resolveBudgetMillis(request);
            String requestId = request.getHeader("X-Request-Id");
            if (requestId == null || requestId.isBlank()) {
                requestId = UUID.randomUUID().toString();
            }
            ctx = BudgetContext.start(requestId, Deadline.after(Duration.ofMillis(budgetMs)));
            request.setAttribute(ATTR_CONTEXT, ctx);
        }
        // 每次分发（含异步再分发）都安装新的作用域，afterCompletion 统一收尾
        BudgetContext.Scope scope = ctx.install();
        request.setAttribute(ATTR_SCOPE, scope);
        MDC.put("requestId", ctx.requestId());
        ctx.checkpoint("request-start");
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
                                Object handler, Exception ex) {
        BudgetContext ctx = (BudgetContext) request.getAttribute(ATTR_CONTEXT);
        BudgetContext.Scope scope = (BudgetContext.Scope) request.getAttribute(ATTR_SCOPE);
        try {
            if (ctx != null) {
                if (ctx.ledger().termination() == null) {
                    // 正常完成或内部错误：终止结论必须确定且可区分
                    ctx.terminate(ex == null ? TerminationReason.NONE
                                             : TerminationReason.INTERNAL_ERROR, true);
                }
                // 预算流水日志：仅阶段名/耗时/终止原因，不含敏感请求内容
                log.info("budget-ledger requestId={} stages={} termination={}",
                        ctx.requestId(), ctx.ledger().stageEntries(), ctx.ledger().termination());
            }
        } finally {
            if (scope != null) {
                scope.close();
            }
            MDC.remove("requestId");
        }
    }

    private long resolveBudgetMillis(HttpServletRequest request) {
        String header = request.getHeader("X-Budget-Millis");
        if (header == null || header.isBlank()) {
            return defaultBudgetMillis;
        }
        try {
            long requested = Long.parseLong(header.trim());
            if (requested <= 0) {
                return defaultBudgetMillis;
            }
            // 调用方只能缩短预算，不能突破服务端上限
            return Math.min(requested, maxBudgetMillis);
        } catch (NumberFormatException e) {
            return defaultBudgetMillis;
        }
    }
}
