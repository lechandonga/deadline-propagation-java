package com.github.lechandonga.deadlinepropagationjava.exec;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 线程池配置：所有异步/并发工作统一经过 {@link ContextPropagatingExecutor}，
 * 保证预算、截止时间与取消标记跨线程传播且请求间隔离。
 */
@Configuration
public class BudgetExecutorConfig {

    @Bean(destroyMethod = "shutdown")
    public ContextPropagatingExecutor budgetExecutor() {
        AtomicInteger seq = new AtomicInteger();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                4, 8,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                r -> {
                    Thread t = new Thread(r, "budget-worker-" + seq.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                });
        return ContextPropagatingExecutor.wrap(pool);
    }
}
