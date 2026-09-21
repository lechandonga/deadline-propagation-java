package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.exec.ContextPropagatingExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Web 配置：
 * <ul>
 *   <li>注册预算拦截器（默认预算/上限来自配置项）；</li>
 *   <li>MVC 异步执行统一走上下文传播线程池，保证异步路径预算连续扣减。</li>
 * </ul>
 */
@Configuration
public class BudgetWebConfig implements WebMvcConfigurer {

    private final ContextPropagatingExecutor budgetExecutor;
    private final long defaultBudgetMillis;
    private final long maxBudgetMillis;

    public BudgetWebConfig(ContextPropagatingExecutor budgetExecutor,
                           @Value("${budget.default-ms:2000}") long defaultBudgetMillis,
                           @Value("${budget.max-ms:10000}") long maxBudgetMillis) {
        this.budgetExecutor = budgetExecutor;
        this.defaultBudgetMillis = defaultBudgetMillis;
        this.maxBudgetMillis = maxBudgetMillis;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new BudgetInterceptor(defaultBudgetMillis, maxBudgetMillis))
                .addPathPatterns("/api/**");
    }

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setTaskExecutor(budgetExecutor::execute);
        configurer.setDefaultTimeout(defaultBudgetMillis);
    }
}
