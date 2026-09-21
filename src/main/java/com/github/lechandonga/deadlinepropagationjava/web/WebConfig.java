package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.exec.BudgetAwareExecutor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;

/** Wires the budget filter and the budget-aware executor. */
@Configuration
@EnableConfigurationProperties(BudgetProperties.class)
public class WebConfig {

    @Bean
    public FilterRegistrationBean<BudgetWebFilter> budgetWebFilter(BudgetProperties properties) {
        FilterRegistrationBean<BudgetWebFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new BudgetWebFilter(properties));
        registration.addUrlPatterns("/api/*");
        registration.setOrder(1);
        return registration;
    }

    /** Shared worker pool; all submissions carry the caller's budget. */
    @Bean(destroyMethod = "close")
    public BudgetAwareExecutor budgetAwareExecutor() {
        return BudgetAwareExecutor.wrap(Executors.newFixedThreadPool(4, runnable -> {
            Thread thread = new Thread(runnable, "budget-worker");
            thread.setDaemon(true);
            return thread;
        }));
    }
}
