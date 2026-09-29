package com.github.lechandonga.deadlinepropagationjava.web;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.observability.BudgetLedger;
import com.github.lechandonga.deadlinepropagationjava.retry.EntrypointResolver;
import com.github.lechandonga.deadlinepropagationjava.retry.EntrypointSettings;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Assigns a fresh budget to every incoming request, installs it for the
 * request thread, and guarantees cleanup so pooled threads stay isolated.
 * For async dispatches an {@link AsyncListener} carries termination to the
 * budget and finalizes the ledger exactly once.
 */
public class BudgetWebFilter extends OncePerRequestFilter {

    public static final String BUDGET_ATTR = BudgetWebFilter.class.getName() + ".BUDGET";
    public static final String LEDGER_ATTR = BudgetWebFilter.class.getName() + ".LEDGER";
    public static final String SETTINGS_ATTR = BudgetWebFilter.class.getName() + ".SETTINGS";

    private static final Logger log = LoggerFactory.getLogger(BudgetWebFilter.class);

    private final EntrypointResolver resolver;

    public BudgetWebFilter(BudgetProperties properties) {
        this(properties, new EntrypointResolver(properties));
    }

    public BudgetWebFilter(BudgetProperties properties, EntrypointResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        // Snapshot per-entrypoint settings once: in-flight requests are never
        // steered by runtime configuration changes.
        EntrypointSettings settings = resolver.resolve(request.getRequestURI());
        request.setAttribute(SETTINGS_ATTR, settings);
        RequestBudget budget = RequestBudget.newBudget(settings.budget());
        BudgetLedger ledger = BudgetLedger.create();
        request.setAttribute(BUDGET_ATTR, budget);
        request.setAttribute(LEDGER_ATTR, ledger);

        try (BudgetContext.Scope ignored = BudgetContext.enter(budget)) {
            chain.doFilter(request, response);
        } catch (ServletException | IOException | RuntimeException e) {
            if (budget.terminationReason() == TerminationReason.NONE) {
                budget.cancel(TerminationReason.ERROR);
            }
            throw e;
        } finally {
            // Scope is closed here, restoring the pooled thread. For async
            // requests the response finishes later, so termination is
            // finalized by an AsyncListener instead.
            if (request.isAsyncStarted()) {
                request.getAsyncContext().addListener(new AsyncListener() {
                    @Override
                    public void onComplete(AsyncEvent event) {
                        finalizeLedger(budget, ledger);
                    }

                    @Override
                    public void onTimeout(AsyncEvent event) {
                        budget.cancel(TerminationReason.CLIENT_CANCELLED);
                    }

                    @Override
                    public void onError(AsyncEvent event) {
                        if (budget.terminationReason() == TerminationReason.NONE) {
                            budget.cancel(TerminationReason.ERROR);
                        }
                    }

                    @Override
                    public void onStartAsync(AsyncEvent event) {
                    }
                });
            } else {
                finalizeLedger(budget, ledger);
            }
        }
    }

    private void finalizeLedger(RequestBudget budget, BudgetLedger ledger) {
        TerminationReason reason = budget.terminationReason();
        // Sync path: once the filter returns, no request work is running.
        ledger.recordTermination(reason, true);
        log.info("request finished {}", ledger.summary());
    }

    /** The budget of the request currently served by this thread, if any. */
    public static RequestBudget currentBudget(HttpServletRequest request) {
        return (RequestBudget) request.getAttribute(BUDGET_ATTR);
    }

    public static BudgetLedger currentLedger(HttpServletRequest request) {
        return (BudgetLedger) request.getAttribute(LEDGER_ATTR);
    }

    /** Immutable entrypoint settings snapshot bound to this request. */
    public static EntrypointSettings currentSettings(HttpServletRequest request) {
        return (EntrypointSettings) request.getAttribute(SETTINGS_ATTR);
    }
}
