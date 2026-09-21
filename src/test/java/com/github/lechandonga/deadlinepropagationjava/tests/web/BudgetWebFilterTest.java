package com.github.lechandonga.deadlinepropagationjava.tests.web;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetContext;
import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.observability.BudgetLedger;
import com.github.lechandonga.deadlinepropagationjava.web.BudgetProperties;
import com.github.lechandonga.deadlinepropagationjava.web.BudgetWebFilter;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BudgetWebFilterTest {

    private BudgetWebFilter newFilter(Duration budget) {
        BudgetProperties properties = new BudgetProperties();
        properties.setDefaultBudget(budget);
        return new BudgetWebFilter(properties);
    }

    @Test
    void everyRequestGetsAFreshBudgetAndThreadIsRestored() throws Exception {
        BudgetWebFilter filter = newFilter(Duration.ofSeconds(5));
        AtomicReference<RequestBudget> insideFirst = new AtomicReference<>();
        AtomicReference<RequestBudget> insideSecond = new AtomicReference<>();

        filter.doFilter(new MockHttpServletRequest("GET", "/api/sync"), new MockHttpServletResponse(),
                (req, res) -> insideFirst.set(BudgetContext.current()));
        filter.doFilter(new MockHttpServletRequest("GET", "/api/sync"), new MockHttpServletResponse(),
                (req, res) -> insideSecond.set(BudgetContext.current()));

        assertNotNull(insideFirst.get(), "budget must be visible inside the chain");
        assertNotNull(insideSecond.get());
        assertNotSame(insideFirst.get(), insideSecond.get(),
                "requests must never share a budget");
        assertNull(BudgetContext.current(),
                "pooled request thread must be restored after the request");
    }

    @Test
    void budgetAndLedgerAreExposedAsRequestAttributes() throws Exception {
        BudgetWebFilter filter = newFilter(Duration.ofSeconds(5));
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/sync");

        filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {
        });

        assertNotNull(BudgetWebFilter.currentBudget(request));
        BudgetLedger ledger = BudgetWebFilter.currentLedger(request);
        assertNotNull(ledger);
        assertEquals(TerminationReason.NONE, ledger.terminationReason(),
                "a normally completed request records NONE, not a fake termination");
        assertTrue(ledger.workStoppedConfirmed());
    }

    @Test
    void downstreamFailureTerminatesBudgetWithError() throws Exception {
        BudgetWebFilter filter = newFilter(Duration.ofSeconds(5));
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/sync");

        assertThrows(IllegalStateException.class, () ->
                filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {
                    throw new IllegalStateException("boom");
                }));

        RequestBudget budget = BudgetWebFilter.currentBudget(request);
        assertEquals(TerminationReason.ERROR, budget.terminationReason());
        assertNull(BudgetContext.current(), "thread restored even on failure");
    }
}
