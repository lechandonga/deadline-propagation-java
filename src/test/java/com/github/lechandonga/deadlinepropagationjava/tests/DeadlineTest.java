package com.github.lechandonga.deadlinepropagationjava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.lechandonga.deadlinepropagationjava.budget.BudgetException;
import com.github.lechandonga.deadlinepropagationjava.budget.Deadline;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class DeadlineTest {

    @Test
    void budgetOnlyDecreases() throws InterruptedException {
        Deadline deadline = Deadline.after(Duration.ofMillis(500));
        long previous = deadline.remainingMillis();
        for (int i = 0; i < 5; i++) {
            Thread.sleep(20);
            long current = deadline.remainingMillis();
            assertThat(current).isLessThanOrEqualTo(previous);
            previous = current;
        }
        assertThat(previous).isLessThan(500);
    }

    @Test
    void remainingNeverNegativeAfterExpiry() throws InterruptedException {
        Deadline deadline = Deadline.after(Duration.ofMillis(30));
        Thread.sleep(80);
        assertThat(deadline.isExpired()).isTrue();
        assertThat(deadline.remaining()).isEqualTo(Duration.ZERO);
        assertThat(deadline.remainingMillis()).isZero();
    }

    @Test
    void checkRemainingThrowsAfterExpiry() throws InterruptedException {
        Deadline deadline = Deadline.after(Duration.ofMillis(30));
        Thread.sleep(80);
        assertThatThrownBy(() -> deadline.checkRemaining("stage-x"))
                .isInstanceOf(BudgetException.DeadlineExceeded.class);
    }

    @Test
    void rejectsNonPositiveBudget() {
        assertThatThrownBy(() -> Deadline.after(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Deadline.after(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
