package com.github.lechandonga.deadlinepropagationjava.budget;

import java.time.Duration;

/**
 * 单调递减的请求截止时间预算。
 *
 * <p>基于 {@link System#nanoTime()} 计算剩余时间，不受系统时钟回拨影响；
 * 预算只减不增——任何阶段超时后都不会重新获得完整预算。
 */
public final class Deadline {

    private final long deadlineNanos;

    private Deadline(long deadlineNanos) {
        this.deadlineNanos = deadlineNanos;
    }

    /** 从当前时刻起，给定整体预算构造截止时间。 */
    public static Deadline after(Duration budget) {
        if (budget == null || budget.isNegative() || budget.isZero()) {
            throw new IllegalArgumentException("budget must be positive");
        }
        return new Deadline(System.nanoTime() + budget.toNanos());
    }

    /** 剩余预算；耗尽后返回 {@link Duration#ZERO}，永不为负。 */
    public Duration remaining() {
        long left = deadlineNanos - System.nanoTime();
        return left <= 0 ? Duration.ZERO : Duration.ofNanos(left);
    }

    public long remainingMillis() {
        return remaining().toMillis();
    }

    public boolean isExpired() {
        return System.nanoTime() >= deadlineNanos;
    }

    /** 剩余预算耗尽时抛出 {@link BudgetException.DeadlineExceeded}，否则返回剩余毫秒数。 */
    public long checkRemaining(String stage) {
        if (isExpired()) {
            throw new BudgetException.DeadlineExceeded(
                    "budget exhausted before stage '" + stage + "'");
        }
        return remainingMillis();
    }
}
