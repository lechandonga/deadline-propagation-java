package com.github.lechandonga.deadlinepropagationjava.budget;

/** 预算/取消相关异常的统一基类，子类型用于区分终止原因。 */
public class BudgetException extends RuntimeException {

    private final TerminationReason reason;

    public BudgetException(TerminationReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public TerminationReason reason() {
        return reason;
    }

    /** 截止时间到达。 */
    public static final class DeadlineExceeded extends BudgetException {
        public DeadlineExceeded(String message) {
            super(TerminationReason.DEADLINE_EXCEEDED, message);
        }
    }

    /** 调用方主动取消。 */
    public static final class Cancelled extends BudgetException {
        public Cancelled(String message) {
            super(TerminationReason.CANCELLED_BY_CALLER, message);
        }
    }

    /** 剩余预算耗尽，拒绝排队新工作（不得静默失败）。 */
    public static final class Rejected extends BudgetException {
        public Rejected(String message) {
            super(TerminationReason.REJECTED_BUDGET_EXHAUSTED, message);
        }
    }
}
