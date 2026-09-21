package com.github.lechandonga.deadlinepropagationjava.budget;

/** 终止原因：超时、调用方取消、客户端取消、内部错误、预算耗尽拒绝 必须可区分。 */
public enum TerminationReason {
    NONE,
    DEADLINE_EXCEEDED,
    CANCELLED_BY_CALLER,
    CLIENT_DISCONNECTED,
    REJECTED_BUDGET_EXHAUSTED,
    INTERNAL_ERROR
}
