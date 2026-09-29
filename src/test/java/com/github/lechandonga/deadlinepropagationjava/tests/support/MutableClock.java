package com.github.lechandonga.deadlinepropagationjava.tests.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/** Deterministic clock shared by budget/retry tests; no real waiting needed. */
public final class MutableClock extends Clock {
    private Instant now = Instant.parse("2026-09-29T00:00:00Z");

    public void advance(Duration d) {
        now = now.plus(d);
    }

    @Override
    public Instant instant() {
        return now;
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
