package com.example.gsb.idempotency;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration for {@link Idempotency}.
 *
 * <ul>
 *   <li>{@code dedupWindow}: how long a finished result (success or, depending on the policy,
 *       failure) is replayed for the same key.</li>
 *   <li>{@code waitTimeout}: how long a coalesced request waits for the in-flight leader before
 *       giving up with {@link IdempotencyWaitTimeoutException}.</li>
 *   <li>{@code exceptionStrategy}: whether a failed execution is cached for the whole window or
 *       removed so the next caller may retry.</li>
 *   <li>{@code maxInFlightDuration}: safety lease for the local in-flight slot. An entry older
 *       than this is considered abandoned (e.g. the leader's JVM died mid-execution) and can be
 *       preempted by a new caller. Defaults to {@link Long#MAX_VALUE} nanoseconds (disabled).</li>
 * </ul>
 */
public final class IdempotencyConfig {

    private final Duration dedupWindow;
    private final Duration waitTimeout;
    private final ExceptionStrategy exceptionStrategy;
    private final Duration maxInFlightDuration;
    private final TimeSource timeSource;

    private IdempotencyConfig(Builder builder) {
        this.dedupWindow = Objects.requireNonNull(builder.dedupWindow, "dedupWindow");
        this.waitTimeout = Objects.requireNonNull(builder.waitTimeout, "waitTimeout");
        this.exceptionStrategy = Objects.requireNonNull(builder.exceptionStrategy, "exceptionStrategy");
        this.maxInFlightDuration =
                Objects.requireNonNull(builder.maxInFlightDuration, "maxInFlightDuration");
        this.timeSource = Objects.requireNonNull(builder.timeSource, "timeSource");
        if (dedupWindow.isZero() || dedupWindow.isNegative()) {
            throw new IllegalArgumentException("dedupWindow must be positive: " + dedupWindow);
        }
        if (waitTimeout.isZero() || waitTimeout.isNegative()) {
            throw new IllegalArgumentException("waitTimeout must be positive: " + waitTimeout);
        }
        if (maxInFlightDuration.isZero() || maxInFlightDuration.isNegative()) {
            throw new IllegalArgumentException(
                    "maxInFlightDuration must be positive: " + maxInFlightDuration);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public Duration getDedupWindow() {
        return dedupWindow;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }

    public ExceptionStrategy getExceptionStrategy() {
        return exceptionStrategy;
    }

    public Duration getMaxInFlightDuration() {
        return maxInFlightDuration;
    }

    public TimeSource getTimeSource() {
        return timeSource;
    }

    public static final class Builder {
        private Duration dedupWindow;
        private Duration waitTimeout = Duration.ofSeconds(10);
        private ExceptionStrategy exceptionStrategy = ExceptionStrategy.CACHE_EXCEPTION;
        private Duration maxInFlightDuration = Duration.ofNanos(Long.MAX_VALUE);
        private TimeSource timeSource = TimeSource.SYSTEM;

        /** Required: how long finished results are replayed for the same key. */
        public Builder dedupWindow(Duration window) {
            this.dedupWindow = window;
            return this;
        }

        /** Max wait time for coalesced requests on an in-flight key. Defaults to 10s. */
        public Builder waitTimeout(Duration timeout) {
            this.waitTimeout = timeout;
            return this;
        }

        /** Failure handling strategy. Defaults to {@link ExceptionStrategy#CACHE_EXCEPTION}. */
        public Builder exceptionStrategy(ExceptionStrategy strategy) {
            this.exceptionStrategy = strategy;
            return this;
        }

        /**
         * Safety lease after which an in-flight slot may be preempted. Disabled by default:
         * within one process the leader always runs the normal finally cleanup, so a slot only
         * gets stuck if a JVM dies, which is irrelevant for the single-process in-memory case.
         */
        public Builder maxInFlightDuration(Duration duration) {
            this.maxInFlightDuration = duration;
            return this;
        }

        /** Inject a time source (mainly for tests). Defaults to the system nano clock. */
        public Builder timeSource(TimeSource source) {
            this.timeSource = source;
            return this;
        }

        public IdempotencyConfig build() {
            if (dedupWindow == null) {
                throw new IllegalStateException("dedupWindow is required");
            }
            return new IdempotencyConfig(this);
        }
    }
}
