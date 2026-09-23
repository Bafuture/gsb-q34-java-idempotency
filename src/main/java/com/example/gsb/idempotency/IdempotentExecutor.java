package com.example.gsb.idempotency;

import java.time.Duration;
import java.util.Objects;

/**
 * Entry point of the idempotency component.
 *
 * <p>For every idempotency key, at most one request inside the dedup
 * window executes the business logic; concurrent followers wait (up to a
 * configurable timeout) and reuse the leader's result or failure. Create
 * instances with {@link #builder()}.</p>
 */
public final class IdempotentExecutor {

    private final IdempotencyStore store;
    private final Duration window;
    private final Duration waitTimeout;
    private final ExceptionPolicy exceptionPolicy;
    private final IdempotencyStats stats = new IdempotencyStats();

    private IdempotentExecutor(Builder builder) {
        this.store = builder.store;
        this.window = builder.window;
        this.waitTimeout = waitTimeout(builder.waitTimeout, builder.window);
        this.exceptionPolicy = builder.exceptionPolicy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public IdempotencyStats stats() {
        return stats;
    }

    /**
     * Execute {@code operation} once per {@code key} inside the dedup window.
     *
     * @throws WaitTimeoutException                the caller was a follower and the wait timeout elapsed
     * @throws IdempotentWaitInterruptedException  the follower thread was interrupted while waiting
     * @throws RuntimeException                     the business logic's runtime failure (or a
     *                                             {@link IdempotentExecutionException} wrapping a checked one),
     *                                             possibly replayed from a cached failure
     */
    public <T> T execute(String key, IdempotentOperation<T> operation) {
        Objects.requireNonNull(key, "idempotency key");
        Objects.requireNonNull(operation, "idempotent operation");

        long deadline = System.nanoTime() + waitTimeout.toNanos();
        while (true) {
            IdempotencyStore.AcquireOutcome outcome = store.tryAcquire(key, window);
            if (outcome instanceof IdempotencyStore.AcquireOutcome.Leader) {
                stats.recordMiss();
                return runAsLeader(key, operation);
            }
            if (outcome instanceof IdempotencyStore.AcquireOutcome.Cached cached) {
                stats.recordHit();
                return materialize(cached.result());
            }
            // Follower: wait for the leader, bounded by an absolute deadline.
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                stats.recordTimeout();
                throw new WaitTimeoutException(key, waitTimeout);
            }
            stats.recordJoin();
            IdempotencyStore.AwaitOutcome awaitOutcome;
            try {
                awaitOutcome = store.await(key, Duration.ofNanos(remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IdempotentWaitInterruptedException(key, e);
            }
            if (awaitOutcome instanceof IdempotencyStore.AwaitOutcome.Result r) {
                return materialize(r.result());
            }
            if (awaitOutcome instanceof IdempotencyStore.AwaitOutcome.Timeout) {
                stats.recordTimeout();
                throw new WaitTimeoutException(key, waitTimeout);
            }
            // The slot was released without a cached failure (ALLOW_RETRY):
            // loop back and either become the leader or converge onto the retry
            // that grabbed the slot first.
        }
    }

    private <T> T runAsLeader(String key, IdempotentOperation<T> operation) {
        T value;
        try {
            value = operation.execute();
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            store.fail(key, error, exceptionPolicy == ExceptionPolicy.CACHE_EXCEPTION);
            throw propagate(error);
        }
        store.complete(key, value);
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <T> T materialize(StoredResult result) {
        if (result instanceof StoredResult.Success success) {
            return (T) success.value();
        }
        throw propagate(((StoredResult.Failure) result).error());
    }

    private static RuntimeException propagate(Throwable error) {
        if (error instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (error instanceof Error e) {
            throw e;
        }
        return new IdempotentExecutionException(error);
    }

    private static Duration waitTimeout(Duration waitTimeout, Duration window) {
        if (waitTimeout == null) {
            return window;
        }
        return waitTimeout.compareTo(window) > 0 ? window : waitTimeout;
    }

    public static final class Builder {

        private IdempotencyStore store;
        private Duration window = Duration.ofMinutes(5);
        private Duration waitTimeout;
        private ExceptionPolicy exceptionPolicy = ExceptionPolicy.CACHE_EXCEPTION;

        /** Storage implementation; defaults to {@link InMemoryIdempotencyStore}. */
        public Builder store(IdempotencyStore store) {
            this.store = Objects.requireNonNull(store, "store");
            return this;
        }

        /** Dedup / result-cache window, measured from first acquisition. Must be positive. */
        public Builder window(Duration window) {
            this.window = Objects.requireNonNull(window, "window");
            return this;
        }

        /**
         * Maximum time a follower waits for the in-flight execution. Defaults to
         * the window; capped at the window (there is nothing left to wait for
         * once the slot expires).
         */
        public Builder waitTimeout(Duration waitTimeout) {
            this.waitTimeout = Objects.requireNonNull(waitTimeout, "waitTimeout");
            return this;
        }

        /** Failure strategy; defaults to {@link ExceptionPolicy#CACHE_EXCEPTION}. */
        public Builder exceptionPolicy(ExceptionPolicy exceptionPolicy) {
            this.exceptionPolicy = Objects.requireNonNull(exceptionPolicy, "exceptionPolicy");
            return this;
        }

        public IdempotentExecutor build() {
            if (window.compareTo(Duration.ZERO) <= 0) {
                throw new IllegalArgumentException("window must be positive: " + window);
            }
            if (waitTimeout != null && waitTimeout.compareTo(Duration.ZERO) <= 0) {
                throw new IllegalArgumentException("waitTimeout must be positive: " + waitTimeout);
            }
            if (store == null) {
                store = new InMemoryIdempotencyStore();
            }
            return new IdempotentExecutor(this);
        }
    }
}
