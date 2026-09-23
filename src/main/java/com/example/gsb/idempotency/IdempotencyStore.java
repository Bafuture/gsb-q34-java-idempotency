package com.example.gsb.idempotency;

import java.time.Duration;

/**
 * Storage SPI for idempotency slots.
 *
 * <p>An implementation must make {@link #tryAcquire(String, Duration)}
 * atomic for a given key: concurrent callers observe exactly one
 * {@link AcquireOutcome.Leader} while an execution is in flight. Slots
 * expire once {@code window} has elapsed from acquisition.</p>
 *
 * <p>Slot state machine:
 * <pre>
 *   (absent/expired) --tryAcquire--> IN_PROGRESS
 *   IN_PROGRESS --complete--> SUCCESS (replayed until expiry)
 *   IN_PROGRESS --fail(cache=true)--> FAILURE (replayed until expiry)
 *   IN_PROGRESS --fail(cache=false)--> RELEASED/removed (retry allowed)
 * </pre>
 *
 * <p>To replace the in-memory implementation with an external store (Redis,
 * a database, ...) implement this interface against an atomic compare-and-set
 * primitive and point the executor builder at it; nothing else in the
 * component depends on the storage technology. See README for a Redis sketch.</p>
 */
public interface IdempotencyStore {

    /**
     * Atomically claim/look up the slot for {@code key}.
     *
     * @param window time after acquisition at which the slot expires
     * @return {@link AcquireOutcome.Leader} if this caller must run the
     *         business logic, {@link AcquireOutcome.Follower} if an execution
     *         is already in flight, or {@link AcquireOutcome.Cached} if a
     *         final result inside the window is available
     */
    AcquireOutcome tryAcquire(String key, Duration window);

    /**
     * Publish a successful result for an in-flight slot and unblock followers.
     */
    void complete(String key, Object value);

    /**
     * Publish the failure outcome for an in-flight slot.
     *
     * @param cacheFailure {@code true} caches the error for replay until
     *                     expiry; {@code false} releases the slot so another
     *                     request may retry
     */
    void fail(String key, Throwable error, boolean cacheFailure);

    /**
     * Wait up to {@code timeout} for the in-flight slot to reach a final state.
     */
    AwaitOutcome await(String key, Duration timeout) throws InterruptedException;

    /** Outcome of {@link #tryAcquire(String, Duration)}. */
    sealed interface AcquireOutcome permits AcquireOutcome.Leader, AcquireOutcome.Follower, AcquireOutcome.Cached {

        final class Leader implements AcquireOutcome {
            private static final Leader INSTANCE = new Leader();
            private Leader() {
            }
            static Leader instance() {
                return INSTANCE;
            }
        }

        final class Follower implements AcquireOutcome {
            private static final Follower INSTANCE = new Follower();
            private Follower() {
            }
            static Follower instance() {
                return INSTANCE;
            }
        }

        record Cached(StoredResult result) implements AcquireOutcome {
        }
    }

    /** Result of {@link #await(String, Duration)}. */
    sealed interface AwaitOutcome permits AwaitOutcome.Result, AwaitOutcome.Released, AwaitOutcome.Timeout {

        /** The leader finished; replay the success or failure. */
        record Result(StoredResult result) implements AwaitOutcome {
        }

        /** The slot was released without a cached failure; the caller may retry and become the leader. */
        final class Released implements AwaitOutcome {
            private static final Released INSTANCE = new Released();
            private Released() {
            }
            static Released instance() {
                return INSTANCE;
            }
        }

        /** The timeout elapsed before the leader finished. */
        final class Timeout implements AwaitOutcome {
            private static final Timeout INSTANCE = new Timeout();
            private Timeout() {
            }
            static Timeout instance() {
                return INSTANCE;
            }
        }
    }
}
