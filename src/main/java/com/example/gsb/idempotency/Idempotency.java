package com.example.gsb.idempotency;

import com.example.gsb.idempotency.store.IdempotencyRecord;
import com.example.gsb.idempotency.store.IdempotencyStore;
import com.example.gsb.idempotency.store.InMemoryIdempotencyStore;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Idempotency control component.
 *
 * <p>For a caller-supplied idempotency key it guarantees, within this JVM:
 * <ol>
 *   <li><b>Result caching</b>: a finished result is replayed for every same-key call inside the
 *       configured deduplication window; after the window elapses the business logic may run
 *       again.</li>
 *   <li><b>Concurrent coalescing</b>: when several same-key requests arrive while no cached
 *       result exists, exactly one (the leader) executes the action; all others wait on its
 *       {@link CompletableFuture} and reuse its value/exception.</li>
 *   <li><b>Bounded waiting</b>: a waiter that is interrupted, or whose configured wait timeout
 *       elapses, receives a definite outcome and never blocks forever.</li>
 * </ol>
 *
 * <p>Finished results live in the pluggable {@link IdempotencyStore}; the in-flight registry
 * ({@link InFlight}) is always local to this instance. See the project README for the
 * multi-instance / external-store boundary.
 */
public final class Idempotency {

    /** Sentinel stored in futures/records so that a {@code null} action result round-trips. */
    private static final Object NULL = new Object();

    private final IdempotencyStore store;
    private final IdempotencyConfig config;
    private final IdempotencyStats stats = new IdempotencyStats();

    /** Key -> in-flight execution. An entry exists exactly while a leader is executing. */
    private final ConcurrentHashMap<String, InFlight> inFlight = new ConcurrentHashMap<>();

    public Idempotency(IdempotencyConfig config) {
        this(config, new InMemoryIdempotencyStore(config.getTimeSource()));
    }

    public Idempotency(IdempotencyConfig config, IdempotencyStore store) {
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
    }

    /**
     * Executes {@code action} once per key per deduplication window, coalescing concurrent
     * same-key requests onto the leader's result.
     *
     * @return the leader's result, or the cached/replayed result
     * @throws Exception the leader's original exception, rethrown to coalesced waiters and
     *                   (under {@link ExceptionStrategy#CACHE_EXCEPTION}) to later callers;
     *                   {@link IdempotencyWaitTimeoutException} when a waiter times out
     */
    @SuppressWarnings("unchecked")
    public <T> T execute(String key, IdempotentAction<T> action) throws Exception {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(action, "action");

        while (true) {
            final long now = config.getTimeSource().nanoTime();

            // 1) Finished, still-in-window result: plain cache hit.
            IdempotencyRecord cached = store.find(key, now);
            if (cached != null) {
                stats.recordHit();
                return (T) replay(cached);
            }

            // 2) Race to register the single in-flight execution for this key.
            InFlight fresh = new InFlight(now);
            InFlight existing = inFlight.putIfAbsent(key, fresh);
            if (existing == null) {
                // We won the slot race; verify no leader finished in the gap between our
                // cache lookup and slot registration before declaring ourselves the leader.
                return claimLeadership(key, fresh, action);
            }
            if (existing.isExpired(now, config) && inFlight.replace(key, existing, fresh)) {
                // The previous leader abandoned the slot (safety lease elapsed, e.g. its JVM
                // died): this request preempts it and becomes the new leader.
                return claimLeadership(key, fresh, action);
            }

            // 3) Follower: join the in-flight execution, bounded by the wait timeout.
            stats.recordCoalescedWait();
            Throwable leaderFailure = awaitLeader(key, existing);
            if (leaderFailure == null) {
                // Leader completed normally: its success value is now in the store.
                IdempotencyRecord done = store.find(key, config.getTimeSource().nanoTime());
                if (done != null) {
                    stats.recordHit();
                    return (T) replay(done);
                }
                // Defensive: success always stores a record, so this should be unreachable
                // with a well-behaved store; loop and re-resolve the key if it happens.
            } else {
                // Leader failed: waiters always receive the leader's exact exception. Under
                // CACHE_EXCEPTION the failure is also replayed to later callers; under
                // ALLOW_RETRY the next caller becomes a new leader, but this call is a
                // coalesced waiter of the failed attempt and does not retry inline.
                throw rethrow(leaderFailure);
            }
        }
    }

    /**
     * Double-check performed by a request that just acquired an in-flight slot.
     *
     * <p>Closes the check-then-register race: the previous leader may have stored its result
     * and released its slot <em>after</em> our {@code store.find} but <em>before</em> our
     * {@code putIfAbsent}. The store write happens-before slot release happens-before our
     * successful registration, so this second lookup is guaranteed to observe it. If found, we
     * hand the cached outcome to anyone parked on our slot instead of executing a second time.
     */
    @SuppressWarnings("unchecked")
    private <T> T claimLeadership(String key, InFlight slot, IdempotentAction<T> action)
            throws Exception {
        IdempotencyRecord raced = store.find(key, config.getTimeSource().nanoTime());
        if (raced != null) {
            stats.recordHit();
            handOffCached(key, slot, raced);
            return (T) replay(raced);
        }
        stats.recordMiss();
        return runAsLeader(key, slot, action);
    }

    /** Releases our slot and wakes anyone parked on it toward the already-cached outcome. */
    private void handOffCached(String key, InFlight slot, IdempotencyRecord record) {
        inFlight.remove(key, slot);
        if (record.success()) {
            slot.future.complete(null);
        } else {
            slot.future.completeExceptionally(record.error());
        }
    }

    private <T> T runAsLeader(String key, InFlight slot, IdempotentAction<T> action)
            throws Exception {
        T result;
        try {
            result = action.execute();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                // Preserve the interrupt status on the executing thread for its caller.
                Thread.currentThread().interrupt();
            }
            finishFailure(key, slot, e);
            throw e;
        } catch (Error e) {
            finishFailure(key, slot, e);
            throw e;
        }
        finishSuccess(key, slot, result);
        return result;
    }

    private void finishSuccess(String key, InFlight slot, Object result) {
        long expireAt = config.getTimeSource().nanoTime() + config.getDedupWindow().toNanos();
        store.save(IdempotencyRecord.success(key, result == null ? NULL : result, expireAt));
        // Unblock waiters only after the outcome is durably visible in the store.
        inFlight.remove(key, slot);
        slot.future.complete(null);
    }

    private void finishFailure(String key, InFlight slot, Throwable error) {
        if (config.getExceptionStrategy() == ExceptionStrategy.CACHE_EXCEPTION) {
            long expireAt =
                    config.getTimeSource().nanoTime() + config.getDedupWindow().toNanos();
            store.save(IdempotencyRecord.failure(key, error, expireAt));
        } else {
            // No cached failure: drop anything stale so the next caller may retry.
            store.remove(key);
        }
        inFlight.remove(key, slot);
        slot.future.completeExceptionally(error);
    }

    /**
     * Waits bounded on the leader's future.
     *
     * @return {@code null} if the leader completed successfully; otherwise the leader's
     *         original throwable (to be re-delivered to this waiter)
     * @throws IdempotencyWaitTimeoutException the wait timeout elapsed before completion
     * @throws InterruptedException this waiter thread was interrupted while waiting
     */
    private Throwable awaitLeader(String key, InFlight slot) throws InterruptedException {
        try {
            slot.future.get(config.getWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return null;
        } catch (TimeoutException e) {
            stats.recordTimeout();
            throw new IdempotencyWaitTimeoutException(
                    key, config.getWaitTimeout().toMillis());
        } catch (InterruptedException e) {
            // Definite outcome, not a swallowed/reset wait: keep the interrupt flag and
            // surface the interruption to the caller.
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private Object replay(IdempotencyRecord record) throws Exception {
        if (record.success()) {
            Object value = record.value();
            return value == NULL ? null : value;
        }
        throw rethrow(record.error());
    }

    private static Exception rethrow(Throwable t) throws Exception {
        if (t instanceof Exception e) {
            throw e;
        }
        if (t instanceof Error e) {
            throw e;
        }
        throw new RuntimeException(t);
    }

    /** Manually drops any stored result for a key, allowing the next call to execute again. */
    public void invalidate(String key) {
        store.remove(key);
    }

    public IdempotencyStats stats() {
        return stats;
    }

    public IdempotencyConfig getConfig() {
        return config;
    }

    /** One in-flight leader execution plus its start time for the safety lease. */
    private static final class InFlight {
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        private final long startedAtNanos;

        private InFlight(long startedAtNanos) {
            this.startedAtNanos = startedAtNanos;
        }

        private boolean isExpired(long now, IdempotencyConfig config) {
            return now - startedAtNanos >= config.getMaxInFlightDuration().toNanos();
        }
    }
}
