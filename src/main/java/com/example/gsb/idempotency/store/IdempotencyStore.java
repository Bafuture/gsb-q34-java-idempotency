package com.example.gsb.idempotency.store;

/**
 * Storage SPI for <b>finished</b> idempotency results.
 *
 * <p>The store only keeps terminal outcomes (a value or an exception) together with their
 * deduplication-window expiry; in-flight request coalescing is implemented inside
 * {@code Idempotency} itself with JVM-local concurrency primitives and is intentionally not part
 * of this SPI.
 *
 * <p>Implementations must be safe for concurrent use. Expired records must be treated as
 * absent by {@link #find(String, long)}.
 */
public interface IdempotencyStore {

    /**
     * Returns the fresh record stored for {@code key}, or {@code null} if none exists or the
     * existing record has expired (is outside the deduplication window).
     *
     * @param key idempotency key
     * @param now current monotonic time in nanoseconds (same clock convention as
     *            {@link System#nanoTime()})
     */
    IdempotencyRecord find(String key, long now);

    /**
     * Stores (or overwrites) the terminal record for {@code record.key()}. TTL semantics may be
     * enforced eagerly by the implementation (e.g. a Redis {@code SET key value PX ttl}) or
     * lazily on lookup, as the in-memory implementation does.
     */
    void save(IdempotencyRecord record);

    /**
     * Removes the record for {@code key}, if any. Used by the
     * {@code ExceptionStrategy.ALLOW_RETRY} policy so that a failed attempt can be retried, and
     * exposed for manual invalidation.
     */
    void remove(String key);
}
