package com.example.gsb.idempotency.store;

import com.example.gsb.idempotency.TimeSource;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-JVM {@link IdempotencyStore} backed by a {@link ConcurrentHashMap}.
 *
 * <p>Expiration is checked lazily on {@link #find(String, long)}: an expired entry is deleted
 * and reported as absent. There is no background cleanup thread, so keys that are never looked
 * up again may linger; use {@code remove} / a bounded map if that matters.
 *
 * <p>An external store (Redis, a database table, ...) replaces this class by implementing
 * {@link IdempotencyStore}; see the project README for details.
 */
public final class InMemoryIdempotencyStore implements IdempotencyStore {

    private final ConcurrentHashMap<String, IdempotencyRecord> records = new ConcurrentHashMap<>();
    private final TimeSource timeSource;

    public InMemoryIdempotencyStore() {
        this(TimeSource.SYSTEM);
    }

    public InMemoryIdempotencyStore(TimeSource timeSource) {
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    }

    @Override
    public IdempotencyRecord find(String key, long now) {
        Objects.requireNonNull(key, "key");
        IdempotencyRecord record = records.get(key);
        if (record == null) {
            return null;
        }
        if (!record.isFresh(now)) {
            records.remove(key, record);
            return null;
        }
        return record;
    }

    @Override
    public void save(IdempotencyRecord record) {
        Objects.requireNonNull(record, "record");
        records.put(record.key(), record);
    }

    @Override
    public void remove(String key) {
        Objects.requireNonNull(key, "key");
        records.remove(key);
    }

    /** Current number of entries (including possibly expired-but-not-yet-swept ones). */
    public int size() {
        return records.size();
    }

    /** Monotonic time source used when callers do not pass an explicit {@code now}. */
    public long now() {
        return timeSource.nanoTime();
    }
}
