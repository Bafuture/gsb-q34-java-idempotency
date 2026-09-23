package com.example.gsb.idempotency;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Single-JVM {@link IdempotencyStore} backed by a {@link ConcurrentHashMap}
 * of per-key slots. Suitable for single-process deployments and tests; it is
 * not shared across JVMs — use an external-store implementation for a
 * multi-node deployment.
 */
public final class InMemoryIdempotencyStore implements IdempotencyStore {

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final LongSupplier nanoTime;

    public InMemoryIdempotencyStore() {
        this(System::nanoTime);
    }

    /** Test seam: allows driving the expiry clock deterministically. */
    InMemoryIdempotencyStore(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
    }

    @Override
    public AcquireOutcome tryAcquire(String key, Duration window) {
        long now = nanoTime.getAsLong();
        synchronized (this) {
            Entry entry = entries.get(key);
            if (entry != null && entry.expiresAt <= now) {
                entries.remove(key, entry);
                entry = null;
            }
            if (entry == null) {
                entries.put(key, new Entry(now + window.toNanos()));
                return AcquireOutcome.Leader.instance();
            }
            return switch (entry.state) {
                case IN_PROGRESS -> AcquireOutcome.Follower.instance();
                case SUCCESS -> new AcquireOutcome.Cached(new StoredResult.Success(entry.value));
                case FAILURE -> new AcquireOutcome.Cached(new StoredResult.Failure(entry.error));
                case RELEASED -> AcquireOutcome.Follower.instance();
            };
        }
    }

    @Override
    public void complete(String key, Object value) {
        Entry entry = entries.get(key);
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            entry.value = value;
            entry.state = Entry.State.SUCCESS;
            entry.notifyAll();
        }
    }

    @Override
    public void fail(String key, Throwable error, boolean cacheFailure) {
        Entry entry = entries.get(key);
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            if (cacheFailure) {
                entry.error = error;
                entry.state = Entry.State.FAILURE;
            } else {
                entry.state = Entry.State.RELEASED;
                entries.remove(key, entry);
            }
            entry.notifyAll();
        }
    }

    @Override
    public AwaitOutcome await(String key, Duration timeout) throws InterruptedException {
        Entry entry = entries.get(key);
        if (entry == null) {
            return AwaitOutcome.Released.instance();
        }
        long deadline = nanoTime.getAsLong() + timeout.toNanos();
        synchronized (entry) {
            while (entry.state == Entry.State.IN_PROGRESS) {
                long remaining = deadline - nanoTime.getAsLong();
                if (remaining <= 0) {
                    return AwaitOutcome.Timeout.instance();
                }
                long millis = remaining / 1_000_000L;
                int nanos = (int) (remaining % 1_000_000L);
                entry.wait(millis, nanos);
            }
            return switch (entry.state) {
                case SUCCESS -> new AwaitOutcome.Result(new StoredResult.Success(entry.value));
                case FAILURE -> new AwaitOutcome.Result(new StoredResult.Failure(entry.error));
                case RELEASED -> AwaitOutcome.Released.instance();
                case IN_PROGRESS -> AwaitOutcome.Timeout.instance();
            };
        }
    }

    private static final class Entry {

        enum State {
            IN_PROGRESS, SUCCESS, FAILURE, RELEASED
        }

        State state = State.IN_PROGRESS;
        final long expiresAt;
        Object value;
        Throwable error;

        Entry(long expiresAt) {
            this.expiresAt = expiresAt;
        }
    }
}
