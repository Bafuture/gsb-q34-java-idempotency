package com.example.gsb.idempotency;

import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe runtime statistics of an {@link IdempotentExecutor}.
 */
public final class IdempotencyStats {

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder joins = new LongAdder();
    private final LongAdder timeouts = new LongAdder();

    /** Request was served from a cached result inside the window. */
    public long hits() {
        return hits.sum();
    }

    /** Request became the leader and actually executed the business logic. */
    public long misses() {
        return misses.sum();
    }

    /** Request found an in-flight execution and had to wait for it. */
    public long joins() {
        return joins.sum();
    }

    /** Follower gave up waiting because the wait timeout elapsed. */
    public long timeouts() {
        return timeouts.sum();
    }

    void recordHit() {
        hits.increment();
    }

    void recordMiss() {
        misses.increment();
    }

    void recordJoin() {
        joins.increment();
    }

    void recordTimeout() {
        timeouts.increment();
    }

    /** Immutable point-in-time snapshot. */
    public record Snapshot(long hits, long misses, long joins, long timeouts) {
    }

    public Snapshot snapshot() {
        return new Snapshot(hits(), misses(), joins(), timeouts());
    }
}
