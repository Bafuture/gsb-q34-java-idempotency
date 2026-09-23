package com.example.gsb.idempotency;

import java.util.concurrent.atomic.LongAdder;

/**
 * Component statistics. All counters are monotonic and thread-safe.
 *
 * <ul>
 *   <li>{@code hits}: a stored, in-window result (value or cached exception) was replayed.</li>
 *   <li>{@code misses}: no in-window result existed and this request became the leader that
 *       actually executed the business logic.</li>
 *   <li>{@code coalescedWaits}: the request found an in-flight execution for the same key and
 *       waited on it, whether or not the wait ultimately succeeded.</li>
 *   <li>{@code timeouts}: a coalesced waiter gave up because the leader did not finish within
 *       the configured wait timeout.</li>
 * </ul>
 */
public final class IdempotencyStats {

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder coalescedWaits = new LongAdder();
    private final LongAdder timeouts = new LongAdder();

    void recordHit() {
        hits.increment();
    }

    void recordMiss() {
        misses.increment();
    }

    void recordCoalescedWait() {
        coalescedWaits.increment();
    }

    void recordTimeout() {
        timeouts.increment();
    }

    public long getHits() {
        return hits.sum();
    }

    public long getMisses() {
        return misses.sum();
    }

    public long getCoalescedWaits() {
        return coalescedWaits.sum();
    }

    public long getTimeouts() {
        return timeouts.sum();
    }

    public Snapshot snapshot() {
        return new Snapshot(getHits(), getMisses(), getCoalescedWaits(), getTimeouts());
    }

    /** Immutable point-in-time view of all counters. */
    public record Snapshot(long hits, long misses, long coalescedWaits, long timeouts) {
        /** Total requests observed by the component. */
        public long total() {
            return hits + misses + coalescedWaits;
        }
    }
}
