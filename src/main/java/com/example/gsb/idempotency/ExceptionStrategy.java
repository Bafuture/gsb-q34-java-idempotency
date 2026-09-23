package com.example.gsb.idempotency;

/**
 * How failed executions are treated inside the deduplication window.
 */
public enum ExceptionStrategy {

    /**
     * The exception is cached for the whole deduplication window. Every subsequent caller with
     * the same key gets the same exception replayed and the business logic is never re-run.
     */
    CACHE_EXCEPTION,

    /**
     * The failure is not cached: after the leader's exception has been delivered to coalesced
     * waiters, the stored record is removed and the next caller with the same key becomes a new
     * leader and gets to retry the business logic.
     */
    ALLOW_RETRY
}
