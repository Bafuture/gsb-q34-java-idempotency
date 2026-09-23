package com.example.gsb.idempotency;

/**
 * How a failed first execution is treated for later requests within the
 * dedup window.
 */
public enum ExceptionPolicy {

    /** Cache the failure; every later request in the window rethrows it. */
    CACHE_EXCEPTION,

    /** Release the slot; a later request (including waiting followers) retries. */
    ALLOW_RETRY
}
