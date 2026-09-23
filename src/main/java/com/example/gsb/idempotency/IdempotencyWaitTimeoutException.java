package com.example.gsb.idempotency;

/**
 * Thrown to a coalesced waiter when the in-flight leader has not produced a result within the
 * configured {@code waitTimeout}.
 *
 * <p>The waiter gets a <em>definite</em> outcome (this exception) instead of blocking forever;
 * the leader keeps running and its result is still cached for later callers when it finishes.
 */
public class IdempotencyWaitTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IdempotencyWaitTimeoutException(String key, long timeoutMillis) {
        super("Timeout after " + timeoutMillis + "ms while waiting for the in-flight execution "
                + "of idempotency key '" + key + "'");
    }
}
