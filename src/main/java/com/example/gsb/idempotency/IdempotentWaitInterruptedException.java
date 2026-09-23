package com.example.gsb.idempotency;

/**
 * Thrown when a follower thread is interrupted while waiting for the
 * in-flight execution. The caller's interrupt status is restored before
 * this exception is thrown.
 */
public class IdempotentWaitInterruptedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String key;

    public IdempotentWaitInterruptedException(String key, InterruptedException cause) {
        super("Interrupted while waiting for in-flight execution of idempotency key: " + key, cause);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
