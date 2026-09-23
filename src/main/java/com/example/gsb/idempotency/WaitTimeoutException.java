package com.example.gsb.idempotency;

import java.time.Duration;

/**
 * Thrown when a follower request waited for the in-flight execution longer
 * than the configured wait timeout. Callers receive a definite outcome
 * instead of blocking forever.
 */
public class WaitTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String key;
    private final Duration waitTimeout;

    public WaitTimeoutException(String key, Duration waitTimeout) {
        super("Timed out after " + waitTimeout + " while waiting for in-flight execution of idempotency key: " + key);
        this.key = key;
        this.waitTimeout = waitTimeout;
    }

    public String getKey() {
        return key;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }
}
