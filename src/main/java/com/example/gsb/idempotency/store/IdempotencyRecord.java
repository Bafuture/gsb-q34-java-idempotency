package com.example.gsb.idempotency.store;

/**
 * The terminal outcome of one leader execution, stored for replay within the deduplication
 * window.
 *
 * @param key          idempotency key
 * @param success      whether the action returned normally
 * @param value        action return value when {@code success == true}; {@code null} otherwise
 *                     (and also a valid return value on success)
 * @param error        thrown exception when {@code success == false}; {@code null} otherwise
 * @param expireAtNanos {@link System#nanoTime()} value at which the record leaves the window
 */
public record IdempotencyRecord(
        String key,
        boolean success,
        Object value,
        Throwable error,
        long expireAtNanos) {

    public static IdempotencyRecord success(String key, Object value, long expireAtNanos) {
        return new IdempotencyRecord(key, true, value, null, expireAtNanos);
    }

    public static IdempotencyRecord failure(String key, Throwable error, long expireAtNanos) {
        return new IdempotencyRecord(key, false, null, error, expireAtNanos);
    }

    /** Whether this record is still inside the deduplication window according to {@code now}. */
    public boolean isFresh(long now) {
        return now - expireAtNanos < 0L;
    }
}
