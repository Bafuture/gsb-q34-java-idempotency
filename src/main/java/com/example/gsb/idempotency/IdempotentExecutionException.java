package com.example.gsb.idempotency;

/**
 * Unchecked wrapper used when a business operation throws a checked
 * exception (either during the first execution or replayed from a cached
 * failure). The original throwable is available via {@link #getCause()}.
 */
public class IdempotentExecutionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IdempotentExecutionException(Throwable cause) {
        super(cause);
    }
}
