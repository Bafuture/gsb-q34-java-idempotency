package com.example.gsb.idempotency;

/**
 * Business logic guarded by the idempotency component.
 *
 * @param <T> result type
 */
@FunctionalInterface
public interface IdempotentAction<T> {

    /**
     * Executes the (non-idempotent) business logic. Only invoked by the single leader request
     * for a given key; coalesced and cache-hit callers reuse its return value.
     */
    T execute() throws Exception;
}
