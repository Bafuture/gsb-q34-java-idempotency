package com.example.gsb.idempotency;

/**
 * Business operation guarded by the idempotency component. Allows checked
 * exceptions: a checked failure is wrapped into
 * {@link IdempotentExecutionException} when rethrown to callers.
 */
@FunctionalInterface
public interface IdempotentOperation<T> {

    T execute() throws Exception;
}
