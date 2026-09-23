package com.example.gsb.idempotency;

/**
 * Final, replayable outcome of an execution slot.
 */
public sealed interface StoredResult permits StoredResult.Success, StoredResult.Failure {

    record Success(Object value) implements StoredResult {
    }

    record Failure(Throwable error) implements StoredResult {
    }
}
