package com.example.gsb.idempotency;

/**
 * Monotonic time source used for every TTL / window computation inside the component.
 *
 * <p>The default implementation delegates to {@link System#nanoTime()}. Tests can inject a
 * controllable implementation to deterministically simulate window expiration without sleeping.
 */
@FunctionalInterface
public interface TimeSource {

    /** Returns the current value, in nanoseconds, of a monotonically increasing clock. */
    long nanoTime();

    /** Default time source backed by {@link System#nanoTime()}. */
    TimeSource SYSTEM = System::nanoTime;
}
