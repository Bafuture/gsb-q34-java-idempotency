package com.example.gsb.idempotency.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.gsb.idempotency.TimeSource;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

class InMemoryIdempotencyStoreTest {

    private static final class FixedClock implements TimeSource {
        private final AtomicLong value;

        private FixedClock(long value) {
            this.value = new AtomicLong(value);
        }

        @Override
        public long nanoTime() {
            return value.get();
        }

        void set(long v) {
            value.set(v);
        }
    }

    @Test
    void findReturnsNullWhenAbsentOrExpiredAndRemoveDropsRecord() {
        FixedClock clock = new FixedClock(0L);
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(clock);

        assertThat(store.find("k", 0L)).isNull();

        store.save(IdempotencyRecord.success("k", "v", 1_000L));
        assertThat(store.find("k", 999L)).isNotNull();

        // Exactly at expiry (now == expireAt) the record is stale.
        assertThat(store.find("k", 1_000L)).isNull();

        store.save(IdempotencyRecord.failure("k2", new IllegalStateException("x"), 5_000L));
        IdempotencyRecord failure = store.find("k2", 1_000L);
        assertThat(failure).isNotNull();
        assertThat(failure.success()).isFalse();
        assertThat(failure.error()).isInstanceOf(IllegalStateException.class);

        store.remove("k2");
        assertThat(store.find("k2", 1_000L)).isNull();
    }
}
