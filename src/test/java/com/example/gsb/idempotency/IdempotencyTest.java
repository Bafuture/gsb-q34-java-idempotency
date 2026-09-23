package com.example.gsb.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Required scenario coverage:
 * <ol>
 *   <li>serial duplicates hit the cache, business runs once;</li>
 *   <li>concurrent same-key requests coalesce onto a single execution;</li>
 *   <li>after the deduplication window elapses the action can run again;</li>
 *   <li>exception policy: cache the exception vs allow retry;</li>
 *   <li>waiters get a definite timeout when the leader runs too long;</li>
 *   <li>interrupting the leader delivers a definite outcome to everyone (no permanent block).</li>
 * </ol>
 */
class IdempotencyTest {

    private static IdempotencyConfig.Builder baseConfig() {
        return IdempotencyConfig.builder()
                .dedupWindow(Duration.ofMinutes(10))
                .waitTimeout(Duration.ofSeconds(5));
    }

    private static void awaitLatch(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(5, TimeUnit.SECONDS))
                .as("synchronization latch reached within 5s")
                .isTrue();
    }

    private static void waitForCoalesced(Idempotency idempotency, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (idempotency.stats().getCoalescedWaits() < expected
                && System.nanoTime() < deadline) {
            Thread.sleep(2);
        }
        assertThat(idempotency.stats().getCoalescedWaits())
                .as("%d followers parked on the in-flight leader", expected)
                .isEqualTo(expected);
    }

    /** A controllable monotonic clock for deterministic window-expiration tests. */
    private static final class MutableClock implements TimeSource {
        private final AtomicLong now = new AtomicLong();

        @Override
        public long nanoTime() {
            return now.get();
        }

        void advance(Duration duration) {
            now.addAndGet(duration.toNanos());
        }
    }

    @Test
    void serialDuplicatesExecuteActionOnceAndReplayCachedResult() throws Exception {
        Idempotency idempotency = new Idempotency(baseConfig().build());
        AtomicInteger executions = new AtomicInteger();

        IdempotentAction<String> action = () -> "result-" + executions.incrementAndGet();

        assertThat(idempotency.execute("k1", action)).isEqualTo("result-1");
        assertThat(idempotency.execute("k1", action)).isEqualTo("result-1");
        assertThat(idempotency.execute("k1", action)).isEqualTo("result-1");

        assertThat(executions).hasValue(1);
        assertThat(idempotency.stats().getMisses()).isEqualTo(1);
        assertThat(idempotency.stats().getHits()).isEqualTo(2);
        assertThat(idempotency.stats().getCoalescedWaits()).isZero();
        assertThat(idempotency.stats().getTimeouts()).isZero();
    }

    @Test
    void nullReturnValueIsCachedAndReplayed() throws Exception {
        Idempotency idempotency = new Idempotency(baseConfig().build());
        AtomicInteger executions = new AtomicInteger();

        IdempotentAction<String> returnsNull = () -> {
            executions.incrementAndGet();
            return null;
        };

        assertThat(idempotency.execute("null-key", returnsNull)).isNull();
        assertThat(idempotency.execute("null-key", returnsNull)).isNull();
        assertThat(executions).hasValue(1);
    }

    @Test
    void concurrentSameKeyRequestsCoalesceIntoSingleExecution() throws Exception {
        final int threads = 16;
        Idempotency idempotency = new Idempotency(baseConfig().build());
        AtomicInteger executions = new AtomicInteger();

        CountDownLatch leaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLeader = new CountDownLatch(1);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    awaitLatch(start);
                    return idempotency.execute("only", () -> {
                        executions.incrementAndGet();
                        leaderStarted.countDown();
                        awaitLatch(releaseLeader);
                        return "ok";
                    });
                }));
            }

            start.countDown();
            awaitLatch(leaderStarted);
            // Deterministically wait until every non-leader request is parked on the future.
            waitForCoalesced(idempotency, threads - 1);
            releaseLeader.countDown();

            for (Future<String> future : futures) {
                assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo("ok");
            }
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(executions)
                .as("business logic executed exactly once under %d concurrent requests", threads)
                .hasValue(1);
        assertThat(idempotency.stats().getMisses()).isEqualTo(1);
        assertThat(idempotency.stats().getCoalescedWaits()).isEqualTo(threads - 1);
        assertThat(idempotency.stats().getTimeouts()).isZero();

        // After the burst, the cached result is still replayed serially.
        assertThat(idempotency.execute("only", () -> {
            executions.incrementAndGet();
            return "second";
        })).isEqualTo("ok");
        assertThat(executions).hasValue(1);
    }

    @Test
    void concurrentBurstsRepeatedManyTimesAlwaysExecuteExactlyOnce() throws Exception {
        // Aggressive race-finder: repeated fully-coalesced waves of same-key concurrent calls
        // must never execute the action more than once per wave (covers the cache-check vs
        // slot-registration gap and the release/wakeup handoff).
        final int waves = 200;
        final int threads = 8;
        Idempotency idempotency = new Idempotency(baseConfig()
                .waitTimeout(Duration.ofSeconds(10))
                .build());
        AtomicInteger executions = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int wave = 0; wave < waves; wave++) {
                String key = "wave-" + wave;
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch releaseLeader = new CountDownLatch(1);
                List<Future<Integer>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    futures.add(pool.submit(() -> {
                        awaitLatch(start);
                        return idempotency.execute(key, () -> {
                            int value = executions.incrementAndGet();
                            awaitLatch(releaseLeader);
                            return value;
                        });
                    }));
                }
                start.countDown();
                waitForCoalesced(idempotency, (wave + 1) * (threads - 1));
                releaseLeader.countDown();
                Integer waveResult = futures.get(0).get(10, TimeUnit.SECONDS);
                for (Future<Integer> future : futures) {
                    assertThat(future.get(10, TimeUnit.SECONDS))
                            .as("every caller in wave %d reuses the same result", wave)
                            .isEqualTo(waveResult);
                }
            }
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(executions)
                .as("exactly one execution per wave across %d waves", waves)
                .hasValue(waves);
        assertThat(idempotency.stats().getMisses()).isEqualTo(waves);
        assertThat(idempotency.stats().getCoalescedWaits())
                .isEqualTo((long) waves * (threads - 1));
        assertThat(idempotency.stats().getTimeouts()).isZero();
    }

    @Test
    void actionCanExecuteAgainAfterDeduplicationWindowExpires() throws Exception {
        MutableClock clock = new MutableClock();
        IdempotencyConfig config = IdempotencyConfig.builder()
                .dedupWindow(Duration.ofMillis(100))
                .waitTimeout(Duration.ofSeconds(5))
                .timeSource(clock)
                .build();
        Idempotency idempotency = new Idempotency(
                config, new com.example.gsb.idempotency.store.InMemoryIdempotencyStore(clock));
        AtomicInteger executions = new AtomicInteger();

        IdempotentAction<String> action = () -> "v" + executions.incrementAndGet();

        assertThat(idempotency.execute("k", action)).isEqualTo("v1");
        clock.advance(Duration.ofMillis(50));
        assertThat(idempotency.execute("k", action)).isEqualTo("v1");
        assertThat(executions).hasValue(1);

        clock.advance(Duration.ofMillis(60)); // total 110ms > 100ms window
        assertThat(idempotency.execute("k", action)).isEqualTo("v2");
        assertThat(executions).hasValue(2);
        assertThat(idempotency.stats().getMisses()).isEqualTo(2);
        assertThat(idempotency.stats().getHits()).isEqualTo(1);
    }

    @Test
    void cachedExceptionIsReplayedUntilWindowExpires() {
        Idempotency idempotency = new Idempotency(baseConfig()
                .exceptionStrategy(ExceptionStrategy.CACHE_EXCEPTION)
                .build());
        AtomicInteger executions = new AtomicInteger();

        IdempotentAction<String> failing = () -> {
            executions.incrementAndGet();
            throw new IllegalStateException("boom");
        };

        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> idempotency.execute("fail", failing))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("boom");
        }
        assertThat(executions)
                .as("failure cached: action never re-runs inside the window")
                .hasValue(1);
        assertThat(idempotency.stats().getMisses()).isEqualTo(1);
        assertThat(idempotency.stats().getHits()).isEqualTo(2);
    }

    @Test
    void allowRetryRerunsActionForTheNextCallerAfterFailure() throws Exception {
        Idempotency idempotency = new Idempotency(baseConfig()
                .exceptionStrategy(ExceptionStrategy.ALLOW_RETRY)
                .build());
        AtomicInteger executions = new AtomicInteger();

        IdempotentAction<String> flaky = () -> {
            if (executions.incrementAndGet() == 1) {
                throw new IllegalStateException("transient");
            }
            return "recovered";
        };

        assertThatThrownBy(() -> idempotency.execute("flaky", flaky))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("transient");
        // Failure was not cached: the next same-key request is a new leader and may succeed.
        assertThat(idempotency.execute("flaky", flaky)).isEqualTo("recovered");

        assertThat(executions).hasValue(2);
        assertThat(idempotency.stats().getMisses()).isEqualTo(2);
        assertThat(idempotency.stats().getHits()).isZero();

        // The recovered result is cached normally for the rest of the window.
        assertThat(idempotency.execute("flaky", flaky)).isEqualTo("recovered");
        assertThat(executions).hasValue(2);
        assertThat(idempotency.stats().getHits()).isEqualTo(1);
    }

    @Test
    void coalescedWaiterTimesOutWithDefiniteResultButLeaderResultIsCachedLater()
            throws Exception {
        Idempotency idempotency = new Idempotency(baseConfig()
                .waitTimeout(Duration.ofMillis(200))
                .build());
        AtomicInteger executions = new AtomicInteger();

        CountDownLatch leaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLeader = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> leader = pool.submit(() -> idempotency.execute("slow", () -> {
                executions.incrementAndGet();
                leaderStarted.countDown();
                awaitLatch(releaseLeader);
                return "done";
            }));

            awaitLatch(leaderStarted);

            Future<?> waiter = pool.submit(() -> idempotency.execute("slow", () -> {
                executions.incrementAndGet();
                return "should-not-run";
            }));

            // Bounded wait: definite timeout exception, never a permanent block.
            assertThatThrownBy(() -> waiter.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IdempotencyWaitTimeoutException.class);

            releaseLeader.countDown();
            assertThat(leader.get(5, TimeUnit.SECONDS)).isEqualTo("done");
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(executions).hasValue(1);
        assertThat(idempotency.stats().getTimeouts()).isEqualTo(1);
        assertThat(idempotency.stats().getCoalescedWaits()).isEqualTo(1);

        // Once the leader finishes, later callers replay the cached value.
        assertThat(idempotency.execute("slow", () -> {
            executions.incrementAndGet();
            return "late";
        })).isEqualTo("done");
        assertThat(executions).hasValue(1);
    }

    @Test
    void interruptingLeaderGivesWaitersDefiniteExceptionAndNeverBlocks() throws Exception {
        Idempotency idempotency = new Idempotency(baseConfig()
                .exceptionStrategy(ExceptionStrategy.ALLOW_RETRY)
                .waitTimeout(Duration.ofSeconds(10))
                .build());
        AtomicInteger executions = new AtomicInteger();
        ConcurrentMap<String, Thread> leaderThread = new ConcurrentHashMap<>();

        CountDownLatch leaderInsideAction = new CountDownLatch(1);
        CountDownLatch waiterParked = new CountDownLatch(1);

        IdempotentAction<String> interruptible = () -> {
            executions.incrementAndGet();
            leaderThread.put("t", Thread.currentThread());
            leaderInsideAction.countDown();
            try {
                Thread.sleep(60_000);
                return "unreachable";
            } catch (InterruptedException e) {
                // Action reacts to interruption by aborting with the interrupt exception.
                throw e;
            }
        };

        Thread leader = new Thread(() -> {
            try {
                idempotency.execute("k", interruptible);
            } catch (Exception expected) {
                // InterruptedException expected.
            }
        }, "leader");

        Thread waiter = new Thread(() -> {
            try {
                waiterParked.countDown();
                idempotency.execute("k", interruptible);
            } catch (Exception expected) {
                // Re-delivered leader exception expected.
            }
        }, "waiter");

        leader.start();
        awaitLatch(leaderInsideAction);
        waiter.start();
        awaitLatch(waiterParked);

        // Give the waiter time to park on the in-flight future, then interrupt the leader.
        waitForCoalesced(idempotency, 1);
        leader.interrupt();

        leader.join(5000);
        waiter.join(5000);
        assertThat(leader.isAlive()).as("leader terminated after interruption").isFalse();
        assertThat(waiter.isAlive()).as("waiter unblocked with a definite result").isFalse();

        assertThat(executions)
                .as("waiter never executed the action")
                .hasValue(1);
        assertThat(idempotency.stats().getMisses()).isEqualTo(1);
        assertThat(idempotency.stats().getCoalescedWaits()).isEqualTo(1);
        assertThat(idempotency.stats().getTimeouts()).isZero();

        // ALLOW_RETRY: slot and failure are gone, a later same-key call runs the action again.
        CountDownLatch done = new CountDownLatch(1);
        Thread retry = new Thread(() -> {
            try {
                String value = idempotency.execute("k", () -> {
                    executions.incrementAndGet();
                    return "retried";
                });
                if ("retried".equals(value)) {
                    done.countDown();
                }
            } catch (Exception ignored) {
                // ignored
            }
        }, "retry");
        retry.start();
        assertThat(done.await(5, TimeUnit.SECONDS))
                .as("retry after interrupted leader executes successfully")
                .isTrue();
        retry.join(5000);
        assertThat(executions).hasValue(2);
        assertThat(leaderThread).isNotEmpty();
        assertThat(leaderThread.get("t").isInterrupted())
                .as("leader thread keeps its interrupted status")
                .isTrue();
    }
}
