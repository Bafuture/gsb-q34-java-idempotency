package com.example.gsb.idempotency;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class IdempotentExecutorTest {

    private ExecutorService pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdownNow();
        }
    }

    private ExecutorService pool() {
        return pool = Executors.newCachedThreadPool();
    }

    // ---------- 1. serial duplicates ----------

    @Test
    void serialDuplicatesExecuteBusinessLogicOnceAndReuseResult() {
        AtomicInteger executions = new AtomicInteger();
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofSeconds(10))
                .build();

        String first = executor.execute("create-order-1", () -> {
            executions.incrementAndGet();
            return "OK";
        });
        String second = executor.execute("create-order-1", () -> {
            executions.incrementAndGet();
            return "SHOULD-NOT-HAPPEN";
        });

        assertThat(first).isEqualTo("OK");
        assertThat(second).isEqualTo("OK");
        assertThat(executions).hasValue(1);
        assertThat(executor.stats().misses()).isEqualTo(1);
        assertThat(executor.stats().hits()).isEqualTo(1);

        // A different key is an independent execution.
        assertThat(executor.execute("create-order-2", () -> {
            executions.incrementAndGet();
            return "OK-2";
        })).isEqualTo("OK-2");
        assertThat(executions).hasValue(2);
    }

    // ---------- 2. concurrent convergence ----------

    @Test
    void concurrentRequestsConvergeToExactlyOneBusinessExecution() throws Exception {
        int threads = 16;
        AtomicInteger executions = new AtomicInteger();
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofSeconds(10))
                .waitTimeout(Duration.ofSeconds(10))
                .build();

        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = pool();
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                assertThat(go.await(5, SECONDS)).isTrue();
                return executor.execute("create-order-X", () -> {
                    executions.incrementAndGet();
                    Thread.sleep(200);
                    return "single-result";
                });
            }));
        }
        assertThat(ready.await(5, SECONDS)).isTrue();
        go.countDown();

        for (Future<String> future : futures) {
            assertThat(future.get(10, SECONDS)).isEqualTo("single-result");
        }

        assertThat(executions).hasValue(1);
        assertThat(executor.stats().misses()).isEqualTo(1);
        assertThat(executor.stats().joins() + executor.stats().hits()).isEqualTo(threads - 1);
        assertThat(executor.stats().joins()).isGreaterThanOrEqualTo(1);
        assertThat(executor.stats().timeouts()).isZero();
    }

    // ---------- 3. window expiry ----------

    @Test
    void sameKeyExecutesAgainAfterWindowExpires() {
        AtomicInteger executions = new AtomicInteger();
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofMillis(150))
                .build();

        assertThat(executor.execute("k", () -> {
            executions.incrementAndGet();
            return "v1";
        })).isEqualTo("v1");
        assertThat(executor.execute("k", () -> {
            executions.incrementAndGet();
            return "v2";
        })).isEqualTo("v1");

        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(executor.execute("k", () -> {
            executions.incrementAndGet();
            return "v2";
        })).isEqualTo("v2");
        assertThat(executions).hasValue(2);
        assertThat(executor.stats().misses()).isEqualTo(2);
        assertThat(executor.stats().hits()).isEqualTo(1);
    }

    // ---------- 4. exception strategies ----------

    @Test
    void cacheExceptionStrategyReplaysFailureWithinWindow() {
        AtomicInteger executions = new AtomicInteger();
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofSeconds(10))
                .exceptionPolicy(ExceptionPolicy.CACHE_EXCEPTION)
                .build();

        Runnable call = () -> executor.execute("payment-1", () -> {
            executions.incrementAndGet();
            throw new IllegalStateException("insufficient funds");
        });

        assertThatThrownBy(call::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("insufficient funds");
        assertThatThrownBy(call::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("insufficient funds");

        assertThat(executions).hasValue(1);
        assertThat(executor.stats().misses()).isEqualTo(1);
        assertThat(executor.stats().hits()).isEqualTo(1);
    }

    @Test
    void allowRetryStrategyRunsBusinessLogicAgain() {
        AtomicInteger executions = new AtomicInteger();
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofSeconds(10))
                .exceptionPolicy(ExceptionPolicy.ALLOW_RETRY)
                .build();

        assertThatThrownBy(() -> executor.execute("payment-2", () -> {
            executions.incrementAndGet();
            throw new IllegalStateException("temporary glitch");
        })).isInstanceOf(IllegalStateException.class);

        String result = executor.execute("payment-2", () -> {
            executions.incrementAndGet();
            return "paid";
        });

        assertThat(result).isEqualTo("paid");
        assertThat(executions).hasValue(2);
        assertThat(executor.stats().misses()).isEqualTo(2);
    }

    @Test
    void checkedBusinessExceptionIsWrappedButStillConverges() {
        AtomicInteger executions = new AtomicInteger();
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofSeconds(10))
                .build();

        Runnable call = () -> executor.execute("k", () -> {
            executions.incrementAndGet();
            throw new java.io.IOException("disk full");
        });

        assertThatThrownBy(call::run)
                .isInstanceOf(IdempotentExecutionException.class)
                .hasCauseInstanceOf(java.io.IOException.class);
        assertThatThrownBy(call::run)
                .isInstanceOf(IdempotentExecutionException.class)
                .hasCauseInstanceOf(java.io.IOException.class);
        assertThat(executions).hasValue(1);
    }

    // ---------- 5. wait timeout ----------

    @Test
    void followerGetsTimeoutExceptionInsteadOfBlockingForever() throws Exception {
        CountDownLatch leaderStarted = new CountDownLatch(1);
        CountDownLatch block = new CountDownLatch(1);
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofSeconds(30))
                .waitTimeout(Duration.ofMillis(150))
                .build();
        ExecutorService pool = pool();

        Future<String> leader = pool.submit(() -> executor.execute("slow-key", () -> {
            leaderStarted.countDown();
            assertThat(block.await(10, SECONDS)).isTrue();
            return "late";
        }));
        assertThat(leaderStarted.await(5, SECONDS)).isTrue();

        long start = System.nanoTime();
        assertThatThrownBy(() -> executor.execute("slow-key", () -> "never"))
                .isInstanceOf(WaitTimeoutException.class);
        long waitedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(waitedMs).isBetween(100L, 2_000L);
        assertThat(executor.stats().timeouts()).isEqualTo(1);
        assertThat(executor.stats().joins()).isEqualTo(1);

        block.countDown();
        assertThat(leader.get(5, SECONDS)).isEqualTo("late");
    }

    // ---------- interruption handling ----------

    @Test
    void interruptedLeaderUnblocksFollowersWithDefiniteOutcome() throws Exception {
        CountDownLatch leaderStarted = new CountDownLatch(1);
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofSeconds(30))
                .waitTimeout(Duration.ofSeconds(5))
                .exceptionPolicy(ExceptionPolicy.CACHE_EXCEPTION)
                .build();
        ExecutorService pool = pool();

        Future<String> leader = pool.submit(() -> executor.execute("int-key", () -> {
            leaderStarted.countDown();
            Thread.sleep(30_000);
            return "never";
        }));
        assertThat(leaderStarted.await(5, SECONDS)).isTrue();

        Future<String> follower = pool.submit(() -> executor.execute("int-key", () -> "retry-blocked"));
        Thread.sleep(100);
        leader.cancel(true);

        assertThatThrownBy(() -> follower.get(5, SECONDS))
                .isInstanceOf(java.util.concurrent.ExecutionException.class)
                .hasCauseInstanceOf(IdempotentExecutionException.class)
                .rootCause()
                .isInstanceOf(InterruptedException.class);

        // A later serial caller gets the cached failure instead of hanging.
        assertThatThrownBy(() -> executor.execute("int-key", () -> "x"))
                .isInstanceOf(IdempotentExecutionException.class)
                .hasCauseInstanceOf(InterruptedException.class);
    }

    @Test
    void interruptedFollowerGetsInterruptedExceptionAndDoesNotExecuteBusiness() throws Exception {
        CountDownLatch leaderStarted = new CountDownLatch(1);
        CountDownLatch block = new CountDownLatch(1);
        AtomicInteger followerExecutions = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<Throwable> caught =
                new java.util.concurrent.atomic.AtomicReference<>();
        IdempotentExecutor executor = IdempotentExecutor.builder()
                .window(Duration.ofSeconds(30))
                .waitTimeout(Duration.ofSeconds(10))
                .build();
        ExecutorService pool = pool();

        pool.submit(() -> executor.execute("int-key-2", () -> {
            leaderStarted.countDown();
            block.await(10, SECONDS);
            return "x";
        }));
        assertThat(leaderStarted.await(5, SECONDS)).isTrue();

        Future<?> follower = pool.submit(() -> {
            try {
                executor.execute("int-key-2", () -> {
                    followerExecutions.incrementAndGet();
                    return "y";
                });
            } catch (Throwable t) {
                caught.set(t);
            }
        });
        Thread.sleep(100);
        follower.cancel(true);

        long deadline = System.currentTimeMillis() + 3000;
        while (caught.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(caught.get())
                .isInstanceOf(IdempotentWaitInterruptedException.class)
                .hasCauseInstanceOf(InterruptedException.class);
        block.countDown();
        assertThat(followerExecutions).hasValue(0);
    }
}
