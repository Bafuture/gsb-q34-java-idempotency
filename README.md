# 幂等控制组件（Idempotency Component）

一个零外部依赖（仅 JUnit 5 / AssertJ 测试依赖）的 Java 17 幂等控制组件：调用方显式传入
幂等键，组件保证**去重窗口内同一键的业务逻辑最多执行一次**，并对并发请求做合流、对
结果（含异常）做缓存。

## 运行方式

```bash
./mvnw -q verify
```

需要本机有 JDK 17（`mvnw` 会自动下载 Maven 3.9.9）。

## 快速上手

```java
Idempotency idempotency = new Idempotency(
        IdempotencyConfig.builder()
                .dedupWindow(Duration.ofMinutes(10))   // 必填：结果缓存窗口
                .waitTimeout(Duration.ofSeconds(5))    // 可选：合流等待上限，默认 10s
                .exceptionStrategy(ExceptionStrategy.CACHE_EXCEPTION) // 可选，默认缓存异常
                .build());

// key 由调用方显式传入（请求头 / 订单号 / 上游唯一 ID …）
String result = idempotency.execute("order-123", () -> doBusiness());
```

- 窗口内同 key 的串行重复调用：直接回放第一次的结果，`doBusiness()` 不再执行。
- 同 key 的并发调用：只有一个请求（leader）执行业务，其余（follower）等待并复用同一结果。
- 窗口过期后：同 key 再次调用会重新执行业务。
- 异常策略：
  - `CACHE_EXCEPTION`（默认）：异常也缓存整个窗口，之后所有同 key 调用回放同一异常；
  - `ALLOW_RETRY`：失败不缓存，已在等待的 follower 收到 leader 的原始异常，之后的下一个
    同 key 调用成为新 leader，可以重试。

## 并发合流的实现方式

核心在 `Idempotency`，由两张表协作：

1. **结果存储 `IdempotencyStore`（已完成结果，含 TTL）**：保存成功值或失败异常及过期时刻。
2. **在途登记表 `ConcurrentHashMap<String, InFlight>`（仅本 JVM）**：每个 key 至多一个
   `InFlight`，内部持有一个 `CompletableFuture` 和登记起始时间。

请求处理流程（`Idempotency.execute`）：

1. 先查 `store.find(key, now)`：命中且未过期 → 记一次 `hits`，回放结果/异常。
2. 未命中则用 `putIfAbsent(key, new InFlight(...))` 竞争 leader：
   - 成功者记一次 `misses`，**在自己的线程内同步执行业务**；
   - 失败者记一次 `coalescedWaits`，成为 follower，调用
     `future.get(waitTimeout, MILLISECONDS)` 有界等待；leader 完成后 follower 从 store 回放
     结果，因此得到的与 leader 完全一致（成功值或原始异常对象）。
3. leader 结束时严格按 **先写 store、后 `inFlight.remove`、最后 complete future** 的顺序
   发布，保证 follower 被唤醒时结果一定可见，不会读到“没有缓存但执行已结束”的空洞。
4. `null` 返回值用内部哨兵对象包装，保证可以正常缓存与回放。

测试 `concurrentSameKeyRequestsCoalesceIntoSingleExecution` 用 16 个线程同时打同一 key，
通过两个 `CountDownLatch` 确定性地让 15 个 follower 全部停在等待点后再放行 leader，断言
业务计数器恰为 1、所有人拿到同一结果；统计为 `misses=1 / coalescedWaits=15`。

## 中断与超时：等待方永远拿到明确结果

| 情形 | 行为 |
|------|------|
| follower 等待超过 `waitTimeout` | 抛 `IdempotencyWaitTimeoutException`，记一次 `timeouts`；leader 不受影响继续执行，其结果在完成后照常缓存，后续调用仍可命中（见 `coalescedWaiterTimesOut...` 测试） |
| follower 等待期间自身被中断 | 恢复中断标志并抛 `InterruptedException`，不吞中断、不重置等待 |
| leader 执行中被中断（action 抛 `InterruptedException`） | 异常经 future 原样传递给所有 follower；leader 线程恢复中断标志。`ALLOW_RETRY` 下在途槽与失败记录都会清理，下一个同 key 请求可立即重试（见 `interruptingLeader...` 测试） |

组件只控制**等待边界**，不会强行停止业务线程——leader 的 action 在自己的调用线程上同步
执行，超时只解放等待方，不中断一个可能正在写库的业务操作。

## 存储 SPI 与替换为外部存储

SPI 为 `com.example.gsb.idempotency.store.IdempotencyStore`，只管**已结束结果**：

```java
public interface IdempotencyStore {
    IdempotencyRecord find(String key, long now);  // 过期视为不存在
    void save(IdempotencyRecord record);           // value/error + expireAtNanos
    void remove(String key);                       // ALLOW_RETRY / 手动失效
}
```

- 自带 `InMemoryIdempotencyStore`：`ConcurrentHashMap` + 查询时惰性过期，适合单机与测试；
  构造 `Idempotency` 时不传 store 即默认使用它。
- 替换外部存储：实现该接口并通过 `new Idempotency(config, store)` 注入即可，组件其余部分
  无需改动。以 Redis 为例：
  - `save` → `SET idem:{key} <序列化的 value/error + 状态> PX <窗口毫秒>`，TTL 交给服务端；
  - `find` → `GET` 后反序列化（结果含成功/失败标记，异常需可序列化，建议存错误码/消息）；
  - `remove` → `DEL`。
  - 记录值需要可序列化；接口刻意只暴露原始对象，序列化方式由实现决定。

### 分布式边界（重要）

- 在途合流（`InFlight`/`CompletableFuture`）是 **JVM 本地**的。多实例部署时，结果缓存
  借助外部存储天然共享（A 实例执行后，B 实例在窗口内命中缓存），但两个实例上的**同时**
  首次请求都可能成为本地 leader。要做到跨实例“恰好一次执行”，需要外部存储提供原子占位，
  例如 Redis `SET NX PX` 抢执行权 + `SUBSCRIBE`/轮询结果，或数据库唯一约束事务；这套
  协调语义因基础设施而异，没有硬编码进本 SPI。
- 进程崩溃会留下本地在途槽。`maxInFlightDuration` 是一个安全租约：登记时间超过它的槽被
  视为“leader 已死”，新请求可原子抢占（`ConcurrentHashMap.replace` CAS）。单进程内
  leader 总会走到清理逻辑，故默认关闭（`Long.MAX_VALUE`）；开启它意味着“宁可可能重复
  执行，也不让槽永久卡死”，这是 at-most-once 与可用性之间的取舍。
- 时钟统一通过 `TimeSource.nanoTime()`（单调时钟，不受系统时间回拨影响）；测试可注入
  可控时钟，无需真实 sleep 即可验证窗口过期。

## 统计

`idempotency.stats()` 返回 `IdempotencyStats`（`LongAdder`，线程安全）：

| 指标 | 含义 |
|------|------|
| `hits` | 命中窗口内已缓存结果（成功值或被缓存的异常）并回放 |
| `misses` | 无可用结果，本请求成为 leader 真正执行业务 |
| `coalescedWaits` | 发现同 key 在途执行而进入等待（无论最后等到还是超时） |
| `timeouts` | 合流等待超过 `waitTimeout` 而拿到超时异常的次数 |

另有 `snapshot()` 一次性获取四个计数；`snapshot.total() = hits + misses + coalescedWaits`。

## 测试覆盖（`mvn -q verify`）

`IdempotencyTest` 共 9 个用例，覆盖要求的五类场景、中断与边界：

1. 串行重复：`serialDuplicatesExecuteActionOnceAndReplayCachedResult`
2. 并发合流：`concurrentSameKeyRequestsCoalesceIntoSingleExecution`（16 线程，业务仅 1 次）
3. 窗口过期后可再次执行：`actionCanExecuteAgainAfterDeduplicationWindowExpires`
4. 异常策略：`cachedExceptionIsReplayedUntilWindowExpires`、
   `allowRetryRerunsActionForTheNextCallerAfterFailure`
5. 等待超时：`coalescedWaiterTimesOutWithDefiniteResultButLeaderResultIsCachedLater`
6. 执行中断：`interruptingLeaderGivesWaitersDefiniteExceptionAndNeverBlocks`

另有 `null` 返回值缓存（`nullReturnValueIsCachedAndReplayed`）、200 波 × 8 线程的
竞态压测（`concurrentBurstsRepeatedManyTimesAlwaysExecuteExactlyOnce`，断言每波业务恰执行
1 次），以及 `InMemoryIdempotencyStoreTest` 的 TTL/删除单测。

## 代码结构

```
src/main/java/com/example/gsb/idempotency/
├── Idempotency.java                 # 门面：execute / 合流 / 中断超时处理
├── IdempotencyConfig.java           # 去重窗口、等待超时、异常策略、安全租约、时钟
├── ExceptionStrategy.java           # CACHE_EXCEPTION / ALLOW_RETRY
├── IdempotentAction.java            # 业务逻辑函数式接口
├── IdempotencyWaitTimeoutException.java
├── TimeSource.java                  # 可注入的单调时钟
├── IdempotencyStats.java            # 命中/未命中/合流等待/超时
└── store/
    ├── IdempotencyStore.java        # 存储 SPI
    ├── IdempotencyRecord.java       # 成功/失败结果 + 过期时刻
    └── InMemoryIdempotencyStore.java# 内存实现（ConcurrentHashMap，惰性过期）
```

## 边界小结

- 保证范围：**单 JVM 内**“窗口内同 key 业务最多执行一次 + 并发合流 + 结果一致回放”。
- 不在保证范围：跨 JVM 的同时首访去重（需外部存储原子占位）、action 本身的可取消性、
  外部存储的故障切换语义。
- follower 超时/中断只影响等待方自身；leader 的结果发布遵循“先持久化、后唤醒”，不会出现
  等待方被唤醒却拿不到结果的情况，任何路径都不会永久阻塞。

---

> Pair-wise GSB 标注任务仓库（第 4 批 / 34）。`main` 为初始环境快照，`A`、`B` 为两次
> 独立执行的工作分支。
