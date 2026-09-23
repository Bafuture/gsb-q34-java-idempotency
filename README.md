# 从零实现幂等控制组件

Pair-wise GSB 标注任务仓库（第 4 批 / 34）。

| 项目 | 内容 |
|------|------|
| 任务类型 | Feature 迭代 |
| 任务难度 | 困难 |
| 语言/框架 | Java, Maven, JUnit 5 |
| 环境可复现等级 | 无外部依赖 |
| 构建方式 | Maven（含 mvnw wrapper，无需本机安装 Maven） |

## 运行方式

```bash
./mvnw -q verify
```

---

# 幂等控制组件（idempotency）

对外接口被上游重复调用时，保证同一个幂等键在去重窗口内**业务逻辑只执行一次**，并发重复请求**合流**到首次执行并复用其结果。无任何第三方运行时依赖，仅 JUnit 5 + AssertJ。

## 快速开始

```java
IdempotentExecutor executor = IdempotentExecutor.builder()
        .store(new InMemoryIdempotencyStore())   // 默认即内存实现
        .window(Duration.ofMinutes(5))           // 去重/缓存窗口（从首次获取槽位算起）
        .waitTimeout(Duration.ofSeconds(10))     // 跟随方最长等待时间（默认=窗口，并被窗口截断）
        .exceptionPolicy(ExceptionPolicy.CACHE_EXCEPTION) // 或 ALLOW_RETRY
        .build();

String result = executor.execute("order:20260923:1001", () -> {
    // 只在窗口内第一次调用时真正执行；重复调用复用这里的返回值
    return orderService.createOrder();
});
```

- 幂等键由调用方显式传入（如“业务名:业务唯一 ID”）。
- 业务接口为 `IdempotentOperation<T>`，允许抛出 checked exception（会被包装为 `IdempotentExecutionException` 重放）。
- 返回值在窗口内原样复用（包括 `null`）。

## 配置与异常

| 配置 | 说明 | 默认值 |
|------|------|--------|
| `window` | 去重窗口 / 结果缓存有效期，从首次 `tryAcquire` 起算，必须为正 | 5 分钟 |
| `waitTimeout` | 跟随方等待首次执行的最长时间；超过窗口时截断为窗口 | = window |
| `exceptionPolicy` | `CACHE_EXCEPTION`：窗口内重放异常；`ALLOW_RETRY`：释放槽位允许后续请求重试 | `CACHE_EXCEPTION` |
| `store` | 存储 SPI 实现 | `InMemoryIdempotencyStore` |

| 异常 | 触发场景 |
|------|----------|
| `WaitTimeoutException` | 跟随方等待超过 `waitTimeout`，得到明确失败而非永久阻塞 |
| `IdempotentWaitInterruptedException` | 跟随线程在等待期间被中断（中断标志会被恢复） |
| 业务异常原样重抛 | RuntimeException / Error 直接抛出；checked exception 包装为 `IdempotentExecutionException` |

## 并发合流的实现方式

核心是“每个幂等键一个槽位（slot）”的状态机，位于 `IdempotencyStore` SPI 之后：

```
                 tryAcquire（原子）
(不存在/已过期) ───────────────▶ IN_PROGRESS（Leader 执行业务）
                                      │
              ┌───────────────────────┼───────────────────────┐
       complete(value)          fail(cache=true)        fail(cache=false)
              ▼                        ▼                        ▼
          SUCCESS                  FAILURE                  RELEASED（删除槽位）
     （窗口内重放结果）         （窗口内重放异常）          （跟随方重新 tryAcquire 重试）
```

`IdempotentExecutor.execute(key, op)` 的流程：

1. **原子占位**：`store.tryAcquire(key, window)` 是一次原子操作，返回三种结局之一：
   - `Leader`：自己执行业务，统计 miss；
   - `Follower`：已有同键执行在进行中，统计 join，进入等待；
   - `Cached`：槽位已是终态（成功/失败）且未过期，统计 hit，直接重放结果。
2. **Leader 执行**：在 `try/catch/finally` 语义下保证业务无论正常返回还是抛异常（含线程中断），都调用 `complete` / `fail` 发布终态并唤醒所有跟随者——Leader 线程意外异常不会留下“僵尸槽位”。
3. **Follower 等待**：基于**绝对截止时间**（进入 execute 时计算 `deadline = now + waitTimeout`）调用 `store.await`。内存实现用槽位对象的内置监视器 `wait(millis, nanos)` + `notifyAll` 阻塞；`await` 返回三态结果：`Result`（重放）、`Released`（ALLOW_RETRY 下槽位被释放，回到 tryAcquire 重试）、`Timeout`（抛 `WaitTimeoutException`，统计 timeout）。
4. **异常策略**：`CACHE_EXCEPTION` 时失败进入终态 FAILURE，跟随方和窗口内后续请求拿到同一个异常，业务只执行一次；`ALLOW_RETRY` 时槽位直接释放，等待中的跟随方被唤醒并重新竞争 Leader，从而完成一次重试。

内存实现（`InMemoryIdempotencyStore`）用 `ConcurrentHashMap<String, Entry>` + 监视器：

- `tryAcquire` 在 store 监视器上串行化“查过期→判定状态→插入”的复合动作，保证同键并发只有一个 Leader；
- `Entry` 自带状态与过期时间，跟随方在 Entry 监视器上等待，终态发布时 `notifyAll`；
- 过期槽位在下次 `tryAcquire` 时惰性清除。

## 执行中断与超时的保证

- **Leader 被中断**：业务通常以 `InterruptedException` 结束，组件捕获后照常发布终态（按异常策略缓存失败或释放槽位），并恢复中断标志；跟随方立即被唤醒拿到明确结果，不会永久阻塞。
- **Follower 被中断**：`await` 响应中断，组件恢复中断标志并抛 `IdempotentWaitInterruptedException`，跟随方从不执行业务。
- **Leader 执行过慢**：跟随方受绝对截止时间保护，超时即抛 `WaitTimeoutException`（注意：超时只让跟随方放弃等待，Leader 不被取消，其结果仍会正常写入缓存供后续请求使用）。

## 统计

`executor.stats()` 提供线程安全计数器（`LongAdder`）：

| 指标 | 含义 |
|------|------|
| `hits()` | 命中：直接返回窗口内已缓存的终态结果（成功或失败） |
| `misses()` | 未命中：成为 Leader，真正执行业务的次数 |
| `joins()` | 合流等待：发现同键执行在进行中而进入等待的次数 |
| `timeouts()` | 超时：跟随方等待超过 `waitTimeout` 的次数 |

另有 `snapshot()` 返回一致快照。

## 存储 SPI 与替换为外部存储

SPI 只有 4 个方法（见 `IdempotencyStore`）：

- `tryAcquire(key, window)` —— 必须是**原子的** compare-and-set，返回 Leader / Follower / Cached；
- `complete(key, value)` —— 发布成功终态并唤醒等待者；
- `fail(key, error, cacheFailure)` —— 发布失败终态或释放槽位；
- `await(key, timeout)` —— 有界等待终态，返回 Result / Released / Timeout。

替换方式：实现该接口，通过 `builder().store(...)` 注入即可，执行器与统计层不依赖存储技术。以 Redis 为例的实现思路：

- **占位**：`SET idem:{key} <leader-token> NX PX <windowMillis>` —— 设置成功为 Leader；键已存在时读取内容，若值为终态 JSON 则 Cached，否则 Follower。完成时用 Lua 脚本凭 leader-token 校验后改写终态并续期/保留 TTL。
- **等待**：跨进程没有条件变量，可用短间隔轮询 `GET`（配合指数退避）或订阅 `idem:{key}` 频道；Leader 终态写入后 PUBLISH 唤醒，二者都必须保留 `waitTimeout` 截止时间作为最终兜底。
- **释放/重试**：`ALLOW_RETRY` 对应删除键后 PUBLISH，下一次 `SET NX` 自然产生新 Leader。
- 外部存储里结果需可序列化；异常重放需要记录异常类型/消息并在客户端重建（或统一包装为业务异常）。

## 边界与限制

- 内存实现只在**单 JVM 内**生效；多实例部署必须换成外部存储实现，且去重语义的严格程度取决于外部存储 `SET NX` 这类原子原语。
- **窗口短于业务执行时间**时，槽位可能在执行结束前过期，此后的请求会成为新 Leader，即“窗口外不保证只执行一次”。因此窗口应大于业务最坏耗时；`waitTimeout` 默认被截断为窗口。
- 跟随方超时**不取消** Leader（组件无法安全地替业务做取消决策）；如需取消，应由业务自身响应中断。
- 缓存内容是 Java 对象引用（内存实现），调用方不应在拿到结果后原地修改可变对象。
- 统计是尽力而为的运行时观测值，重启不持久化。

## 测试覆盖

`IdempotentExecutorTest`（`mvn -q verify`）：

1. **串行重复**：同键两次调用只执行一次，第二次复用结果，hit/miss 统计正确；
2. **并发合流**：16 线程经起跑闸同时打同键，断言业务只执行 1 次、全部拿到同一结果、join/hit 合计 15；
3. **窗口过期**：窗口 150ms，过期后同键再次真正执行；
4. **异常策略**：`CACHE_EXCEPTION` 下异常被重放且业务只执行一次；`ALLOW_RETRY` 下失败后可再次成功；另含 checked exception 包装用例；
5. **等待超时**：Leader 被阻塞时跟随方在 ~150ms 得到 `WaitTimeoutException`，timeout 计数准确；
6. 另含两个中断用例：Leader 被中断后跟随方立刻拿到明确失败；跟随方被中断时得到 `IdempotentWaitInterruptedException` 且业务零执行。

## 代码结构

```
src/main/java/com/example/gsb/idempotency/
├── IdempotentExecutor.java           执行器入口 + Builder
├── IdempotentOperation.java          业务函数式接口（允许 checked exception）
├── IdempotencyStore.java             存储 SPI（tryAcquire/complete/fail/await + 结局密封类型）
├── InMemoryIdempotencyStore.java     单机内存实现（ConcurrentHashMap + 监视器）
├── StoredResult.java                 成功/失败终态
├── ExceptionPolicy.java              CACHE_EXCEPTION / ALLOW_RETRY
├── IdempotencyStats.java             hit/miss/join/timeout 计数器
├── WaitTimeoutException.java         跟随方等待超时
├── IdempotentExecutionException.java checked 业务异常包装
└── IdempotentWaitInterruptedException.java 跟随方等待被中断
```
