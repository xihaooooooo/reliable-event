# M2.2：双 Worker 并发竞争测试

> 状态：已完成
>
> 目标：使用真实 MySQL 8.0，让两个独立 Worker 同时竞争同一份候选快照，证明只有一个 Worker 能获得执行权并调用发送器。

## 为什么单独做这一阶段

M2.1 已经实现基于版本号的条件抢占，并分别验证了：

- 正确版本能够抢占事件；
- 同一个旧候选不能重复抢占；
- 过期抢占版本不能完成状态更新。

这些测试证明了条件更新规则，但仍然是按顺序执行。M2.2 不增加新的状态或失败策略，只补上真实并发证据：两个线程、两个 Worker、两个独立事务同时对 MySQL 中的同一行执行抢占，最终只能有一个成功者。

## 要证明的行为

```text
同一个 PENDING 事件
        ↓
预先取得同一份 EventCandidate(id, version)
        ↓
Worker A ──同时开始── Worker B
        ↓                 ↓
按相同版本条件抢占   按相同版本条件抢占
        ↓                 ↓
一个更新 1 行        一个更新 0 行
        ↓                 ↓
调用发送器并完成      跳过，不发送
        ↓
PUBLISHED
```

测试不关心 A、B 中谁获胜，只关心成功者数量严格等于一。

## 并发测试必须满足的条件

### 1. 使用真实 MySQL 8.0

继续使用现有 Testcontainers 环境，不使用 H2、Mock Repository 或只验证 Java 锁的单元测试。要验证的是 MySQL 条件更新在两个独立事务竞争同一行时的实际行为。

### 2. 两个独立 Worker

测试中创建两个 `JdbcEventPublicationWorker` 实例。它们可以共享线程安全的 `DataSource` 和 `PlatformTransactionManager`，但每个 Worker 必须拥有自己的 Repository 和 `TransactionTemplate` 实例。

两个 Worker 必须运行在不同线程上。Spring 事务资源绑定在线程上，因此两个线程会分别取得数据库连接和事务上下文。

### 3. 使用同一份候选快照

不能简单地同时调用两次 `publishDueEvents()`：一个线程可能在另一个线程查询前已经把事件改成 `PUBLISHING`，导致第二个线程根本没有看到候选。这只能证明扫描过滤有效，不能证明两个相同版本的抢占请求发生竞争。

测试应当先查询一次候选快照，再把同一个不可变 `EventCandidate` 同时交给两个 Worker。这样两个抢占请求都携带相同的 `id` 和 `version`。

### 4. 使用明确的并发起点

使用 `CountDownLatch` 或等价并发原语：

1. 两个任务分别在线程池中启动；
2. 两个任务报告已经就绪；
3. 测试线程确认双方就绪后开放统一起点；
4. 两个任务同时处理相同候选。

不要使用 `Thread.sleep()` 猜测线程时序。所有等待必须设置超时，测试失败时不能无限挂起。

## 最小生产代码调整

当前 `publishDueEvents()` 同时负责查询候选和处理候选。为了让测试在不增加测试专用 Hook 的情况下把同一份候选交给两个 Worker，可以提取一个包内可见方法，例如：

```java
int publishCandidates(List<EventCandidate> candidates)
```

调用关系为：

```text
publishDueEvents()
        ↓
findDueEventCandidates(...)
        ↓
publishCandidates(candidates)
```

`publishCandidates` 继续执行 M2.1 已有流程：

```text
短事务条件抢占 → 事务外发送 → 短事务标记 PUBLISHED
```

约束如下：

- 方法保持包内可见，不加入公开 API；
- 不增加仅供测试调用的回调、休眠或全局开关；
- 不把 Repository 或事务对象暴露给业务调用方；
- `publishDueEvents()` 的对外行为和返回值语义保持不变；
- 如果实现时采用语义等价的内部拆分，可以调整方法名称，但测试必须让两个 Worker 处理同一份预先取得的候选快照。

## Fake Sender

并发测试使用线程安全的 Fake Sender 记录调用：

- 可以让两个 Worker 分别持有 Sender，再汇总调用次数；
- 也可以共享一个基于 `AtomicInteger` 或并发集合实现的 Sender；
- 不能使用非线程安全的 `ArrayList` 作为并发断言依据；
- Sender 不负责阻塞或决定抢占结果，数据库条件更新仍然是唯一执行权来源。

最终必须能够断言发送总次数严格为 `1`，并确认发送的是目标事件。

## 建议测试步骤

1. 在事务内登记一条已经到期的事件；
2. 查询并保存唯一的 `EventCandidate`，确认初始版本为 `0`；
3. 创建两个 Worker 和两个并发任务；
4. 两个任务都到达准备点后，由测试线程同时放行；
5. 两个 Worker 分别处理同一个候选列表；
6. 使用带超时的 `Future.get(...)` 等待两个任务完成；
7. 在 `finally` 中关闭测试线程池；
8. 查询数据库并完成最终断言。

测试不得依赖哪一个 Worker 先执行，也不得把死锁或超时当成预期结果。

## 必须断言的结果

并发执行完成后，同时验证：

- 两个 Worker 都正常结束，没有未捕获异常；
- 两个 Worker 返回的发布数量之和为 `1`；
- 恰好一个 Worker 返回 `1`，另一个返回 `0`；
- Fake Sender 总调用次数为 `1`；
- Sender 收到的事件 ID 和事件键正确；
- Outbox 中仍然只有一条事件；
- 最终状态为 `PUBLISHED`；
- `attempt_count = 1`；
- 最终 `version = 2`，即抢占和完成状态各递增一次；
- `published_at` 不为空。

其中 Worker 胜负顺序是不稳定的，断言必须与具体线程身份无关。

## 测试稳定性要求

- 使用固定 `Clock`，避免时间边界造成候选忽隐忽现；
- 候选事件的 `availableAt` 应早于固定当前时间；
- 线程同步使用并发原语，不使用任意时长休眠；
- 所有 latch 和 future 等待都设置明确超时；
- 线程池必须在 `finally` 中关闭；
- 测试失败信息应能区分准备超时、执行超时和断言失败；
- 单次测试必须能稳定证明竞争条件，不通过高次数循环掩盖不可控时序。

## 完成标准

- 存在一个基于 MySQL 8.0 Testcontainers 的双 Worker 并发集成测试；
- 两个 Worker 使用同一份候选版本并从统一起点开始竞争；
- 数据库条件更新是决定唯一执行权的机制；
- 测试证明只有一个 Worker 调用发送器；
- 抢占次数、最终版本和状态均符合 M2.1 语义；
- 没有新增测试专用的公开 API 或生产环境开关；
- M0、M1、M2.1 的已有测试继续通过；
- `mvn clean verify` 在 Java 17 和 MySQL 8.0 下通过。

## 本阶段明确不做

- 在单个 Worker 内增加并行线程池；
- 后台定时调度；
- `RETRY_WAIT` 和指数退避；
- 最大尝试次数和 `DEAD`；
- `lease_owner`、`lease_until` 和宕机恢复；
- RocketMQ 适配；
- 性能或吞吐量基准测试。

M2.2 只证明同一时刻的数据库抢占排他性，不声称已经解决抢占后进程退出的问题。

## 阶段性边界

如果获胜 Worker 在抢占事务提交后、状态更新前退出，事件仍会停留在 `PUBLISHING`。这不属于并发抢占错误，而是 M3 要通过租约和过期恢复解决的故障窗口。

如果发送器正常抛出异常，M2.2 仍没有失败状态转换；M2.3 再实现 `RETRY_WAIT` 和退避时间。

## 后续顺序

```text
M2.2 双 Worker 并发竞争测试
        ↓
M2.3 指数退避与 RETRY_WAIT
        ↓
M2.4 最大尝试次数与 DEAD
```
