# M3.3：并发接管与恢复编排

> 状态：已完成
>
> 目标：将过期租约恢复与普通发布冻结为一次有界、可观测的执行顺序，并通过真实 MySQL 并发测试证明多个恢复者竞争同一过期租约时只有一个成功，旧 Worker 在恢复和重新抢占后不能覆盖新状态。

## 为什么在 M3.2 之后做这一阶段

M3.2 已经提供两个彼此独立的内部入口：

```text
JdbcExpiredLeaseRecovery.recoverExpiredLeases()
JdbcEventPublicationWorker.publishDueEvents()
```

前者能够限量查询过期的 `PUBLISHING` 记录，并使用事件 ID、版本、Owner 和截止时间快照将其条件恢复为 `RETRY_WAIT` 或 `DEAD`；后者能够查询已经到期的 `PENDING` 和 `RETRY_WAIT` 事件，条件抢占后在事务外调用 Sender。

但当前还存在两个没有闭合的边界：

1. 恢复入口需要显式调用，没有一个单轮执行入口定义恢复和发布的先后关系；
2. 现有测试只按顺序证明旧恢复候选不能重复更新，没有让两个恢复者真正并发竞争同一份候选快照，也没有覆盖旧发送 Worker 与恢复、新抢占交错执行的竞态。

如果直接进入后台调度或独立进程故障注入，失败时将很难区分问题来自调度、进程生命周期还是数据库并发协议。因此 M3.3 只完成编排和进程内可确定复现的并发接管测试，继续保持同步调用、Fake Sender 和短事务边界。

## 本阶段冻结的结论

M3.3 冻结以下决定：

- 单轮执行顺序固定为“先恢复过期租约，再扫描并发布到期事件”；
- 恢复和发布各自只执行一个受批次大小限制的批次，不在单轮内循环到数据库为空；
- 两个阶段不共享数据库事务，前一阶段已经提交的更新不会因后一阶段失败而回滚；
- 单轮结果分别返回恢复数量和成功发布数量，不把二者合并成一个含义模糊的总数；
- 并发恢复测试必须让多个恢复者使用同一份 `ExpiredLeaseCandidate` 快照；
- 恢复竞争仍依赖数据库条件更新，不增加 JVM 锁、分布式锁或 `SKIP LOCKED`；
- 旧 Worker 丢失租约后仍按 M3.1 语义明确失败，不能把失去所有权伪装成发布成功；
- 本阶段不创建后台线程、不定义轮询间隔，也不处理应用关闭；这些属于 M4 自动配置与运行时生命周期。

## 本阶段完成后的流程

```text
调用 runOnce()
        ↓
阶段一：限量查询过期 PUBLISHING
        ↓
逐条短事务条件恢复
        ├── 尚有次数 → RETRY_WAIT
        ├── 次数耗尽 → DEAD
        └── 候选失效 → 跳过
        ↓
阶段二：限量查询到期 PENDING / RETRY_WAIT
        ↓
逐条短事务条件抢占
        ↓
事务外调用 Sender
        ↓
按版本 + Owner + 有效期完成状态
        ↓
返回 recoveredCount + publishedCount
```

`runOnce()` 表示一次有界工作周期，不表示启动常驻发布器。调用方如果希望持续处理积压，必须在后续调度层重复调用。

## 单轮编排语义

### 1. 为什么先恢复再发布

过期的 `PUBLISHING` 事件已经不再拥有有效 Worker，但普通发布扫描不会读取它们。先执行恢复可以让这些事件尽早回到正常状态机，再由后续轮次根据 `next_attempt_at` 重新竞争。

顺序固定为：

```text
recoverExpiredLeases()
        ↓
publishDueEvents()
```

恢复进入 `RETRY_WAIT` 时仍然必须计算正数退避，因此在正常时钟条件下不会在同一轮立即重新发送。编排层不得绕过 `next_attempt_at`，也不得为了追求单轮完成而直接把恢复事件交给 Sender。

先恢复再发布还使单轮诊断顺序稳定：如果数据库中同时存在遗留租约和普通积压，恢复数量先确定，随后才产生本轮新的租约。

### 2. 每个阶段只执行一个批次

单轮分别遵守现有配置：

- 恢复阶段最多处理 `recoveryBatchSize` 个候选；
- 发布阶段最多处理 `batchSize` 个候选；
- 恢复数量不占用发布批次名额；
- 发布数量也不会触发额外恢复扫描；
- 即使某一阶段恰好处理满批次，也不在 `runOnce()` 内自动继续下一页。

这样可以为未来调度器提供可预测的单轮工作上限，避免一次调用因持续新增事件而长期不返回。

### 3. 阶段之间没有总事务

编排层不得使用一个事务包住恢复和发布：

- 恢复候选仍然逐条使用独立短事务；
- 普通抢占和状态完成仍然各自使用短事务；
- Sender 继续在数据库事务外调用；
- 发布阶段失败时，已经完成的恢复不会回滚；
- 后一个事件失败时，前面已经提交的事件状态不会回滚。

单轮结果是执行摘要，不是原子提交凭证。

### 4. 失败传播

保持现有错误边界，不在编排层静默吞掉异常：

- 恢复候选因并发变化导致条件更新零行，是正常竞争，计数为零并继续；
- Sender 抛出的普通发送异常继续由发布 Worker 分类为 `RETRY_WAIT` 或 `DEAD`；
- 恢复阶段发生数据库访问、事务或退避计算异常时，`runOnce()` 立即失败，不开始发布阶段；
- 发布阶段发生未被现有 Worker 处理的异常时，`runOnce()` 向上抛出；
- 旧 Worker 在完成状态时丢失租约，继续抛出 M3.1 定义的所有权异常；
- 只有两个阶段都正常返回时才创建单轮结果。

即使 `runOnce()` 抛出，异常发生前已经提交的短事务仍然有效。调用方不能根据“本轮失败”推断本轮没有修改任何记录。

## 编排组件

新增包内组件：

```text
JdbcEventPublicationCycle
```

它只依赖两个已有组件：

```text
JdbcExpiredLeaseRecovery
JdbcEventPublicationWorker
```

建议接口：

```java
final class JdbcEventPublicationCycle {

    PublicationCycleResult runOnce();
}
```

伪代码：

```text
runOnce()
    recoveredCount = recovery.recoverExpiredLeases()
    publishedCount = worker.publishDueEvents()
    return PublicationCycleResult(recoveredCount, publishedCount)
```

约束如下：

- 两个依赖都不能为空；
- 组件不直接访问 `JdbcTemplate`，不重复实现查询、恢复或发布逻辑；
- 组件不维护跨轮次可变状态；
- 组件不捕获并改写底层异常；
- 组件不使用循环、休眠、定时器或线程池；
- 组件保持包内可见，M4 再决定由哪个自动配置 Bean 和调度器调用。

## 单轮结果

新增包内不可变结果：

```text
PublicationCycleResult(
    recoveredCount,
    publishedCount
)
```

字段含义：

- `recoveredCount`：本轮条件恢复成功的过期租约数量，包括恢复到 `RETRY_WAIT` 和 `DEAD` 的记录；
- `publishedCount`：本轮 Sender 成功且状态成功更新为 `PUBLISHED` 的事件数量。

基本不变量：

- 两个计数都不能为负数；
- 查询到但竞争失败的恢复候选不计入 `recoveredCount`；
- 抢占失败、进入重试或进入死信的事件不计入 `publishedCount`；
- 结果不推断积压是否已经清空；
- M3.3 不进一步拆分恢复到重试和恢复到死信的数量，M4 指标设计时再决定是否扩展内部统计。

## 为并发测试保留同一候选快照

生产入口 `recoverExpiredLeases()` 会自行查询候选。如果两个线程各自调用这个入口，第二个线程可能在第一个线程提交后才执行查询，从而根本看不到候选。这只能证明先后扫描结果正确，不能证明两个恢复者使用同一份旧快照竞争时条件更新安全。

因此 `JdbcExpiredLeaseRecovery` 增加包内测试接缝：

```text
int recoverCandidates(List<ExpiredLeaseCandidate> candidates)
```

现有入口调整为：

```text
recoverExpiredLeases()
    candidates = repository.findExpiredLeaseCandidates(recoveryBatchSize)
    return recoverCandidates(candidates)
```

`recoverCandidates` 的语义：

- 输入列表不能为空；
- 按输入顺序逐条处理；
- 每个候选仍使用独立短事务；
- 每个候选仍根据自身 `attemptCount` 决定 `RETRY_WAIT` 或 `DEAD`；
- 条件更新成功才增加返回计数；
- 条件更新零行时继续处理后续候选；
- 不重新查询最新版本、Owner 或截止时间；
- 不为失败的旧候选自动生成新快照并重试；
- 保持包内可见，不作为业务调用 API。

这与现有 `JdbcEventPublicationWorker.publishCandidates(...)` 的测试接缝保持一致，使测试能够明确控制多个执行者看到的候选版本。

## 并发恢复协议

两个恢复者读取同一过期候选时：

```text
数据库：PUBLISHING, version = v1, owner = worker-old
        ↓
查询一次 ExpiredLeaseCandidate(v1, worker-old, leaseUntil1)
        ↓
Recovery A 与 Recovery B 同时使用这份候选
        ↓
两者执行相同条件更新
        ↓
一个影响 1 行：version 变为 v2
另一个影响 0 行：正常跳过
```

获胜恢复者由数据库更新顺序决定，Java 层不预先选主。最终必须满足：

- 两个恢复结果之和为 `1`；
- 最终只发生一次版本递增；
- `attempt_count` 不增加；
- 租约字段只被清空一次；
- 状态只进入一次 `RETRY_WAIT` 或 `DEAD`；
- 失败恢复者不能覆盖获胜者写入的 `next_attempt_at`、`last_error` 或最终状态。

测试至少覆盖恢复到 `RETRY_WAIT` 的分支。恢复到 `DEAD` 的耗尽分支也应使用相同并发方式覆盖，证明两个目标状态都受同一快照条件保护。

## 旧 Worker 与恢复者的竞态

### 场景一：旧 Sender 返回前租约已经被恢复

```text
Old Worker 抢占 v1，进入 Sender 并阻塞
        ↓
数据库时间上的租约到期
        ↓
Recovery 将 v1 恢复为 RETRY_WAIT，version = v2
        ↓
Old Sender 返回成功
        ↓
Old Worker 使用 v1 + old owner 标记 PUBLISHED
        ↓
条件更新 0 行，抛出所有权异常
```

最终记录必须保持恢复者写入的状态。旧 Worker 不能把它改为 `PUBLISHED`，也不能清除恢复原因或回退版本。

Sender 可能已经把消息交给 Broker，而 Outbox 没有记录成功，这是至少一次投递无法消除的未知结果窗口。M3.3 的目标是保护数据库新状态，不是假装能够判断 Broker 是否收到消息。

### 场景二：恢复后已经被新 Worker 重新抢占

更强的三方竞态如下：

```text
Old Worker 抢占：version = v1, owner = old
        ↓
Old Sender 阻塞，租约过期
        ↓
Recovery 恢复：version = v2, status = RETRY_WAIT
        ↓
到达 next_attempt_at
        ↓
New Worker 抢占：version = v3, owner = new
        ↓
New Sender 阻塞
        ↓
Old Sender 返回并尝试用 v1 完成
        ↓
更新 0 行，不能覆盖 v3 的新租约
        ↓
New Sender 返回并用 v3 完成
        ↓
PUBLISHED, version = v4
```

该场景必须证明：

- 旧 Worker 的失败更新不改变新 Owner、截止时间、版本或状态；
- 新 Worker 仍然能够使用自己的租约正常完成；
- `attempt_count = 2`，分别对应旧、新两次成功抢占；
- 恢复动作没有额外增加尝试次数；
- 最终版本只由旧抢占、恢复、新抢占和新完成各增加一次；
- 两次 Sender 调用符合至少一次投递语义，不应断言全流程只发送一次。

## 并发测试的同步方式

所有并发测试继续使用 MySQL 8.0.36 Testcontainers，并使用显式同步原语控制交错顺序：

- `CountDownLatch` 或 `CyclicBarrier` 确认多个任务已经准备完成；
- `ExecutorService` 启动独立恢复者或 Worker；
- 阻塞 Fake Sender 在调用发生后通知测试线程，再等待测试线程释放；
- `Future` 获取后台任务结果和异常；
- 每个等待都必须有明确超时，避免测试失败时永久挂起；
- `finally` 中释放阻塞点并关闭执行器；
- 不使用 `Thread.sleep(...)` 猜测线程是否已经到达目标位置；
- 不依赖两个独立扫描“恰好同时”读到同一候选；并发恢复必须显式复用同一份候选列表。

租约过期仍然通过数据库更新或数据库时间条件构造，不等待真实租约自然流逝。恢复后的重新抢占可以使用可控 `Clock` 到达已读取的 `next_attempt_at`，不得用长时间真实等待完成测试。

## 预计代码改动

### 新增 `PublicationCycleResult`

- 保存恢复数量和成功发布数量；
- 拒绝负数；
- 保持包内可见和不可变。

### 新增 `JdbcEventPublicationCycle`

- 依赖 `JdbcExpiredLeaseRecovery` 和 `JdbcEventPublicationWorker`；
- 校验依赖非空；
- `runOnce()` 先恢复、后发布；
- 正常完成后返回 `PublicationCycleResult`；
- 不增加事务、线程或调度职责。

### `JdbcExpiredLeaseRecovery`

- 抽取包内 `recoverCandidates(...)`；
- `recoverExpiredLeases()` 保留生产查询入口并委托给新方法；
- 不改变 M3.2 的恢复 SQL、退避、计数和异常语义。

### 测试支持

- 增加可阻塞、可计数的 Fake Sender，或在测试内部使用等价实现；
- 使用同步器精确控制旧 Worker 和新 Worker 的 Sender 返回时机；
- 提供读取当前状态、版本、Owner、截止时间和下一次尝试时间的断言辅助方法；
- 并发测试结束后确保线程池关闭，不遗留后台任务。

### 数据库表与 Repository

本阶段不修改表结构、索引和恢复条件 SQL。若现有包内 Repository 方法已经足以准备候选和读取状态，不为测试扩大生产 API。

## 单元测试清单

### `PublicationCycleResult`

至少覆盖：

1. 接受零和正数计数；
2. 拒绝负的恢复数量；
3. 拒绝负的发布数量。

### `JdbcEventPublicationCycle`

至少覆盖：

1. 拒绝空恢复组件；
2. 拒绝空发布 Worker；
3. 严格先调用恢复，再调用发布；
4. 分别保存两个阶段的返回数量；
5. 恢复阶段抛出异常后不调用发布阶段；
6. 发布阶段抛出的异常向上保留。

测试可以使用可控替身或 Mockito 验证调用顺序，不要求为此公开组件或增加生产接口。

### `recoverCandidates`

至少覆盖空列表返回零，并拒绝空列表引用。候选的字段不变量继续由 `ExpiredLeaseCandidateTest` 负责。

## MySQL 集成测试清单

继续使用 MySQL 8.0.36 Testcontainers，保留 M0 至 M3.2 的全部回归测试。

### 单轮同时执行恢复和发布

- 准备一条过期 `PUBLISHING` 事件和一条普通到期事件；
- 调用一次 `runOnce()`；
- 过期事件被恢复为 `RETRY_WAIT` 或在次数耗尽时进入 `DEAD`；
- 普通到期事件被发布为 `PUBLISHED`；
- 结果分别报告正确的恢复数量和发布数量；
- 恢复事件遵守退避，不因同轮编排而绕过 `next_attempt_at`。

调用顺序本身由单元测试精确验证，集成测试负责证明两个真实组件组合后保持各自数据库语义。

### 两个恢复者竞争同一可重试候选

- 准备一条仍有剩余次数的过期 `PUBLISHING` 事件；
- 只查询一次候选快照；
- 两个独立 `JdbcExpiredLeaseRecovery` 实例同时调用 `recoverCandidates`；
- 两个返回数量之和为 `1`；
- 最终状态为 `RETRY_WAIT`；
- 版本只增加一次，尝试次数不变；
- 租约字段被清空；
- `last_error` 和 `next_attempt_at` 来自获胜恢复更新。

### 两个恢复者竞争同一耗尽候选

- 准备一条 `attempt_count == max_attempts` 的过期事件；
- 两个恢复者使用同一候选快照并发执行；
- 只有一个恢复为 `DEAD`；
- 版本只增加一次，尝试次数不变；
- 不计算退避、不消费随机源；
- 失败恢复者不能再次修改死信记录。

### 旧 Sender 返回前租约被恢复

- 旧 Worker 成功抢占后阻塞在 Sender；
- 将其租约设置为数据库当前时间之前；
- 运行恢复并确认记录已经进入 `RETRY_WAIT`；
- 释放旧 Sender，让其返回成功；
- 旧 Worker 的后台任务以所有权异常结束；
- 数据库仍保持恢复后的状态、版本、错误原因和下一次尝试时间；
- 不能出现旧 Worker 写入的 `published_at`。

### 恢复、新抢占与旧 Sender 三方交错

- 旧 Worker 抢占后阻塞 Sender；
- 使租约过期并恢复到 `RETRY_WAIT`；
- 使用新 Worker ID 在到达 `next_attempt_at` 后重新抢占；
- 新 Worker 阻塞在自己的 Sender，确认数据库 Owner 和版本已经属于新租约；
- 释放旧 Sender，确认旧完成更新失败且新租约完全不变；
- 再释放新 Sender，确认最终进入 `PUBLISHED`；
- 最终 `attempt_count = 2`，版本符合四次成功状态变化；
- 两个 Sender 各被调用一次。

### 并发失败不能阻止后续轮次

- 完成一次旧 Worker 丢失所有权的竞态；
- 不复用失败任务的异常作为数据库状态；
- 再创建一个新单轮执行或 Worker；
- 证明后续有效候选仍可被扫描和处理。

此测试验证失败只结束当前调用，不会在组件内留下永久的 Java 锁或“正在运行”标志。

## 完成标准

- 存在一个同步、包内、单轮执行的恢复与发布编排组件；
- 单轮严格先恢复过期租约，再发布普通到期事件；
- 两个阶段各自只处理一个有界批次；
- 两个阶段不共享长事务，Sender 仍在事务外；
- 单轮结果分别表达恢复数量和成功发布数量；
- `JdbcExpiredLeaseRecovery` 能处理显式传入的同一候选快照；
- 两个恢复者并发使用同一快照时只有一个条件更新成功；
- 可重试和次数耗尽的并发恢复分支都保持版本与尝试次数不变量；
- 旧 Worker 在租约被恢复后不能写入成功、重试或死信状态；
- 事件被新 Worker 重新抢占后，旧 Worker 仍不能覆盖新 Owner、版本和租约；
- 三方竞态最终可以由新 Worker 正常完成；
- 并发测试使用同步原语和超时，不使用 `Thread.sleep(...)` 猜测时序；
- M0 至 M3.2 的 46 个已有测试继续通过；
- 新增单元测试和 MySQL 并发集成测试全部通过；
- `mvn clean verify` 在 Java 17 和 MySQL 8.0 下通过。

## 本阶段明确不做

- 启动后台定时调度器；
- 定义轮询间隔、线程名称或调度线程池；
- Worker 内并行发送或有界执行线程池；
- 独立 JVM 抢占后退出的故障注入；
- 独立 JVM 在 Sender 成功后、状态更新前退出的故障注入；
- 真实 RocketMQ 发送和 Broker 结果确认；
- 租约续期或心跳；
- `SKIP LOCKED` 替代并发协议；
- Spring Boot 自动配置、外部配置属性和优雅停机；
- Micrometer 指标和结构化日志；
- 死信人工重放；
- 无 Owner 或截止时间异常记录的自动修复。

## 阶段性边界

M3.3 完成后，可以表述项目已经通过真实 MySQL 并发测试验证“多个恢复者只能有一个成功接管同一过期租约”，并且已经证明旧 Worker 在恢复和重新抢占后不能覆盖新状态。

但这些竞态仍由同一测试进程使用同步器精确构造。项目尚未证明真实应用进程在以下位置退出后的端到端行为：

```text
抢占提交后、Sender 调用前退出
Sender 成功后、PUBLISHED 更新前退出
```

因此 M3.3 仍不能单独宣称完成全部宕机恢复故障注入，也不能宣称消除了重复消息。至少一次投递和消费者幂等要求保持不变。

## 后续顺序

```text
M3.3 并发接管与恢复编排
        ↓
M3.4 发布进程退出故障注入
        ↓
M4 RocketMQ 适配、Starter 与运行时调度
```
