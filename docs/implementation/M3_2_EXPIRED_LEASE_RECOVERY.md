# M3.2：过期租约恢复

> 状态：已完成
>
> 目标：限量扫描已经过期的 `PUBLISHING` 事件，使用候选版本、租约 Owner 和截止时间进行条件恢复，在不覆盖活跃 Worker 的前提下将事件转为 `RETRY_WAIT` 或 `DEAD`。

## 为什么在 M3.1 之后做恢复

M3.1 已经为每次成功抢占写入数据库时间租约，并要求成功、重试和死信更新同时校验版本、Owner 和租约有效期。当前流程能够阻止错误 Owner、旧版本和过期租约完成状态，但还不能主动处理遗留事件：

```text
Worker 抢占事件
        ↓
PUBLISHING
lease_owner = worker-a
lease_until = 数据库时间 + 30s
        ↓
Worker 在完成状态前退出
        ↓
lease_until 到期
        ↓
事件仍然停留在 PUBLISHING
```

普通候选扫描只接受 `PENDING` 和 `RETRY_WAIT`，因此这些记录不会自行重新发布。M3.2 增加一个显式的过期租约恢复入口，让其他 Worker 能够把已经失去有效所有者的事件重新放回状态机。

本阶段只实现恢复机制本身和顺序集成测试。多个恢复 Worker 同时竞争、恢复与旧发送者交错执行的完整竞态测试属于 M3.3；独立 JVM 退出故障注入属于 M3.4。

## 本阶段完成后的流程

```text
限量查询过期 PUBLISHING 候选
        ↓
ExpiredLeaseCandidate
id + version + leaseOwner + leaseUntil
attemptCount + maxAttempts
        ↓
逐条短事务条件恢复
        ↓
再次校验：
id + PUBLISHING + version + Owner
+ leaseUntil 快照 + leaseUntil 已过期
        ↓
attemptCount < maxAttempts
        ├── 是 → RETRY_WAIT
        │         next_attempt_at = 数据库时间 + backoff
        │
        └── 否 → DEAD
        ↓
清空租约字段，version + 1
```

查询到候选不代表恢复成功。只有条件更新影响一行，才表示当前恢复者成功处理了该过期租约。

## 恢复语义

### 1. 什么是过期租约

M3.2 沿用 M3.1 已冻结的数据库时间边界：

```text
status = PUBLISHING
lease_until <= UTC_TIMESTAMP(3)
```

等于数据库当前时间时视为已经过期。查询候选和条件恢复都必须使用 MySQL 时间，不能由恢复 Worker 的 JVM 本地时钟判断其他 Worker 的所有权。

### 2. 过期不等于 Broker 发送失败

租约过期只能证明原 Worker 没有在有效期内完成数据库状态，无法判断发送动作是否发生：

```text
情况 A：抢占后、发送前退出
情况 B：发送过程中退出
情况 C：Broker 已接收，标记 PUBLISHED 前退出
情况 D：Sender 返回失败，写入 RETRY_WAIT 前退出
```

因此恢复不能伪造“Broker 不可用”或其他发送异常。`last_error` 使用固定、独立的恢复原因：

```text
Publication lease expired before completion
```

该文本不拼接 Payload、Headers、异常堆栈或访问凭据。原租约 Owner 和截止时间由恢复候选提供，M4 增加结构化日志时再作为独立字段记录。

### 3. 尝试次数

`attempt_count` 已经在原 Worker 成功抢占时增加。恢复动作不能再次增加尝试次数。

例如：

```text
首次抢占成功 → attempt_count = 1
Worker 退出
租约过期恢复 → attempt_count 仍为 1
再次到期并被新 Worker 抢占 → attempt_count = 2
```

这保证 `max_attempts` 继续表示最大总发送尝试次数，而不是“抢占次数加恢复次数”。

### 4. 恢复目标状态

恢复决策只取决于持久化的尝试次数：

```text
attempt_count < max_attempts → RETRY_WAIT
attempt_count >= max_attempts → DEAD
```

最后一次允许的发送如果在完成状态前失去租约，系统无法确认 Broker 是否收到消息。由于没有剩余自动尝试次数，事件进入 `DEAD`，并保存租约过期原因。

这可能出现“Broker 已收到消息，但 Outbox 最终为 `DEAD`”的未知结果窗口。它是至少一次投递和有限重试共同产生的边界，不能通过把次数重置或额外赠送一次尝试来隐藏。

### 5. 恢复后的退避

仍有剩余次数时，恢复进入 `RETRY_WAIT`，并使用现有 `ExponentialBackoff` 根据当前 `attempt_count` 计算延迟：

```text
delay = backoff.nextDelay(attemptCount)
next_attempt_at = UTC_TIMESTAMP(3) + delay
```

恢复时间和下一次可尝试时间统一使用数据库时间。这样不同恢复实例的 JVM 时钟偏差不会让事件立即重试或异常延后。

M3.2 不修改普通 Sender 失败路径；普通失败仍使用当前可注入 `Clock` 计算 `failedAt` 和 `nextAttemptAt`。是否在后续阶段统一所有调度时间来源，需要单独评估，不在 M3.2 扩大范围。

## 过期租约候选

新增包内不可变候选类型：

```text
ExpiredLeaseCandidate(
    eventId,
    version,
    leaseOwner,
    leaseUntil,
    attemptCount,
    maxAttempts
)
```

字段含义：

- `eventId`：目标事件；
- `version`：查询候选时的当前版本；
- `leaseOwner`：原抢占 Worker；
- `leaseUntil`：查询候选时的租约截止时间；
- `attemptCount`：原抢占已经消耗的发送尝试次数；
- `maxAttempts`：该事件允许的最大总发送次数。

基本不变量：

- 事件 ID 必须有效；
- 版本必须为正数，因为只有抢占后的事件才可能处于 `PUBLISHING`；
- Owner 不能为空或空白；
- 截止时间不能为空；
- `attemptCount` 和 `maxAttempts` 必须为正数；
- `attemptCount` 不得大于 `maxAttempts`。

候选不携带 Payload 和 Headers。恢复只修改发布状态，不需要读取或反序列化事件内容。

建议提供包内方法：

```text
canRetry() → attemptCount < maxAttempts
```

避免恢复组件在多个位置重复解释最大尝试次数。

## 候选查询

Repository 增加限量查询：

```sql
SELECT id,
       version,
       lease_owner,
       lease_until,
       attempt_count,
       max_attempts
FROM reliable_event_outbox
WHERE status = 1
  AND lease_owner IS NOT NULL
  AND TRIM(lease_owner) <> ''
  AND lease_until <= UTC_TIMESTAMP(3)
ORDER BY lease_until, id
LIMIT ?;
```

查询使用现有索引：

```text
idx_lease_recovery (status, lease_until, id)
```

约束如下：

- `limit` 必须为正数；
- 每轮只读取有限数量，不能一次加载全部过期记录；
- 按最早过期时间和事件 ID 排序，保证处理顺序稳定；
- 查询不加长事务锁，也不把结果视为已获得恢复权；
- `lease_until IS NULL` 的异常 `PUBLISHING` 记录不自动恢复；
- `lease_owner IS NULL` 或空白的异常记录被查询条件排除，应作为数据完整性问题单独诊断。

M3.1 之后正常抢占一定同时写入 Owner 和截止时间。项目尚未公开发布，不为 M2 时期可能遗留的无租约 `PUBLISHING` 记录增加自动迁移逻辑。

## 条件恢复为 RETRY_WAIT

候选仍有剩余尝试次数时执行：

```sql
UPDATE reliable_event_outbox
SET status = 3,
    next_attempt_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)),
    last_error = ?,
    lease_owner = NULL,
    lease_until = NULL,
    updated_at = UTC_TIMESTAMP(3),
    version = version + 1
WHERE id = ?
  AND status = 1
  AND version = ?
  AND lease_owner = ?
  AND lease_until = ?
  AND lease_until <= UTC_TIMESTAMP(3);
```

参数包含：

- 根据 `attemptCount` 计算的退避微秒数；
- 固定租约过期原因；
- 候选事件 ID；
- 候选版本；
- 候选 Owner；
- 候选截止时间。

受影响行数：

- `1`：恢复成功；
- `0`：候选已失效，跳过，不抛出所有权异常。

恢复候选失效是预期并发现象，例如另一个恢复 Worker 已经先完成更新，或者未来的租约续期使截止时间发生变化。它与发送完成时丢失租约不同，不应让整轮恢复失败。

## 条件恢复为 DEAD

候选已经耗尽发送次数时执行：

```sql
UPDATE reliable_event_outbox
SET status = 4,
    last_error = ?,
    lease_owner = NULL,
    lease_until = NULL,
    updated_at = UTC_TIMESTAMP(3),
    version = version + 1
WHERE id = ?
  AND status = 1
  AND version = ?
  AND lease_owner = ?
  AND lease_until = ?
  AND lease_until <= UTC_TIMESTAMP(3);
```

进入 `DEAD` 时：

- 不计算退避；
- 不消费随机源；
- 不修改 `attempt_count`；
- 不修改 `next_attempt_at`；
- 不写入 `published_at`；
- 保存固定租约过期原因；
- 清空租约字段并递增版本。

恢复为 `DEAD` 的条件更新同样返回布尔结果，零行表示候选已经失效。

## 为什么恢复条件要包含截止时间快照

版本和 Owner 已经能防止大多数旧候选写入，但恢复操作还应校验查询时的 `lease_until`：

```text
id + PUBLISHING + version + leaseOwner + leaseUntil
```

原因包括：

- 未来如果增加租约续期，Owner 可能不变但截止时间会延长；
- 某些维护操作可能修正截止时间而未改变 Owner；
- 候选必须证明自己恢复的是查询时看到的同一份过期租约，而不只是同一个 Worker 名称。

即使未来租约续期同时递增版本，保留截止时间条件仍能让 SQL 的意图更明确。

## 恢复组件

新增包内组件，例如：

```text
JdbcExpiredLeaseRecovery
```

建议依赖：

```text
JdbcOutboxRepository
TransactionTemplate
ExponentialBackoff
recoveryBatchSize
```

公开给内部调度层的方法保持很小：

```text
int recoverExpiredLeases()
```

返回值只统计条件更新成功的事件数量，不统计查询到但因竞争而跳过的候选。

执行流程：

```text
candidates = repository.findExpiredLeaseCandidates(batchSize)
recoveredCount = 0

for candidate in candidates
    if candidate.canRetry()
        delay = backoff.nextDelay(candidate.attemptCount)
        短事务条件恢复为 RETRY_WAIT
    else
        短事务条件恢复为 DEAD

    条件更新成功
        recoveredCount + 1

return recoveredCount
```

约束如下：

- 候选查询不包裹整个恢复批次；
- 每个候选使用独立短事务；
- 一个候选条件冲突后继续处理后续候选；
- 数据库访问异常必须向上抛出，不能谎称已经恢复；
- 退避计算异常必须向上抛出；
- 不捕获 `Error`；
- 组件不依赖 `EventSender`，恢复动作本身不发送消息；
- M3.2 不自动从 `publishDueEvents()` 调用恢复组件，调度顺序留到 M3.3 冻结。

## 事务与并发边界

候选查询和恢复更新之间允许其他 Worker 改变事件。安全性由条件更新保证，不依赖 Java 锁或数据库长事务。

```text
Recovery A 查询到候选 v1
Recovery B 查询到同一候选 v1
        ↓
A 条件更新成功，version 变为 v2
        ↓
B 使用 v1 更新零行并跳过
```

M3.2 至少通过顺序方式验证旧候选只能恢复一次。两个恢复组件真正并发使用同一候选、以及旧发送 Worker 与恢复 Worker 交错的测试属于 M3.3。

## 预计代码改动

### 新增 `ExpiredLeaseCandidate`

- 保存事件 ID、版本、Owner、截止时间、尝试次数和最大次数；
- 校验基本不变量；
- 提供 `canRetry()`；
- 保持包内可见和不可变。

### `JdbcOutboxRepository`

- 增加限量查询过期租约候选的方法；
- 增加条件恢复为 `RETRY_WAIT` 的方法，接收候选、退避时间和固定错误原因；
- 增加条件恢复为 `DEAD` 的方法；
- 两种恢复方法返回 `boolean`，零行表示候选失效；
- 恢复成功时清空租约字段并递增版本；
- 查询和更新全部使用 `UTC_TIMESTAMP(3)` 判断租约过期。

### 新增 `JdbcExpiredLeaseRecovery`

- 注入 JDBC、事务管理器、退避组件和批量大小；
- 限量查询候选并逐条使用短事务恢复；
- 仍可重试时才计算退避；
- 返回本轮成功恢复的事件数量；
- 不调用 Sender，不承担后台调度职责。

### 数据库表

M3.2 不修改表结构。现有字段和 `idx_lease_recovery` 已满足本阶段需求。

## 单元测试清单

### `ExpiredLeaseCandidate`

至少覆盖：

1. 合法候选能够构造；
2. 版本必须为正数；
3. Owner 不能为空或空白；
4. 截止时间不能为空；
5. 尝试次数和最大次数必须为正数；
6. 尝试次数不得超过最大次数；
7. `attemptCount < maxAttempts` 时 `canRetry()` 为真；
8. `attemptCount == maxAttempts` 时 `canRetry()` 为假。

### 恢复组件配置

至少覆盖：

- `recoveryBatchSize` 必须为正数；
- 空 Repository、事务管理器或退避组件被拒绝；
- 已耗尽候选不调用退避随机源。

如果依赖校验在 MySQL 集成测试中更自然，可以保留包内构造器并在那里覆盖，不为测试扩大公开 API。

## MySQL 集成测试清单

继续使用 MySQL 8.0.36 Testcontainers，不使用真实等待。

### 活跃租约不会被查询或恢复

- 抢占一条事件并保留未来截止时间；
- 查询过期租约候选时结果为空；
- 运行恢复组件返回 `0`；
- 状态、版本、Owner、截止时间和尝试次数保持不变。

### 过期租约恢复为 RETRY_WAIT

- 使用 `max_attempts > attempt_count` 的事件；
- 抢占后将 `lease_until` 设置为数据库当前时间之前；
- 查询候选能够读取正确的版本、Owner、截止时间和尝试次数；
- 运行恢复组件返回 `1`；
- 状态变为 `RETRY_WAIT`；
- `attempt_count` 不变；
- `version` 增加一次；
- `lease_owner` 和 `lease_until` 被清空；
- `last_error` 等于固定租约过期原因；
- `next_attempt_at` 等于恢复数据库时间加确定性退避，并落在可验证区间内；
- `published_at` 保持为空。

测试应使用固定随机源，并读取恢复前后的数据库时间验证 `next_attempt_at`，不能用 JVM 固定时钟与数据库时间做绝对等值比较。

### 恢复后未到时间不能重新抢占

- 将普通发布 Worker 的 `Clock` 设置为 `next_attempt_at` 之前；
- 事件不能进入普通候选列表；
- 到达 `next_attempt_at` 后可以重新抢占；
- 新抢占使 `attempt_count` 再增加一次，并获得新的租约。

### 次数耗尽后恢复为 DEAD

- 使用 `max_attempts = 1` 的事件；
- 第一次抢占后令租约过期；
- 恢复组件将事件转为 `DEAD`；
- `attempt_count = 1`；
- `version = 2`，即抢占和恢复各增加一次；
- 租约字段被清空；
- `last_error` 等于固定租约过期原因；
- `published_at` 为空；
- 不计算退避、不消费随机源；
- 后续普通扫描不会再看到该事件。

### 旧恢复候选不能重复更新

- 查询并保存一份过期候选；
- 第一次条件恢复返回成功；
- 再次使用同一候选恢复返回失败；
- 状态、版本、尝试次数和错误原因不再变化。

### Owner、版本或截止时间变化时跳过

分别构造以下变化：

- 数据库版本前进；
- 租约 Owner 改变；
- 截止时间被延长到未来；
- 截止时间仍过期但不等于候选快照。

旧候选的条件恢复必须返回失败，不能覆盖当前记录。

### 恢复数量受批次限制

- 准备多条过期 `PUBLISHING` 事件；
- 使用小于事件数量的 `recoveryBatchSize`；
- 单轮成功恢复数量不超过批次上限；
- 优先恢复 `lease_until` 最早、ID 更小的记录；
- 未进入本轮候选的记录保持不变，可在下一轮恢复。

### 异常无租约记录不自动恢复

- 构造 `PUBLISHING` 但 Owner 或截止时间为空的异常记录；
- 候选查询不返回该记录；
- 恢复组件不修改它；
- 测试明确说明这是数据完整性问题，不是可安全推断的过期租约。

## 完成标准

- 存在独立的过期租约候选模型；
- 候选查询只返回使用数据库时间判断为过期的 `PUBLISHING` 记录；
- 查询受正数批次大小限制，并使用 `idx_lease_recovery` 对应的排序；
- 恢复更新同时校验事件 ID、状态、版本、Owner、截止时间快照和截止时间已经过期；
- 有剩余次数的事件进入 `RETRY_WAIT`；
- 已耗尽次数的事件进入 `DEAD`；
- 恢复不增加 `attempt_count`；
- 恢复成功清空租约字段并递增版本；
- 恢复原因与 Broker 发送失败明确区分；
- 恢复退避基于当前尝试次数，并使用数据库时间生成下一次可用时间；
- 失效候选返回失败并跳过，不覆盖当前状态；
- 恢复组件不调用 Sender，也不持有覆盖整个批次的长事务；
- M0 至 M3.1 的 34 个已有测试继续通过；
- 新增单元测试和 MySQL 8.0 集成测试全部通过；
- `mvn clean verify` 在 Java 17 和 MySQL 8.0 下通过。

## 本阶段明确不做

- 从普通发布循环自动调用恢复组件；
- 两个恢复 Worker 的真实并发竞争测试；
- 旧发送 Worker、恢复 Worker 和新抢占 Worker 的三方竞态测试；
- 独立 JVM 抢占后退出故障注入；
- Sender 成功后、状态更新前退出的重复发送测试；
- 租约续期或心跳；
- 后台定时调度和并行线程池；
- RocketMQ 发送适配；
- Spring Boot 自动配置和外部配置属性；
- Micrometer 租约恢复指标和结构化日志；
- 自动修复无 Owner 或无截止时间的异常 `PUBLISHING` 记录；
- 死信人工重放。

## 阶段性边界

M3.2 完成后，代码已经能够显式调用恢复组件处理过期租约，但还没有证明多个恢复实例和旧发送者真正并发时的完整行为，也不会由现有发布循环自动触发恢复。

项目可以表述已经实现“过期租约条件恢复机制”，但仍不能表述已经完成：

- 多实例恢复竞争验证；
- 发布进程退出故障注入；
- 完整宕机恢复闭环；
- 自动后台恢复调度。

M3.3 将把恢复动作放入明确的单轮执行顺序，并使用真实 MySQL 并发测试证明：同一过期租约只有一个恢复者成功，恢复或重新抢占后旧 Worker 不能覆盖新状态。

## 后续顺序

```text
M3.2 过期租约条件恢复
        ↓
M3.3 并发接管与恢复编排
        ↓
M3.4 发布进程退出故障注入
```
