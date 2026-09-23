# M2.1：基于版本号的条件抢占

> 状态：已完成
>
> 目标：在不引入租约、重试和线程池的前提下，让发布器通过数据库条件更新取得事件的唯一执行权，并缩短数据库事务持有时间。

## 为什么先做这一小步

M1 已经能够完成：

```text
扫描 PENDING 事件 → Fake 发送 → 标记为 PUBLISHED
```

但是当前 Worker 将“标记为 `PUBLISHING`、调用发送器、标记为 `PUBLISHED`”放在同一个数据库事务中。发送期间事务和行锁不会释放，这不适合后续接入真实网络发送，也没有把“查询到候选事件”和“真正获得执行权”明确区分开。

M2.1 只建立安全抢占所需的最小基础。完成后，候选查询仍然可以被多个实例同时看到，但只有版本匹配且条件更新成功的实例可以发送事件。

## 本阶段完成后的流程

```text
查询到期候选 id + version
        ↓
短事务：按 id + status + version 条件抢占
        ↓
更新成功：PENDING → PUBLISHING
attempt_count + 1，version + 1
        ↓
提交抢占事务
        ↓
事务外调用 EventSender
        ↓
短事务：按 id + PUBLISHING + 抢占版本标记 PUBLISHED
```

条件更新影响一行才表示抢占成功。影响零行表示事件已经被其他 Worker 抢占或候选快照已经过期，本实例必须跳过发送。

## 数据模型

现有表已经包含本阶段所需字段，无需修改建表 SQL：

- `status`：区分 `PENDING`、`PUBLISHING` 和 `PUBLISHED`；
- `attempt_count`：在成功抢占时增加，而不是在候选查询或发送完成时增加；
- `version`：作为乐观并发控制令牌；
- `updated_at`：记录本次状态更新发生的时间。

M2.1 不使用已有的 `lease_owner` 和 `lease_until` 字段。它们留到 M3。

## 候选查询

候选查询从只返回 `id` 改为返回不可变快照：

```text
EventCandidate(id, version)
```

查询条件仍然只包含已经到期的 `PENDING` 事件：

```sql
SELECT id, version
FROM reliable_event_outbox
WHERE status = 0
  AND next_attempt_at <= ?
ORDER BY next_attempt_at, id
LIMIT ?;
```

查询结果只是候选，不代表获得执行权。查询后到条件更新前，其他实例可以先一步完成抢占。

## 条件抢占

对每个候选事件执行：

```sql
UPDATE reliable_event_outbox
SET status = 1,
    attempt_count = attempt_count + 1,
    version = version + 1,
    updated_at = ?
WHERE id = ?
  AND status = 0
  AND next_attempt_at <= ?
  AND version = ?;
```

约束如下：

- 受影响行数为 `1`：抢占成功；
- 受影响行数为 `0`：抢占失败，直接跳过；
- 不允许先读取最新版本再重试条件更新，否则失去候选快照的并发保护意义；
- `attempt_count` 只在抢占成功时增加；
- 本阶段只允许从 `PENDING` 抢占，`RETRY_WAIT` 在 M2.3 引入。

MySQL 8.0 没有 `UPDATE ... RETURNING`。抢占成功后，在同一个短事务中按 ID 读取事件内容，并构造：

```text
ClaimedEvent(storedEvent, claimVersion)
```

其中 `claimVersion` 是条件更新后的版本，即候选版本加一。后续完成状态时必须携带该版本。

## 成功状态更新

发送成功后，使用抢占时获得的版本更新状态：

```sql
UPDATE reliable_event_outbox
SET status = 2,
    published_at = ?,
    updated_at = ?,
    version = version + 1
WHERE id = ?
  AND status = 1
  AND version = ?;
```

受影响行数必须为 `1`。如果为 `0`，说明当前 Worker 持有的状态快照已经失效，必须明确报错，不能把发送成功静默当成数据库状态更新成功。

M2.1 尚未引入 `lease_owner`，因此成功更新先使用 `id + status + version` 校验。M3 再增加租约所有者校验。

## 事务边界

本阶段需要调整当前 Worker 的事务范围：

1. 候选查询不包裹发送过程；
2. 每个候选事件使用一个短事务完成条件抢占和事件读取；
3. 抢占事务提交后，在数据库事务外调用 `EventSender`；
4. 发送成功后，使用另一个短事务更新为 `PUBLISHED`；
5. 不得在调用 `EventSender` 时持有数据库事务和行锁。

Worker 仍然可以按批次顺序处理，不在 M2.1 引入并行线程池。

## 预计代码改动

### `JdbcOutboxRepository`

- 将 `findDueEventIds` 改为返回包含 `id` 和 `version` 的候选对象；
- 用版本号条件抢占替换当前只校验状态的 `markPublishing`；
- 抢占成功后返回事件内容和抢占版本；
- `markPublished` 增加版本条件并递增版本；
- 条件更新失败时，不读取或发送事件。

### `JdbcEventPublicationWorker`

- 将一个覆盖完整发送流程的事务拆成两个短事务；
- 只有获得 `ClaimedEvent` 才调用发送器；
- 在事务外执行发送；
- 使用 `claimVersion` 完成成功状态更新；
- 保持现有批次大小和返回发布数量的语义。

### 内部模型

- 增加包内可见的候选快照类型；
- 增加包内可见的已抢占事件类型；
- 不改变 `ReliableEventPublisher` 等公开 API。

具体类型名称可以在实现时微调，但必须保留“候选快照”和“抢占令牌”两个不同概念，不能只在方法之间传递裸 `long id`。

## 测试清单

继续使用 MySQL 8.0 Testcontainers，至少增加以下验证：

1. 到期事件候选包含数据库中的当前版本；
2. 使用正确的候选版本能够抢占，状态变为 `PUBLISHING`；
3. 抢占成功后 `attempt_count` 和 `version` 各增加一次；
4. 使用同一个旧候选再次抢占时返回失败，计数和版本不再变化；
5. 未到 `next_attempt_at` 的事件不能被抢占；
6. 只有抢占成功的 Worker 会调用 Fake Sender；
7. 成功更新必须匹配抢占版本，并将状态改为 `PUBLISHED`；
8. 使用过期版本完成状态时必须失败，不能覆盖当前状态；
9. M1 的事务提交、回滚、幂等登记和未来事件过滤测试继续通过。

M2.1 先证明条件更新机制本身。两个 Worker 真正并发竞争同一事件的测试属于 M2.2。

## 完成标准

- 查询候选和获得执行权在代码与测试中是两个明确步骤；
- 抢占 SQL 同时校验事件 ID、`PENDING` 状态、到期时间和候选版本；
- 抢占成功才增加尝试次数和版本；
- 发送动作不持有数据库事务；
- 成功状态更新校验抢占版本；
- 现有公开 API 不发生变化；
- `mvn verify` 全部通过；
- 完成记录只描述已有测试能够证明的行为，不提前声称已经完成双实例验证。

## 本阶段明确不做

- 双实例并发竞争测试；
- `RETRY_WAIT` 和指数退避；
- 最大尝试次数判定和 `DEAD`；
- `lease_owner`、`lease_until` 和过期租约恢复；
- RocketMQ 发送适配；
- 并行线程池和后台定时调度；
- Spring Boot 自动配置和 Micrometer 指标。

## 阶段性边界

抢占事务在发送前提交后，如果发送器抛出异常或进程退出，事件会暂时停留在 `PUBLISHING`：

- 普通发送失败由 M2.3 更新为 `RETRY_WAIT`；
- 进程宕机造成的遗留 `PUBLISHING` 由 M3 的租约过期恢复处理。

因此 M2.1 是可靠性能力的中间增量，不是可以单独发布的完整版本。此处不加入临时回滚或立即重置为 `PENDING` 的逻辑，避免制造无退避的高频重试和后续需要删除的过渡行为。

## 后续顺序

```text
M2.1 版本号条件抢占
        ↓
M2.2 双实例并发竞争测试
        ↓
M2.3 指数退避与 RETRY_WAIT
        ↓
M2.4 最大尝试次数与 DEAD
```
