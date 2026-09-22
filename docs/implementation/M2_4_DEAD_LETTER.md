# M2.4：最大尝试次数与死信

> 状态：已完成
>
> 目标：限制每条事件的总发送次数，将重试耗尽或明确不可重试的事件转为 `DEAD`，并保证死信事件不再参与自动扫描和发送。

## 为什么 M2 必须在这里收口

M2.3 已经能够把普通发送失败转为 `RETRY_WAIT`，并在退避时间到达后重新抢占。但是当前所有 Sender `RuntimeException` 都被视为可重试，而且数据库中的 `max_attempts` 尚未参与决策。

如果不补上终止条件，永久失败事件会无限循环：

```text
PUBLISHING → RETRY_WAIT → PUBLISHING → RETRY_WAIT → ...
```

M2.4 增加两个终止入口：

```text
可重试错误 + 尝试次数耗尽 → DEAD
明确不可重试错误           → DEAD
```

完成本阶段后，M2 的状态机闭合。租约、进程退出恢复和故障注入仍属于 M3。

## `max_attempts` 的准确语义

`max_attempts` 表示允许调用 Sender 的最大总次数，不是“首次发送之外还能重试多少次”。

示例：

```text
max_attempts = 1
第 1 次发送失败 → DEAD

max_attempts = 3
第 1 次发送失败 → RETRY_WAIT
第 2 次发送失败 → RETRY_WAIT
第 3 次发送失败 → DEAD
```

`attempt_count` 仍然只在条件抢占成功时增加。失败状态更新不能再次增加次数。

如果最后一次允许的发送成功，事件进入 `PUBLISHED`，不能仅因为 `attempt_count == max_attempts` 就进入 `DEAD`。

## 状态机变化

为 `EventStatus` 增加：

```text
DEAD(4)
```

M2 完成后的状态机为：

```text
PENDING ───────→ PUBLISHING ───────→ PUBLISHED
                    │
                    ├──────────────→ RETRY_WAIT ─────→ PUBLISHING
                    │
                    └──────────────→ DEAD
```

终态规则：

- `PUBLISHED` 不再参与扫描；
- `DEAD` 不再参与扫描；
- M2.4 不提供 `DEAD` 的自动恢复或人工重放；
- 后续如需人工重放，必须设计显式接口，不能通过后台扫描偷偷复活死信。

## 抢占结果需要携带最大次数

Worker 在发送失败后必须同时知道本次尝试次数和最大次数。因此抢占结果扩展为：

```text
ClaimedEvent(storedEvent, claimVersion, attemptCount, maxAttempts)
```

约束如下：

- `attemptCount >= 1`；
- `maxAttempts >= 1`；
- 正常抢占结果必须满足 `attemptCount <= maxAttempts`；
- 两个值都来自抢占成功后的数据库记录，不能读取 Worker 本地默认值代替；
- 同一张表可以存在不同 `max_attempts` 的事件，终止判断必须逐事件执行。

## 候选扫描和抢占上限

候选扫描增加剩余次数条件：

```sql
SELECT id, version
FROM reliable_event_outbox
WHERE status IN (0, 3)
  AND next_attempt_at <= ?
  AND attempt_count < max_attempts
ORDER BY next_attempt_at, id
LIMIT ?;
```

条件抢占必须重复校验：

```sql
UPDATE reliable_event_outbox
SET status = 1,
    attempt_count = attempt_count + 1,
    version = version + 1,
    updated_at = ?
WHERE id = ?
  AND status IN (0, 3)
  AND next_attempt_at <= ?
  AND attempt_count < max_attempts
  AND version = ?;
```

这样即使两个 Worker 持有旧候选，或者数据库中出现已经达到上限的 `RETRY_WAIT` 记录，也不会发生第 `max_attempts + 1` 次发送。

正确的 M2.4 流程不会产生“达到上限但仍为 `RETRY_WAIT`”的新记录：最后一次发送失败时必须直接写入 `DEAD`。开发阶段遗留的异常测试数据可以清理，本阶段不增加面向已发布版本的数据修复任务。

## 失败决策顺序

Sender 抛出 `RuntimeException` 后，按以下顺序决定状态：

```text
1. 生成安全的 last_error 摘要
2. 对发送异常进行分类
3. 如果不可重试 → DEAD
4. 否则如果 attemptCount >= maxAttempts → DEAD
5. 否则 → 计算退避并进入 RETRY_WAIT
```

不可重试错误优先于次数判断。进入 `DEAD` 时不计算退避，也不调用随机源。

发送成功仍然直接进入 `PUBLISHED`。最大次数只约束失败后的下一步，不预先阻止本次已经合法抢占的发送。

## 错误分类模型

M2.4 增加一个包内错误分类结果：

```text
RETRYABLE
NON_RETRYABLE
```

同时提供一个内部发送异常类型或语义等价的最小机制，让 Sender 明确标记错误是否可重试。例如：

```text
EventSendException.retryable(message, cause)
EventSendException.nonRetryable(message, cause)
```

分类规则：

- 明确标记为 `NON_RETRYABLE` 的发送异常直接进入 `DEAD`；
- 明确标记为 `RETRYABLE` 的发送异常按次数决定重试或死信；
- 未知的普通 `RuntimeException` 默认视为 `RETRYABLE`，避免因为未识别异常直接丢弃事件；
- 不通过异常消息文本匹配判断类型；
- 不捕获 `Error`；
- 分类组件只判断失败性质，不访问数据库、不计算退避。

当前 Fake Sender 用类型化异常验证分类。M4 接入 RocketMQ 时，再由适配器把 Broker、网络、配置和消息大小错误映射为这些内部语义。

### 计划中的分类对应关系

可重试：

- Broker 暂时不可用；
- 网络超时或连接失败；
- 服务端明确返回可恢复错误；
- 未识别的普通运行时发送异常。

不可重试：

- 事件类型没有目标映射；
- Topic 或 Tag 配置非法；
- 消息超过 Broker 限制；
- 适配器明确判断继续尝试不会改变结果的错误。

Payload 在登记阶段序列化失败时不会产生 Outbox 记录，因此不进入后台死信流程。

## 死信状态更新

进入 `DEAD` 时执行短事务：

```sql
UPDATE reliable_event_outbox
SET status = 4,
    last_error = ?,
    updated_at = ?,
    version = version + 1
WHERE id = ?
  AND status = 1
  AND version = ?;
```

约束如下：

- 使用当前 `ClaimedEvent.claimVersion`；
- 受影响行数必须为 `1`，否则明确报错；
- 不增加 `attempt_count`；
- 不写入 `published_at`；
- 不修改 `next_attempt_at`，因为 `DEAD` 不再有下一次自动尝试；
- `last_error` 使用 M2.3 已实现的安全摘要；
- `updated_at` 表示进入死信状态的时间；
- 本阶段不新增 `dead_at` 字段。

`version` 在抢占和死信更新时各增加一次，因此一条首次失败即死信的事件最终版本为 `2`。

## Worker 行为

发送失败后的 Worker 逻辑调整为：

```text
catch RuntimeException from sender
    failedAt = clock.instant()
    lastError = failureSummary(exception)
    failureType = classifier.classify(exception)

    if failureType == NON_RETRYABLE
       or attemptCount >= maxAttempts
        短事务标记 DEAD
    else
        计算退避
        短事务标记 RETRY_WAIT

    publishedCount 不变
    继续处理下一个候选
```

捕获范围继续只包围 Sender 调用。分类、退避或数据库状态更新失败时，异常必须向上抛出，不能再次被当成发送失败。

一个事件进入 `DEAD` 后，Worker 继续处理当前批次中的其他候选。

## 预计代码改动

### `EventStatus`

- 增加 `DEAD(4)`。

### `ClaimedEvent`

- 增加 `maxAttempts`；
- 校验尝试次数和最大次数的基本不变量。

### `JdbcOutboxRepository`

- 候选扫描和条件抢占增加 `attempt_count < max_attempts`；
- 抢占成功后读取 `max_attempts`；
- 增加按抢占版本标记 `DEAD` 的方法；
- 保持 `PUBLISHED`、`RETRY_WAIT` 和 `DEAD` 更新之间互斥。

### `JdbcEventPublicationWorker`

- 注入或持有内部错误分类组件；
- 发送失败后先分类，再判断尝试次数；
- 只有仍可重试时才计算退避；
- 进入 `DEAD` 后继续处理同批次其他事件；
- 返回值仍然只统计进入 `PUBLISHED` 的事件。

### 内部错误类型

- 增加可重试和不可重试的内部发送失败语义；
- 未知 `RuntimeException` 默认可重试；
- 不改变业务方使用的 `ReliableEventPublisher` 公共接口。

## 单元测试清单

错误分类至少覆盖：

1. 显式可重试异常分类为 `RETRYABLE`；
2. 显式不可重试异常分类为 `NON_RETRYABLE`；
3. 未知普通 `RuntimeException` 默认分类为 `RETRYABLE`；
4. 分类不依赖异常消息内容；
5. 异常类型能够保留原始 cause。

失败决策至少覆盖：

- 可重试错误且 `attemptCount < maxAttempts` 选择 `RETRY_WAIT`；
- 可重试错误且 `attemptCount == maxAttempts` 选择 `DEAD`；
- 不可重试错误在第一次尝试就选择 `DEAD`；
- 最后一次发送成功仍可进入 `PUBLISHED`。

如果失败决策直接保留在 Worker 中，可以通过集成测试覆盖，不要求为了测试强行增加公开组件。

## MySQL 集成测试清单

### 可重试错误耗尽次数

- 使用 `max_attempts = 2` 登记事件；
- 第一次发送失败后进入 `RETRY_WAIT`；
- 到期后第二次发送仍失败，进入 `DEAD`；
- Sender 总调用次数为 `2`；
- 最终 `attempt_count = 2`；
- 最终 `version = 4`；
- `published_at` 为空；
- `last_error` 保存最后一次失败摘要。

### 死信不再扫描

- 在事件进入 `DEAD` 后再次推进时钟并运行 Worker；
- Sender 调用次数不再增加；
- 状态、尝试次数和版本保持不变。

### 不可重试错误立即死信

- 使用大于 `1` 的 `max_attempts`；
- Sender 第一次调用抛出明确不可重试异常；
- 事件直接进入 `DEAD`；
- `attempt_count = 1`；
- `version = 2`；
- 不计算或消费退避随机数。

### 最后一次允许的发送成功

- 使用 `max_attempts = 2`；
- 第一次发送失败进入 `RETRY_WAIT`；
- 第二次发送成功进入 `PUBLISHED`；
- 不进入 `DEAD`；
- `attempt_count = 2`；
- 最终 `version = 4`。

### 死信不阻塞同批次其他事件

- 同一批次准备两个到期事件；
- 第一个事件发生不可重试错误并进入 `DEAD`；
- 第二个事件发送成功并进入 `PUBLISHED`；
- Worker 返回成功数量 `1`。

### 旧抢占版本不能写入死信

- 抢占事件后使数据库版本前进；
- 使用旧 `ClaimedEvent` 标记 `DEAD`；
- 条件更新影响零行并明确失败；
- 当前数据库状态不被覆盖。

测试继续使用固定 `Clock`、确定性退避和 MySQL 5.7 Testcontainers，不依赖真实等待。

## 完成标准

- `DEAD(4)` 已进入状态模型；
- `max_attempts` 被解释为最大总发送次数；
- 候选扫描和条件抢占不会超过最大次数；
- 可重试错误在次数耗尽前进入 `RETRY_WAIT`；
- 可重试错误在最后一次失败后进入 `DEAD`；
- 明确不可重试错误第一次失败即可进入 `DEAD`；
- 未知运行时发送异常保守地按可重试处理；
- `DEAD` 事件不再参与扫描；
- 死信更新校验抢占版本并保存错误摘要；
- 一个事件进入死信不会阻断同批次其他候选；
- M0 至 M2.3 的已有测试继续通过；
- `mvn clean verify` 在 Java 17 和 MySQL 5.7 下通过。

## 本阶段明确不做

- 死信人工重放接口；
- 死信管理后台；
- `lease_owner`、`lease_until` 和租约过期恢复；
- 发布进程退出故障注入；
- RocketMQ 具体异常映射；
- Worker 内部并行线程池和后台定时调度；
- Spring Boot 自动配置、指标和告警。

## 阶段性边界

M2.4 能处理 Sender 明确返回的成功或异常，但仍不能覆盖进程突然退出：

```text
抢占并提交 PUBLISHING
        ↓
进程退出，没有机会写入 RETRY_WAIT 或 DEAD
        ↓
事件停留在 PUBLISHING
```

该问题必须由 M3 的租约所有权、租约过期扫描和恢复测试解决。

此外，`DEAD` 当前只是数据库终态，没有人工查询、告警或重放接口。M4 的指标需要暴露死信数量，人工重放是否加入留到 `0.2.0` 决定。

## M2 完成后的状态

完成 M2.4 后，可以表述已经实现：

- MySQL 5.7 版本号条件抢占；
- 双 Worker 并发竞争保护；
- 带随机抖动的指数退避；
- 最大尝试次数；
- 不可重试错误分类；
- 死信终态。

仍然不能表述已经实现：

- 租约和发布进程宕机恢复；
- 完整多节点故障恢复；
- RocketMQ 生产适配；
- Spring Boot Starter 自动配置；
- 生产级或 Exactly Once。

## 后续顺序

```text
M2.4 最大尝试次数与 DEAD
        ↓
M3 租约、过期恢复与进程退出故障注入
```
