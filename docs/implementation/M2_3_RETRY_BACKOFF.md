# M2.3：失败重试与指数退避

> 状态：已完成
>
> 目标：发送器发生可重试异常时，将事件从 `PUBLISHING` 转为 `RETRY_WAIT`，计算带随机抖动的指数退避时间，并确保事件只有在下一次尝试时间到达后才能再次抢占。

## 为什么单独做这一阶段

M2.1 和 M2.2 已经证明多个 Worker 竞争同一候选事件时，只有一个 Worker 能获得执行权。但当前获胜 Worker 调用发送器后如果抛出异常，事件会停留在 `PUBLISHING`，后续扫描无法继续处理。

M2.3 只补齐普通发送失败的重试闭环：

```text
PENDING → PUBLISHING → RETRY_WAIT → PUBLISHING → PUBLISHED
```

本阶段不判断最大尝试次数，也不进入 `DEAD`。最大次数和死信属于 M2.4；进程在失败状态落库前直接退出造成的遗留 `PUBLISHING` 属于 M3。

## 完成后的失败流程

```text
短事务抢占事件
        ↓
PUBLISHING，attempt_count + 1
        ↓
事务外调用 EventSender
        ↓
捕获可重试 RuntimeException
        ↓
根据本次 attempt_count 计算退避时间
        ↓
短事务：按 id + PUBLISHING + claimVersion 更新
        ↓
RETRY_WAIT
next_attempt_at = failureTime + delay
last_error = 截断后的异常摘要
version + 1
```

Worker 处理完失败状态后继续处理批次中的其他候选，不把一次普通发送失败扩散为整轮扫描失败。`publishDueEvents()` 的返回值仍然只统计成功进入 `PUBLISHED` 的事件。

## 状态机变化

为 `EventStatus` 增加：

```text
RETRY_WAIT(3)
```

本阶段允许的状态转换为：

```text
PENDING    → PUBLISHING
RETRY_WAIT → PUBLISHING
PUBLISHING → PUBLISHED
PUBLISHING → RETRY_WAIT
```

`attempt_count` 在每次成功抢占时增加一次。发送失败状态更新不能再次增加尝试次数。

## 候选扫描和抢占

候选扫描从只查询 `PENDING` 扩展为同时查询已经到期的 `RETRY_WAIT`：

```sql
SELECT id, version
FROM reliable_event_outbox
WHERE status IN (0, 3)
  AND next_attempt_at <= ?
ORDER BY next_attempt_at, id
LIMIT ?;
```

条件抢占同样允许这两个来源状态：

```sql
UPDATE reliable_event_outbox
SET status = 1,
    attempt_count = attempt_count + 1,
    version = version + 1,
    updated_at = ?
WHERE id = ?
  AND status IN (0, 3)
  AND next_attempt_at <= ?
  AND version = ?;
```

扫描结果仍然只是候选。即使某个 `RETRY_WAIT` 事件在扫描时已经到期，抢占时也必须重新校验状态、时间和版本。

现有索引 `idx_publish_scan (status, next_attempt_at, id)` 可以继续支持该查询，本阶段不修改表结构。

## 抢占结果需要携带尝试次数

退避时间取决于本次尝试序号。抢占 SQL 已经在数据库中递增 `attempt_count`，因此抢占成功后返回的内部对象必须携带数据库中的新值：

```text
ClaimedEvent(storedEvent, claimVersion, attemptCount)
```

约束如下：

- 第一次抢占后的 `attemptCount` 为 `1`；
- 第一次失败并再次到期后，第二次抢占得到 `attemptCount = 2`；
- 不能根据内存中的循环次数推测尝试次数；
- 不能在发送失败时再次递增尝试次数；
- `attemptCount` 必须来自抢占成功后的数据库记录。

`StoredEvent` 是否增加尝试次数字段可以在实现时决定，但退避计算必须使用与本次抢占对应的持久化值。

## 指数退避规则

公式沿用项目方向文档：

```text
baseDelay = min(initialDelay × 2^(attemptCount - 1), maxDelay)
jitter = baseDelay × jitterRatio × random[0, 1)
delay = baseDelay + jitter
nextAttemptAt = failureTime + delay
```

默认参数：

- `initialDelay = 1s`；
- `maxDelay = 5min`；
- `jitterRatio = 20%`；
- `random` 的取值范围为大于等于 `0`、小于 `1`。

按照该公式，基础退避受 `maxDelay` 限制，抖动在限制后追加。因此默认情况下最终延迟最多接近 `6min`。这与方向文档中“先取最小值，再加 jitter”的定义保持一致。

### 计算约束

- `attemptCount` 必须大于等于 `1`；
- `initialDelay` 必须为正；
- `maxDelay` 不能小于 `initialDelay`；
- `jitterRatio` 必须在 `[0, 0.2]` 范围内，M2.3 不允许超过项目约定的最大 20% 抖动；
- 随机源每次返回值必须位于 `[0, 1)`，越界时快速失败；
- 乘法和指数增长必须使用饱和计算，不能因为次数过大产生整数溢出；
- 计算结果向下取整到毫秒，以匹配 MySQL `DATETIME(3)`；
- 如果时间相加溢出，必须明确失败，不能生成回绕到过去的重试时间。

## 退避组件

将时间算法放入独立的包内组件，例如：

```text
ExponentialBackoff.nextDelay(attemptCount) → Duration
```

该组件不访问数据库，也不读取系统时钟，只根据配置、尝试次数和随机源计算延迟。

为了让测试完全可重复，随机源必须可注入。可以通过包内构造器接收 `DoubleSupplier` 或语义等价的最小接缝：

```text
固定返回 0.0 → 无额外抖动
固定返回 0.5 → 追加最大抖动的一半
```

生产默认实现可以使用线程安全随机源。不要在测试中用范围断言替代确定结果，也不要通过固定全局随机种子影响其他测试。

本阶段只有一种退避算法，不需要公开通用插件接口。

## 失败状态更新

发送失败后执行短事务：

```sql
UPDATE reliable_event_outbox
SET status = 3,
    next_attempt_at = ?,
    last_error = ?,
    updated_at = ?,
    version = version + 1
WHERE id = ?
  AND status = 1
  AND version = ?;
```

其中版本参数必须是当前 `ClaimedEvent.claimVersion`。

受影响行数必须为 `1`。如果为 `0`，说明抢占令牌已经失效，必须明确报错，不能静默认为失败状态已保存。

失败更新不修改：

- `attempt_count`，因为它已经在抢占时递增；
- `published_at`，失败事件不能写入发布时间；
- `max_attempts`，它是登记事件时确定的配置值；
- 租约字段，M2.3 尚未使用租约。

## 异常捕获边界

当前 `EventSender.send(...)` 没有声明受检异常。M2.3 捕获发送器抛出的 `RuntimeException`，将其视为可重试发送失败。

不要捕获 `Error`。内存溢出、虚拟机错误等严重问题应继续向上抛出，遗留的 `PUBLISHING` 事件由 M3 的租约恢复处理。

如果将失败状态写入数据库本身失败，Worker 应抛出该持久化异常，而不是继续声称事件已经进入重试。此时事件可能停留在 `PUBLISHING`，同样属于 M3 的恢复范围。

M2.3 暂时将所有 Sender `RuntimeException` 视为可重试。不可重试错误分类和直接进入 `DEAD` 的策略在 M2.4 与死信规则一起实现。

## 错误摘要

`last_error` 只保存用于定位问题的异常摘要，不保存完整堆栈、Payload、Headers 或凭据。

建议格式：

```text
异常类名: 异常消息
```

规则：

- 异常消息为空时只保存异常类名；
- 最大长度为 `1024` 个 Unicode 字符，与表字段限制一致；
- 截断时不能切断 Unicode 代理对；
- 不递归拼接完整 cause 链；
- 不把 Payload 或 Headers 添加到错误文本；
- Sender 适配不得把访问凭据等敏感信息放入异常消息；
- Repository 接收已经整理好的字符串，不负责理解异常对象。

错误摘要逻辑应当独立测试，特别覆盖空消息、超长消息和包含补充字符的文本。

## Worker 行为

每个已抢占事件的处理逻辑调整为：

```text
try
    事务外发送
    短事务标记 PUBLISHED
    publishedCount + 1
catch RuntimeException from sender
    failureTime = clock.instant()
    delay = backoff.nextDelay(claimedEvent.attemptCount)
    nextAttemptAt = failureTime + delay
    短事务标记 RETRY_WAIT
    publishedCount 不变
    继续处理下一个候选
```

捕获范围必须只包围 Sender 调用。成功状态更新、失败状态更新或退避计算中的编程错误不能被再次当成发送失败捕获，否则可能掩盖数据库状态不一致。

事件重试成功后，`last_error` 保留最近一次失败摘要，不在 `markPublished` 时清空。该字段表达历史上的最后一次错误，而不是当前状态；`status = PUBLISHED` 才是最终结果来源。

## 预计代码改动

### `EventStatus`

- 增加 `RETRY_WAIT(3)`。

### `ClaimedEvent`

- 增加本次抢占后的 `attemptCount`；
- 保留事件内容和 `claimVersion`。

### `JdbcOutboxRepository`

- 候选扫描包含到期的 `PENDING` 和 `RETRY_WAIT`；
- 条件抢占允许从两个状态进入 `PUBLISHING`；
- 抢占成功后读取并返回数据库中的 `attempt_count`；
- 增加按抢占版本更新 `RETRY_WAIT` 的方法；
- 失败更新同时写入 `next_attempt_at`、`last_error`、`updated_at` 并递增版本。

### `JdbcEventPublicationWorker`

- 注入退避组件；
- 只捕获 Sender 抛出的 `RuntimeException`；
- 失败后计算下一次尝试时间并持久化；
- 单个事件失败后继续处理同批次其他候选；
- 返回值仍然只统计发布成功数量。

### 新增内部组件

- 指数退避计算组件；
- 异常摘要生成逻辑；
- 这些类型保持包内可见，不扩大公开 API。

## 单元测试清单

指数退避组件至少覆盖：

1. 第一次尝试的基础延迟为 `initialDelay`；
2. 第二次和后续尝试按二次幂增长；
3. 基础延迟达到 `maxDelay` 后不再增长；
4. 随机值为 `0.0` 时不增加抖动；
5. 随机值为 `0.5` 时增加最大抖动的一半；
6. 结果精确到毫秒；
7. 极大尝试次数不会发生数值溢出；
8. 非法尝试次数和非法配置会快速失败。

错误摘要至少覆盖：

- 普通类名和消息；
- `null` 或空消息；
- 超过 1024 个字符时正确截断；
- 截断位置附近存在补充字符时不会生成非法 Unicode。

## MySQL 集成测试清单

至少增加以下场景：

### 发送失败进入等待

- 登记一条到期事件；
- Fake Sender 第一次调用抛出固定异常；
- Worker 返回发布成功数量 `0`；
- 状态变为 `RETRY_WAIT`；
- `attempt_count = 1`；
- `version = 2`；
- `published_at` 为空；
- `last_error` 为预期摘要；
- `next_attempt_at` 等于失败时间加确定的退避时间。

### 未到时间不能重试

- 在 `next_attempt_at` 之前再次运行 Worker；
- Sender 调用次数不增加；
- 状态、尝试次数和版本保持不变。

### 到期后再次抢占并成功

- 将 Worker 时钟推进到 `next_attempt_at`；
- 再次运行 Worker，Fake Sender 返回成功；
- 最终状态为 `PUBLISHED`；
- Sender 总调用次数为 `2`；
- `attempt_count = 2`；
- 最终 `version = 4`，即两次抢占和两次完成状态各递增一次；
- `published_at` 不为空。

### 一个失败不阻塞同批次其他事件

- 同一批次准备至少两个到期事件；
- 第一个事件发送失败，第二个发送成功；
- Worker 返回成功数量 `1`；
- 第一个事件为 `RETRY_WAIT`；
- 第二个事件为 `PUBLISHED`。

测试应使用可控 `Clock` 和固定随机源，不依赖真实等待。

## 完成标准

- `RETRY_WAIT(3)` 已进入状态模型；
- 到期的 `RETRY_WAIT` 事件能够再次参与扫描和条件抢占；
- Sender 的普通运行时异常会被保存为重试等待，而不是遗留在 `PUBLISHING`；
- 退避算法符合方向文档公式并包含 `0%～20%` 随机抖动能力；
- 尝试次数只在成功抢占时递增；
- 失败状态更新校验抢占版本；
- 未到 `next_attempt_at` 的事件不能再次发送；
- 一个事件发送失败不会阻断同批次其他候选；
- M0、M1、M2.1、M2.2 的测试继续通过；
- `mvn clean verify` 在 Java 17 和 MySQL 8.0 下通过。

## 本阶段明确不做

- 根据 `max_attempts` 进入 `DEAD`；
- 不可重试异常分类；
- 人工重放死信；
- `lease_owner`、`lease_until` 和过期租约恢复；
- RocketMQ 错误码映射；
- Worker 内部并行线程池；
- 后台定时调度；
- Spring Boot 自动配置和指标。

## 阶段性边界

M2.3 完成后，Sender 抛出的普通异常可以进入延迟重试。但以下窗口仍然存在：

```text
事件已抢占为 PUBLISHING
        ↓
进程在发送期间退出，或在失败状态落库前退出
        ↓
事件停留在 PUBLISHING
```

该窗口必须由 M3 的租约过期恢复解决，不能通过 M2.3 的异常捕获假装已经覆盖进程崩溃。

达到 `max_attempts` 后继续失败的临时行为也尚未封闭；M2.4 必须增加最大次数判断和 `DEAD` 状态，M2 完成前不能发布为完整可靠性版本。

## 后续顺序

```text
M2.3 指数退避与 RETRY_WAIT
        ↓
M2.4 最大尝试次数与 DEAD
        ↓
M3 租约与宕机恢复
```
