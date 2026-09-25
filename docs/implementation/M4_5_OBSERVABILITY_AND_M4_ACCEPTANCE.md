# M4.5：Micrometer 指标、结构化日志与 M4 总验收

> 状态：已完成
>
> 基线：M4.4 已完成；Java 17、Spring Boot 3.5.16、MySQL 8.0、RocketMQ 5.x
>
> 目标：让发布结果、延迟、积压、死信和租约恢复可被观测，并用真实服务场景完成 M4 验收。

M4.4 已完成默认 Starter 的有界停机，但运行时主要依靠零散日志排查问题。M4.5 在现有条件抢占、短事务、发送分类和租约恢复协议上增加观测，不改变至少一次投递语义。指标名称沿用[项目方向文档](../PROJECT_DIRECTION.md#16-可观测性)；本文件冻结每个样本的计入时点、数据来源和失败边界。

实施结果与回归数据见 [M4.5 阶段完成记录](../progress/M4_5_COMPLETED.md)。

```text
M4.4 有界优雅停机
  → M4.5 指标、结构化日志与 M4 总验收
  → M5 原创示例、私有接入与基准测试
```

## 本阶段冻结的语义

1. `publish.success` 和 `publish.failure` 统计 **Sender 单次调用结果**，不是唯一事件数或消费者处理数。结果未知计作一次发送失败，Broker 实际接收后重试可能再计一次成功。不能由 `dispatchOnce().submittedCount()` 推断发送成功。
2. 发送耗时只覆盖一次 `EventSender.send(...)` 调用；失败和结果未知也产生耗时样本。数据库扫描、排队、抢占和状态更新不包含在内。
3. 发布延迟从事件**首次计划可用时间**到一次成功回执的时间，包含本地等待及此前的失败重试。未来事件未到首次可用时间前不产生样本。首次可用时间必须独立保存，不能使用会被重试覆盖的 `next_attempt_at`，也不能用 `created_at` 代替未来事件的可用时间。
4. `backlog` 是数据库中全部 `PENDING` 与 `RETRY_WAIT` 事件数，包含尚未到期的事件；`dead` 是全部 `DEAD` 事件数。两者是数据库快照，不是本地队列长度，也不是某轮查询的候选数。
5. `lease.expired` 只在过期租约的条件恢复实际更新一行后增加。双恢复者读到同一候选时只能计一次；恢复成 `RETRY_WAIT` 或 `DEAD` 均计入。
6. 观测代码不能改变事务提交、异常分类、重试次数、租约所有权或停机截止时间。指标或日志设施故障不得把成功发送改成失败，也不得阻止租约恢复。
7. Starter 在没有 `MeterRegistry` 时照常发布；禁用 `reliable-event.enabled` 不创建本项目的指标采集和后台任务。手动 `runOnce()` 与默认自动调度共用 Worker 和恢复逻辑，发送及恢复指标口径相同。

## 指标契约

下表使用代码中的 Micrometer meter 名；具体导出系统可能改写计数器、计时器的显示后缀，验收以注册的 meter 名和样本为准。

| 名称 | 类型 | 计入时点及口径 |
| --- | --- | --- |
| `reliable_event.publish.success` | Counter | `send` 返回有效成功回执后加一；若随后的 Outbox 完成更新失败，计数仍表示发送侧成功，不能宣称事件已持久化为 `PUBLISHED` |
| `reliable_event.publish.failure` | Counter | `send` 抛出已分类或结果未知异常后加一；抢占零行、候选撤销、数据库异常不计入 |
| `reliable_event.publish.duration` | Timer | 从调用 `send` 前至返回或抛出异常，用单调时钟记录每次发送尝试；标签 `outcome=success/retryable/non_retryable/unknown` |
| `reliable_event.publish.lag` | Timer | 有效成功回执时，记录当前时间减去不可变的首次计划可用时间；时钟偏差造成负值时按零记录并输出安全诊断；结果未知、死信和未发送不产生样本 |
| `reliable_event.backlog` | Gauge | 最近一次成功数据库采样中的 `PENDING + RETRY_WAIT` 总数，包含未来事件和等待退避事件 |
| `reliable_event.dead` | Gauge | 最近一次成功数据库采样中的 `DEAD` 总数 |
| `reliable_event.lease.expired` | Counter | 一次条件恢复成功提交后加一；标签 `result=retry_wait/dead` |

固定标签只使用上表列出的有限枚举值。不得将 `eventId`、`eventKey`、`eventType`、`leaseOwner`、Message ID、异常消息或 Topic 放入 meter 标签，避免高基数和敏感信息进入指标系统。Counter 在每个应用实例本地递增；两个实例的全局事件量需要在监控系统按实例聚合。数据库 Gauge 在两个实例上会看到同一份表的快照，**不能跨实例求和**。

### 首次可用时间与存量数据

当前 Outbox 只有 `next_attempt_at` 和 `created_at`：前者在重试及租约恢复时改写，后者早于未来事件的计划可用时间。因此 M4.5 需在表中增加不可变的 `first_available_at DATETIME(3)`，登记事件时写入 `ReliableEvent.availableAt()`，重复事件键登记仍保留原值。抢占后将该值带入观测路径；正常发布不修改它。提供已有表的增量 SQL 和新建表 SQL，不能只修改 `CREATE TABLE`。

存量行的首次可用时间无法从现有列准确重建。迁移时允许存量行的 `first_available_at` 为 `NULL`；这类行仍正常发布，跳过 `publish.lag` 样本并记录一次安全的缺失字段诊断，不把 `created_at` 或当前 `next_attempt_at` 伪装为首次可用时间。新插入行必须非空，测试覆盖未来事件、重试、租约恢复和重复登记后值保持不变。后续若要清理空值，应单独制定数据修复策略。

### 数据库 Gauge 的采样

Gauge 读取内存中的最后一次成功快照，不在每次监控抓取时直接查询 MySQL。默认自动调度可在既有扫描轮次中以有界频率采样一次状态聚合查询，不增加第二个常驻扫描循环；采样失败保留上次值，记录安全的错误类型，下一轮重试，不能中断发布轮次。启动到首次成功采样前用 `NaN` 表示未知，不能报告虚假的零。停机后停止更新并释放 meter 注册，避免 Context 重建留下旧 Gauge。

手动 `runOnce()` 路径不创建自动采样线程；显式轮次结束时可更新同一快照。SQL 只做 `PENDING`、`RETRY_WAIT`、`DEAD` 的状态聚合，不读取 Payload、Headers 或逐行加载事件。文档说明大表上的采样成本及采样间隔，不把瞬时精确性作为承诺。

## 结构化日志契约

使用稳定的事件名和键值字段，让文本日志及支持结构化输出的日志后端均可检索。事件级日志至少包含 `eventId`、`eventType`、`eventKey`、`attemptCount`、`leaseOwner`；成功回执日志另含 `messageId`。字段取自当前已抢占事件和 `SendReceipt`，不从 Payload 或 Header 反推。建议事件名：`reliable_event.publish.succeeded`、`reliable_event.publish.failed`、`reliable_event.lease.recovered`、`reliable_event.publication.state_update_failed`、`reliable_event.scheduler.failed`、`reliable_event.shutdown.timed_out`。

发送失败日志附加有限枚举 `failureType` 和目标状态 `RETRY_WAIT/DEAD`；结果未知必须明确标记 `UNKNOWN`，不可写成“Broker 未接收”。租约恢复日志只在条件更新成功时输出最终状态。状态更新失败须单独记录，不能输出“发布完成”；成功回执日志可以描述“Broker 回执成功”，与 Outbox 已持久化 `PUBLISHED` 的日志分开。扫描失败和停机超时保留其现有时序，不为格式统一而增加阻塞操作。

不记录 Payload、Header 内容、访问密钥、完整异常消息或数据库连接串。异常只输出受控分类和异常类名；`eventKey` 作为项目方向要求的定位字段，接入方不得用它承载凭据或个人敏感信息。日志记录失败不得改变原有发送与数据库结果。

## 与现有代码的接缝

1. 在 `reliable-event-core` 或 `reliable-event-jdbc` 的内部边界定义极小的观测回调及无操作实现，使 JDBC 状态机不强制依赖 Micrometer。默认 Starter 在存在 `MeterRegistry` 时注入 Micrometer 适配；自定义 `EventSender` 仍使用同一 Worker 观测点。
2. `JdbcEventPublicationWorker` 在 `send` 调用边界记录耗时、回执或分类失败；保留 `SendReceipt.messageId()` 用于成功日志。数据库完成更新独立记录结果，避免把 Broker 回执与 `PUBLISHED` 混为一谈。
3. `JdbcExpiredLeaseRecovery` 在条件恢复成功后记录计数及日志；零行竞争不计数。为死信和重试分类复用当前逻辑，不新增第二套状态机。
4. `JdbcOutboxRepository` 增加首次可用时间字段的写入/读取和状态聚合查询；提供增量迁移 SQL。观测查询不参与发送事务，不持有调度器生命周期锁。
5. `ReliableEventScheduler` 只负责触发有界频率的 Gauge 采样并保留现有停机语义。`ReliableEventPublicationAutoConfiguration` 做可选指标适配的条件装配；`enabled=false`、`scheduling-enabled=false` 和用户自定义 Bean 的规则保持清晰。

## 实施顺序

1. 先补首次可用时间的新表与存量表 SQL、实体读取和迁移测试；固定 `publish.lag` 口径。
2. 加入内部观测回调和 Micrometer 适配，覆盖发送成功、三类失败、耗时、延迟及条件租约恢复。用 `SimpleMeterRegistry` 验证每种结果恰好计一次。
3. 增加状态聚合采样和 Gauge 生命周期，验证采样失败不影响发布及多实例不重复求和的文档说明。
4. 补齐结构化日志字段和敏感信息测试，检查成功回执、结果未知、状态更新失败、恢复和停机超时。
5. 用真实 MySQL 8.0.36 与 RocketMQ 5.5.0 完成下述 M4 总验收；执行 `mvn clean verify`，在独立的 M4.5 完成记录中填写环境、测试数和实际结果。验收前不把 README、交接或简历改成“已完成 M4”。

## M4 总验收

### 指标与日志测试

- 成功回执、可重试失败、不可重试失败、结果未知各发送一次：成功/失败 Counter、Timer 数量和 `outcome` 一致；结果未知后重投产生两次尝试，不能误报一次唯一事件。
- 未来事件首次到期并经历失败重试后成功：`publish.lag` 仍以原始 `first_available_at` 为起点；存量空值行不产生错误样本。
- 双恢复者竞争同一过期租约：只有真正更新行的一方增加 `lease.expired`；恢复至 `DEAD` 和 `RETRY_WAIT` 各有对应结果。
- Gauge 包含未到期的 `PENDING/RETRY_WAIT` 与 `DEAD`，排除 `PUBLISHING/PUBLISHED`；查询失败、首次未采样和 Context 关闭按上述契约处理。
- 日志能按事件键定位一次发送、失败分类和成功 Message ID；故意放入 Payload、Header 与密钥的哨兵值，确认日志和指标均不出现这些值。
- 无 `MeterRegistry`、禁用 Starter、关闭自动调度及自定义 Sender 的 Context 测试均通过；观测异常不改变原状态更新和停机结果。

### 真实服务回归

- 用真实 Broker 验证 Topic/Tag/Key/Body/属性映射与稳定事件 ID；发送成功后 Outbox 为 `PUBLISHED`，指标和日志能对应到该回执。
- 暂停 Broker/Proxy 并恢复：事件进入 `RETRY_WAIT` 后重新发布；发送失败与成功计数、耗时样本和日志顺序符合实际尝试。
- 在成功发送后、状态更新前制造结果未知或进程退出：新实例租约恢复并可能产生重复消息；指标不得宣称 Exactly Once。
- 两实例共用 MySQL 验证条件抢占、过期租约接管、本地容量及排队不占租约；正常关闭与超时关闭仍保持 M4.4 的回调、Producer 关闭顺序和恢复边界。

测试使用可控时钟、同步器、真实数据库状态和有截止时间的等待断言，不根据固定 `sleep` 或日志文本猜测异步顺序。M0 至 M4.4 的既有测试全部回归。M4 验收完成后可表述为“Starter 具备自动调度、有界并发、有界停机和基础可观测性”；仍不承诺 Exactly Once、消费者幂等、租约续期、人工死信重放、业务接入或性能数字，这些按既定 M5 与后续范围处理。

## 参考

- [项目方向文档](../PROJECT_DIRECTION.md)
- [M4.3 常驻调度与有界并发](M4_3_SCHEDULING_AND_BOUNDED_CONCURRENCY.md)
- [M4.4 有界优雅停机](M4_4_GRACEFUL_SHUTDOWN.md)
- [当前交接](../HANDOFF.md)
