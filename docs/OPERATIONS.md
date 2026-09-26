# ReliableEvent 接入与运维

本文对应当前 `0.1.0-SNAPSHOT` 实现：Java 17 编译目标、Spring Boot 3、单数据源 MySQL 8.0、RocketMQ 5.x gRPC Proxy。首次接入可先运行[原创订单示例](../reliable-event-example/README.md)，再将下列配置和检查用于自己的应用。示例中的 Topic、端口和凭据仅供本地演示。

## 接入前检查

1. 应用使用一个 MySQL 8.0 `DataSource`，业务表和 `reliable_event_outbox` 位于**同一物理本地事务**。`ReliableEventPublisher.publish(...)` 必须在活动的 Spring 数据库事务内调用；没有活动事务会失败。逻辑数据源或分片路由需要用提交与回滚测试确认实际事务资源一致。
2. 为 JDBC 连接配置一致的 UTC 时间约定，并用未来 `availableAt` 事件检查数据库与应用时钟。Outbox 的时间列是 `DATETIME(3)`；租约比较使用数据库 `UTC_TIMESTAMP(3)`，时间约定不一致会影响到期判断。原创示例的 JDBC URL 使用 `connectionTimeZone=UTC`。
3. RocketMQ 5.x Proxy 地址对应用可达，并且目标 Topic 已创建。`rocketmq.endpoints` 是 gRPC Proxy 地址，不能把 NameServer 地址直接填入。事件类型必须有明确的 `topic[:tag]` 映射。
4. 消费者以稳定的事件 ID 或 `(eventType, eventKey)` 实现持久化幂等。消息发送成功与消费业务提交是两个事实；生产端可能发送重复消息。

在业务事务中调用 `publish` 时，`eventType + eventKey` 是 Outbox 唯一登记键。同一键再次登记会返回原事件 ID，原记录的 Payload、Header 和可用时间不会被覆盖。这个机制不能代替业务请求本身的幂等控制。`availableAt` 是最早允许扫描的时间，不是硬实时发送保证；第一版也不保证消息顺序。

## 建表和迁移

| 数据库现状 | 操作 |
| --- | --- |
| 没有 Outbox 表 | 执行[正式建表 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)，核对唯一键 `uk_event_identity` 和两个扫描索引。 |
| 已有 M4.4 表、缺少 `first_available_at` | 先备份并确认表版本，再**执行一次**[M4.5 增量 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m4-5.sql)。 |
| 来源或结构不明的旧表 | 对照正式 SQL 逐列、逐索引核对，先制定迁移方案；不能仅因 `CREATE TABLE IF NOT EXISTS` 成功就认为旧表已升级。 |

Starter 不会自动建表或运行迁移。迁移前确认目标库、备份、应用停发窗口及数据库权限；迁移后用 `SHOW CREATE TABLE reliable_event_outbox` 核对。存量 M4.4 行的 `first_available_at` 无法可靠回填，允许为 `NULL`：这些事件继续发布，但不产生 `reliable_event.publish.lag` 样本。新登记行会写入该列。不要用当前 `next_attempt_at` 或 `created_at` 伪造历史首次可用时间。

## 配置与启动

当前 Starter 坐标为 `dev.reliableevent:reliable-event-spring-boot-starter:0.1.0-SNAPSHOT`。应用提供 `DataSource`、`JdbcTemplate`、`PlatformTransactionManager` 和 Jackson `ObjectMapper`；默认装配要求只有一个可选的 `DataSource`。示例配置：

```yaml
reliable-event:
  rocketmq:
    endpoints: localhost:8081
    mappings:
      order-created:
        destination: orders-topic:created
```

该 Topic 名是示意值，部署前须创建相应 RocketMQ 资源。默认 Producer 启动时需要 Proxy 地址和至少一个映射；使用自定义 `Producer`、`EventSender` 或目标解析器时，应按自动配置的 Bean 条件核对自己的组合，不能假定默认配置仍生效。RocketMQ 凭据通过部署环境注入，不能提交到仓库。

| 配置键（均在 `reliable-event` 下） | 默认值 | 含义与约束 |
| --- | --- | --- |
| `enabled` | `true` | 关闭时不装配本项目的发布能力；业务代码仍调用 Publisher 时应先处理依赖关系。 |
| `scheduling-enabled` | `true` | 关闭自动扫描，保留显式 `JdbcEventPublicationCycle.runOnce()` 路径。变更配置需重启应用。 |
| `poll-interval` | `1s` | 固定延迟扫描间隔，至少 `1ms`。 |
| `claim-batch-size` / `recovery-batch-size` | `50` / `50` | 每轮到期候选提交上限 / 过期租约恢复上限，均须为正。 |
| `worker-threads` / `worker-queue-capacity` | `8` / `200` | 同时执行数 / 等待候选数；队列容量可为 `0`。排队候选尚未持有数据库租约。 |
| `lease-duration` | `30s` | 数据库租约，必须大于有效 RocketMQ 客户端 `request-timeout`；当前没有租约续期。 |
| `max-attempts` | `8` | 新事件登记时写入行内的最大抢占尝试次数；修改配置不会回写已有行。耗尽后进入 `DEAD`。 |
| `initial-retry-delay` / `max-retry-delay` | `1s` / `5m` | 指数退避基线与上限，前者不能大于后者；实际延迟在基线之上增加 `0` 至不足 `20%` 的随机抖动。 |
| `shutdown-timeout` | `20s` | 等待自动运行时在途任务的上限；应小于 `spring.lifecycle.timeout-per-shutdown-phase` 并留出 Producer 关闭时间。 |
| `rocketmq.endpoints` | 无 | 默认 Producer 的 gRPC Proxy 地址。 |
| `rocketmq.mappings.<eventType>.destination` | 无 | `topic[:tag]`；每个待发事件类型都要能解析。 |
| `rocketmq.request-timeout` | `5s` | 默认客户端单次请求超时。SDK Producer 设置单次尝试，Outbox 负责持久化重试。 |
| `rocketmq.max-body-bytes` | `4194304` | UTF-8 消息 Body 大小上限；超限属于不可重试发送错误。 |
| `rocketmq.ssl-enabled` | `false` | 是否启用客户端 SSL。 |
| `rocketmq.access-key` / `rocketmq.secret-key` | 无 | 使用静态凭据时必须同时配置；也可提供 `SessionCredentialsProvider`。 |

核心时长和批次在启动时校验。实际发送、状态更新与数据库负载也会消耗租约时间；仅满足 `lease-duration > request-timeout` 不保证任何外部调用都在租约内完成。无 `MeterRegistry` 时发布功能仍运行，但不注册本项目 Micrometer 指标。

## 状态与只读排查

| `status` | 名称 | 含义 |
| ---: | --- | --- |
| `0` | `PENDING` | 已随业务事务提交，等待首次到期。 |
| `1` | `PUBLISHING` | Worker 已抢占并持有租约；租约过期后可由扫描恢复。 |
| `2` | `PUBLISHED` | 生产端收到有效发送回执并成功写入 Outbox 状态；不代表消费者完成。 |
| `3` | `RETRY_WAIT` | 发送失败或租约恢复后，等待 `next_attempt_at`。 |
| `4` | `DEAD` | 不可重试错误或尝试耗尽；不会自动再次扫描。 |

下面的 SQL 在**目标业务库**执行，只读且不读取 Payload/Headers。先确认库名；大表上控制查询频率。`event_key` 可能是业务标识，查询结果按应用的数据访问规则处理。

```sql
-- 总体状态，未来待发事件也包含在 PENDING 中
SELECT status, COUNT(*) AS rows_count
FROM reliable_event_outbox
GROUP BY status;

-- 已到期但尚未抢占的候选；数量增长时核对自动调度和 Broker
SELECT id, event_type, event_key, status, attempt_count, max_attempts,
       next_attempt_at, updated_at
FROM reliable_event_outbox
WHERE status IN (0, 3) AND next_attempt_at <= UTC_TIMESTAMP(3)
ORDER BY next_attempt_at, id
LIMIT 50;

-- 过期租约；正常恢复会在后续扫描轮次转入 RETRY_WAIT 或 DEAD
SELECT id, event_type, event_key, attempt_count, max_attempts,
       lease_owner, lease_until, version
FROM reliable_event_outbox
WHERE status = 1 AND lease_until <= UTC_TIMESTAMP(3)
ORDER BY lease_until, id
LIMIT 50;

-- 死信；仅用于定位，不修改状态
SELECT id, event_type, event_key, attempt_count, max_attempts, updated_at
FROM reliable_event_outbox
WHERE status = 4
ORDER BY updated_at DESC, id DESC
LIMIT 50;
```

| 现象 | 先核对 | 处置方向 |
| --- | --- | --- |
| `PENDING`/`RETRY_WAIT` 持续增长 | 区分未来 `next_attempt_at` 与已到期行；确认 `enabled`、`scheduling-enabled`、实例存活、数据库与 Proxy 可达、Topic 映射、线程与批次容量 | 恢复依赖服务并观察后续轮次。容量调整应参考[M5.3 基准](progress/M5_3_COMPLETED.md)的负载边界；不手动改状态来“排空”。 |
| 同一事件反复进入 `RETRY_WAIT` | `attempt_count`、下一次时间、发送失败日志与 Broker/Proxy 健康 | 修复目标、网络或限流原因；结果未知时先核对 Broker 和消费者身份，避免把重试当成丢失。 |
| `PUBLISHING` 过期未恢复 | 是否有运行的调度实例、数据库时间与连接、`reliable_event.scheduler.failed` 日志 | 修复调度/数据库异常，再观察条件恢复；旧 Worker 可能仍在外部调用中，不要绕过版本与 Owner 栅栏。 |
| 出现 `DEAD` | 事件身份、尝试次数、受限访问下的 `last_error`、发送日志与业务影响 | 先定位原因和消费者实际效果，再决定业务补偿。当前没有人工重放 API；不要直接改 `status`、`version` 或租约字段。 |
| `PUBLISHED` 但下游没有效果 | RocketMQ 消费位置、消费者日志、持久化去重与业务事务 | 生产端回执不证明消费成功；由消费方按其业务恢复协议处理。 |

`last_error` 是异常类名与消息的截断摘要，**可能包含敏感异常文本**；只允许受控的排障访问，不导出到公开报告。生产日志不打印 Payload 和 Header 值，调用方也不应把凭据或个人数据放入 Header。`reliable_event.publication.state_update_failed` 表示发送后状态更新失败，不能据此判断 Broker 未接收。

## 指标、停机与恢复边界

- `reliable_event.publish.success` / `reliable_event.publish.failure` 是 Sender 单次尝试的结果计数。`reliable_event.publish.duration` 测单次发送耗时；`reliable_event.publish.lag` 从首次计划可用时间到成功回执，不包括随后状态提交或消费完成。旧行 `first_available_at IS NULL` 时不产生 lag 样本。
- `reliable_event.backlog` 是 `PENDING + RETRY_WAIT` 的数据库快照，含未来事件；`reliable_event.dead` 是 `DEAD` 快照。首次成功采样前 Gauge 为 `NaN`，多实例看到同一张表，不能将这些 Gauge 跨实例相加。`reliable_event.lease.expired` 在成功恢复过期租约后计数。
- 正常关闭时停止新抢占，撤销尚未抢占的排队候选，并在 `shutdown-timeout` 内等待在途发送及状态更新。该超时不限制默认 Producer 的 `close()` 或整个 Spring Context 的关闭时间。超时、进程退出或外部调用无视中断时，遗留租约由后续实例恢复，可能产生重复消息。
- 当前没有租约续期、`DEAD` 自动重放或通用消费者框架。`scheduling-enabled=false` 时显式 `runOnce()` 不受自动运行时的容量和停机等待管理，调用方自行协调。

## 数据增长与保留

`0.1.0` **不提供自动归档或删除**。`PUBLISHED` 和 `DEAD` 行会留在 Outbox 表中；部署方应监测表行数、磁盘、索引与备份耗时，并制定自己的容量和保留方案。M5.3 的 1 万、10 万、100 万历史行实验提供了特定环境下的扫描证据，不是无限增长保证。

不要直接删除 `PUBLISHED` 行来释放空间：`(event_type, event_key)` 唯一键也是重复登记的持久记录，删除后同一业务键可能再次插入并发布。任何未来归档/清理方案都必须先定义业务身份的保留期限、备份与恢复、去重连续性，以及只处理可安全清理的终态行；不得清理 `PENDING`、`PUBLISHING`、`RETRY_WAIT`，也不能无处置地删除 `DEAD`。本版本不附带清理脚本。

## 相关文档

- [仓库 README](../README.md)与[原创订单示例](../reliable-event-example/README.md)
- [M4.3 调度与有界并发](implementation/M4_3_SCHEDULING_AND_BOUNDED_CONCURRENCY.md)
- [M4.4 优雅停机](implementation/M4_4_GRACEFUL_SHUTDOWN.md)
- [M4.5 指标口径](implementation/M4_5_OBSERVABILITY_AND_M4_ACCEPTANCE.md)
- [M5.3 可复现基准结果](progress/M5_3_COMPLETED.md)
