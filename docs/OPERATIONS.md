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
| 没有 Outbox 表 | 执行[正式建表 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)，创建 Outbox 和身份表，并核对两表的 `uk_event_identity`、发布/恢复/死信扫描索引及 `idx_published_retention`。 |
| 已有 M4.4 表、缺少 `first_available_at` | 先备份并确认表版本，再依次**执行一次**[M4.5 增量 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m4-5.sql)和[M6.1 增量 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m6-1.sql)。 |
| 已有 M4.5 表、缺少 `idx_dead_list` | 使用 M6.1 查询前，先备份并**执行一次**[M6.1 增量 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m6-1.sql)。 |
| 来源或结构不明的旧表 | 对照正式 SQL 逐列、逐索引核对，先制定迁移方案；不能仅因 `CREATE TABLE IF NOT EXISTS` 成功就认为旧表已升级。 |

Starter 不会自动建表或运行迁移。迁移前确认目标库、备份、应用停发窗口及数据库权限；迁移后用 `SHOW CREATE TABLE reliable_event_outbox` 核对。存量 M4.4 行的 `first_available_at` 无法可靠回填，允许为 `NULL`：这些事件继续发布，但不产生 `reliable_event.publish.lag` 样本。新登记行会写入该列。不要用当前 `next_attempt_at` 或 `created_at` 伪造历史首次可用时间。

M7 写入协议要求 `reliable_event_identity` 与 Outbox 同库、同一事务。新库使用正式建表 SQL 创建两表。已有库按以下顺序升级：

1. 保持 `published-retention-enabled=false`，备份 Outbox、身份与重放审计相关数据，暂停所有事件登记入口并等待在途业务事务结束。旧实例仍可完成发送，但不得继续登记。
2. 如旧表缺少 M4.5/M6.1 字段或索引，先按适用版本执行相应增量 SQL。执行一次[身份表创建与全量回填 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-identity-m7-1.sql)，再运行[一致性核对 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-identity-m7-1-check.sql)。三个异常计数在首次迁移时均须为零；核对最大 ID 与身份表的 `AUTO_INCREMENT` 起点。回填包含全部状态，不只包含 `PUBLISHED`。
3. 执行一次[M7 清理扫描索引 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m7-2.sql)。保持登记暂停，直到所有实例都运行新的身份表登记协议，再恢复入口。验证新登记、重复登记、回滚及双表一致性后，才单独考虑打开清理开关。

身份表不可用时新发布器会失败，不能降级为旧协议。清理开始后，关闭清理开关即可暂停删除，但不能直接回滚到旧版发布器；旧版无法识别已清理行的身份。恢复备份时应让 Outbox、身份及重放审计位于相容的数据时间点。`DATETIME(3)` 按 UTC 解释，数据源应使用 UTC 连接时区；M7 的成功发布时刻按 UTC 写入。

开启清理前还须抽查存量 `published_at` 的时区口径。旧版使用应用时钟和 JDBC `Timestamp` 写入；若曾以非 UTC 连接时区运行，先确定历史数据代表的真实时刻并制定修正方案，不能把本地墙上时间直接当成 UTC 清理截止时间。

使用 M6.2 JDBC 重放前，还须在同一业务库**执行一次**[重放审计表 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-replay-audit-m6-2.sql)。新建 Outbox 表或执行 M6.1 增量 SQL 都不会创建该审计表；重放类不会自动迁移数据库。审计表保留操作者、原因和旧失败摘要，应设置独立的访问和保留策略。

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
| `dead-operations-enabled` | `false` | 显式开启 `DeadEventOperations` Java Bean；不创建 HTTP 端点，也不代替应用鉴权。 |
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

M7 清理默认关闭。确认迁移与新写入一致后，设置 `published-retention-enabled=true`，并显式指定正数 `published-retention`（例如 `30d`）。`cleanup-batch-size` 默认 100、范围 1–1000；`cleanup-interval` 默认 `1h`，至少 `1ms`。JDBC 模块也提供 `JdbcPublishedEventRetention.runOnce(retention, batchSize)` 供受控单轮执行，返回扫描数、删除数和剩余最老到期行年龄。自动清理与发布扫描独立运行，只删除 `status=PUBLISHED` 且 `published_at` 早于数据库 UTC 截止时间的行。配置变更需重启应用；停机等待复用 `shutdown-timeout`，数据库调用不响应中断时可能在超时后结束。

## 状态与只读排查

| `status` | 名称 | 含义 |
| ---: | --- | --- |
| `0` | `PENDING` | 已随业务事务提交，等待首次到期。 |
| `1` | `PUBLISHING` | Worker 已抢占并持有租约；租约过期后可由扫描恢复。 |
| `2` | `PUBLISHED` | 生产端收到有效发送回执并成功写入 Outbox 状态；不代表消费者完成。 |
| `3` | `RETRY_WAIT` | 发送失败或租约恢复后，等待 `next_attempt_at`。 |
| `4` | `DEAD` | 不可重试错误或尝试耗尽；不会自动再次扫描。 |

M6.1 的只读查询可经 Starter 装配的 `DeadEventOperations` 调用；JDBC 模块仍提供 `JdbcDeadEventQuery`。`firstPage(50)` 返回事件 ID 倒序的 `DEAD` 列表及可选游标；有游标时用 `nextPage(cursor, 50)` 继续，单页上限 100。`lookup(eventId)` 区分不存在、非死信和死信详情。列表不包含业务键和失败摘要；详情包含 `event_key` 与 `last_error`，须限制访问、避免记录到普通日志。两种查询都不读取 Payload 或 Headers。分页不是一致性快照，期间状态变化后需重新查询。

M6.2 在 JDBC 模块提供 `JdbcDeadEventReplay`；M6.3 通过 Starter 的同一个 `DeadEventOperations` Bean 提供重放。只有设置 `reliable-event.dead-operations-enabled=true` 才装配该 Bean；默认发布路径不需要此配置。应用必须在调用前鉴权，将真实操作者与明确原因写入请求，并先核对 Broker 和消费者是否已处理该事件，特别是发送结果未知的情况。使用 M6.1 详情中的版本作为预期版本；`Replayed` 返回审计 ID，`NotFound`、`NotDead`、`VersionMismatch` 不会更改事件或写审计。重放只将原事件重新入队，由现有调度器发送；它不能保证只投递一次。

```java
// 应用先完成身份认证，并只对获授权的运维角色开放以下调用。
DeadEventOperations deadEvents = ...; // 注入 Starter Bean
DeadEventPage page = deadEvents.firstPage(50);
DeadEventLookup one = deadEvents.lookup(new EventId(123));
// 核对 Broker/消费者事实并修复原因后，使用详情中的当前版本：
if (one instanceof DeadEventLookup.Dead dead) {
    DeadEventReplayResult result = deadEvents.replay(new DeadEventReplayRequest(
            dead.event().id(), dead.event().version(), authenticatedOperator,
            "已核对消费记录并修复目标 Topic"));
    // 持久记录 result 中的审计 ID，并按下述 SQL 追踪发布状态。
}
```

这些能力尚未纳入此前 M5.4 的 `0.1.0` 发布检查。成功重放保留原事件身份和 `max_attempts`，将 `attempt_count` 清零开始新一轮尝试，旧失败摘要留在审计表；`first_available_at` 不变，重放后的 `publish.lag` 仍从最初可用时间计算。

人工处置应在受控工单中记录事件 ID、`eventType + eventKey`、查询版本、操作者身份、批准人与原因、Broker 消息身份、消费者去重及业务效果、修复动作、重放结果和审计 ID。应用层先完成身份认证与授权，建议让查询详情和执行重放使用分开的权限；库不内置角色或审批。若 Broker 已接收而消费者已完成业务效果，必须先确定业务上是否仍需重放，不能把 `DEAD` 当作未投递证明。操作者之间共享的版本是一次性预期值；得到 `NotDead` 或 `VersionMismatch` 时重新查询并人工核对，不以新版本自动再次重放。

重放成功仅表示重新入队。先以审计 ID 核对下面的只读 SQL，再持续查看该事件的 `status`、`attempt_count` 和发送日志：`PUBLISHED` 时继续核对消费者业务效果；`RETRY_WAIT` 时结合 `next_attempt_at` 和失败摘要排查；再次进入 `DEAD` 时保留本轮审计与发送证据，修复新失败后才发起新的人工决定。数据库异常或审计写入失败会使重放事务回滚，应先排查数据库与审计表，不手工改 Outbox 状态。`PUBLISHING` 只表示有 Worker 持有租约，需等待其完成或按现有租约恢复规则处理。

可用返回的审计 ID 只读核对本次操作和当前发布状态，不在普通日志中输出审计表内的原因或旧失败摘要：

```sql
SELECT a.id, a.event_id, a.previous_version, a.new_version,
       a.previous_attempt_count, a.replayed_at, a.result,
       o.status, o.version, o.attempt_count, o.published_at
FROM reliable_event_replay_audit a
LEFT JOIN reliable_event_outbox o ON o.id = a.event_id
WHERE a.id = ?;
```

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
| 出现 `DEAD` | 事件身份、尝试次数、受限访问下的 `last_error`、发送日志与业务影响 | 先定位原因并核对 Broker/消费者事实，再决定业务补偿或使用 M6.2 JDBC 单条重放。不要直接改 `status`、`version` 或租约字段。 |
| `PUBLISHED` 但下游没有效果 | RocketMQ 消费位置、消费者日志、持久化去重与业务事务 | 生产端回执不证明消费成功；由消费方按其业务恢复协议处理。 |

`last_error` 是异常类名与消息的截断摘要，**可能包含敏感异常文本**；只允许受控的排障访问，不导出到公开报告。生产日志不打印 Payload 和 Header 值，调用方也不应把凭据或个人数据放入 Header。`reliable_event.publication.state_update_failed` 表示发送后状态更新失败，不能据此判断 Broker 未接收。

## 指标、停机与恢复边界

- `reliable_event.publish.success` / `reliable_event.publish.failure` 是 Sender 单次尝试的结果计数。`reliable_event.publish.duration` 测单次发送耗时；`reliable_event.publish.lag` 从首次计划可用时间到成功回执，不包括随后状态提交或消费完成。旧行 `first_available_at IS NULL` 时不产生 lag 样本。
- `reliable_event.backlog` 是 `PENDING + RETRY_WAIT` 的数据库快照，含未来事件；`reliable_event.dead` 是 `DEAD` 快照。首次成功采样前 Gauge 为 `NaN`，多实例看到同一张表，不能将这些 Gauge 跨实例相加。`reliable_event.lease.expired` 在成功恢复过期租约后计数。
- 启用 M7 清理且应用提供 `MeterRegistry` 时，`reliable_event.retention.deleted` 计删除行数，`reliable_event.retention.duration` 计单轮时长，`reliable_event.retention.failed` 计失败轮次，`reliable_event.retention.oldest_eligible_age` 为本实例最近成功轮次后剩余最老到期行的年龄（秒）。该 Gauge 是本地采样值，多实例不应求和；未成功执行前为 `NaN`。
- 正常关闭时停止新抢占，撤销尚未抢占的排队候选，并在 `shutdown-timeout` 内等待在途发送及状态更新。该超时不限制默认 Producer 的 `close()` 或整个 Spring Context 的关闭时间。超时、进程退出或外部调用无视中断时，遗留租约由后续实例恢复，可能产生重复消息。
- 当前没有租约续期、`DEAD` 自动重放或通用消费者框架。人工重放须由应用显式调用并鉴权。`scheduling-enabled=false` 时显式 `runOnce()` 不受自动运行时的容量和停机等待管理，调用方自行协调。

## 数据增长与保留

M7 提供默认关闭的 `PUBLISHED` 行分批清理，保留期从生产端成功记录的 `published_at` 计算；它不证明消费者已完成。清理后 Outbox 中的 Payload、Headers 和状态历史不可在线查询，`reliable_event_identity` 则永久保留事件 ID、类型和业务键，以保证重复登记不重新入队。`PENDING`、`PUBLISHING`、`RETRY_WAIT`、`DEAD` 不清理；没有在线完整消息归档。开启前应确定完整消息的备份和排障窗口，监测身份表及审计表增长、索引、备份耗时和磁盘空间。M5.3 的旧基准来自 M7 写入协议之前，不能直接当作新协议的性能数字。

M6.2 的重放审计行同样会持续积累；其中的操作者、原因与旧失败摘要应按业务数据保护要求控制访问、备份和保留期限。

不要绕开 M7 的身份校验手工删除 Outbox 行，也不要删除身份表中的历史键。缺少身份或身份与 Outbox 不匹配时，自动删除会跳过该行；应关闭清理、调查并修复一致性。清理开关关闭后，新写入仍必须使用身份表协议。审计查询使用 `LEFT JOIN`；Outbox 行已清理时审计仍在，不能把联查中的空 Outbox 字段误判为审计丢失。数据库物理空间回收由运维另行安排。

## 相关文档

- [仓库 README](../README.md)与[原创订单示例](../reliable-event-example/README.md)
- [M4.3 调度与有界并发](implementation/M4_3_SCHEDULING_AND_BOUNDED_CONCURRENCY.md)
- [M4.4 优雅停机](implementation/M4_4_GRACEFUL_SHUTDOWN.md)
- [M4.5 指标口径](implementation/M4_5_OBSERVABILITY_AND_M4_ACCEPTANCE.md)
- [M5.3 可复现基准结果](progress/M5_3_COMPLETED.md)
