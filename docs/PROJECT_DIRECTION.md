# ReliableEvent 项目方向文档

> 状态：`0.1.0` 范围已更新；2026-09-26 取消 M5.2 私有优惠券链路验收
> 日期：2026-09-20  
> 数据库基线更正：2026-09-23，第一版由 MySQL 5.7 调整为 MySQL 8.0
> 第一目标版本：`0.1.0`

## 1. 一句话定义

ReliableEvent 是一个基于 Transactional Outbox 模式的 Spring Boot Starter：调用方在本地数据库事务中登记事件，模块在事务提交后将事件可靠地发布到 RocketMQ，并负责多实例抢占、失败重试、租约恢复、死信和可观测性。

## 2. 要解决的问题

典型业务代码同时执行数据库写入和消息发送：

```text
写入业务数据
发送 RocketMQ 消息
提交数据库事务
```

数据库事务和 RocketMQ 不共享一个原子事务，因此可能出现：

- 消息已经发送，数据库事务随后回滚；
- 数据库事务已经提交，应用在发送消息前宕机；
- 消息发送超时，但 Broker 实际已经接收；
- 消息发送成功，应用在记录成功状态前宕机；
- 多个应用实例同时扫描并发布同一个事件。

ReliableEvent 将业务数据和 Outbox 事件写入同一个本地事务。事件发布从业务请求中解耦，由可恢复的后台发布器完成。

## 3. 目标用户

- 使用 Spring Boot 3、MySQL 8.0 和 RocketMQ 的 Java 后端应用；
- 希望避免在每条业务链路重复编写消息补偿逻辑的团队；
- 接受至少一次投递，并能在消费者侧实现业务幂等的系统。

## 4. 成功标准

`0.1.0` 必须同时满足以下条件：

1. 业务事务回滚时，Outbox 事件也回滚；
2. 业务事务提交后，即使当前应用立即宕机，事件仍可由其他实例发布；
3. 多个发布实例竞争时，同一时刻最多只有一个实例获得事件租约；
4. RocketMQ 不可用时按照指数退避策略重试；
5. 发布实例宕机后，租约过期事件可以恢复；
6. 达到最大尝试次数后进入死信状态；
7. 能观察积压数量、发布延迟、成功数、失败数和死信数；
8. 有自动化测试复现上述行为，而不只是在文档中声称支持；
9. 原创示例能从空环境运行，验证业务事务、真实 Broker 发布与消费者按稳定身份去重；
10. 所有性能数字都能通过仓库内脚本复现。

## 5. 明确不做

第一版不包含：

- 自研 Broker、网络协议或消息存储文件；
- Topic、Partition、消费者组或消费位点管理；
- RocketMQ 的替代实现；
- Exactly Once 承诺；
- 全局事务或分布式事务协调器；
- Kafka、RabbitMQ 等第二种生产适配；
- 多数据源事务；
- 跨数据库 Outbox 表；
- 消息顺序保证；
- 管理后台；
- DAG 工作流和分布式计算；
- 复杂权限系统；
- 通过 CDC 或 Binlog 发布事件；
- 自动修改业务消费者代码。

以上能力只有在 `0.1.0` 完成并有真实需求后才重新评估。

## 6. 核心语义

### 6.1 原子性

默认要求 `publish` 在活动的 Spring 数据库事务中调用。事件写入使用调用方的同一个 `DataSource` 和事务连接。

如果没有活动事务，默认快速失败并抛出明确异常，防止调用方误以为业务数据和事件具备原子性。未来可以增加显式的非事务发布方法，但不属于 `0.1.0`。

### 6.2 投递语义

模块提供至少一次投递，不提供 Exactly Once。

下列窗口无法仅靠生产端消除：

```text
RocketMQ 已接收消息
        ↓
发布进程在更新 Outbox 状态前宕机
        ↓
租约过期后事件被再次发送
```

因此每条事件必须有稳定的事件键，消费者必须根据该键实现幂等。

### 6.3 可用时间

事件包含 `availableAt`：

- 当前时间：尽快发布；
- 未来时间：到期后才允许发布；
- 重试时：更新为下一次尝试时间。

延迟精度受轮询间隔、数据库负载和线程池排队影响。它不是硬实时调度器。

### 6.4 顺序

第一版不保证同一业务键或同一 Topic 的严格发布顺序。调用方不得依赖发布顺序表达业务正确性。

## 7. 模块与接缝

### 7.1 外部模块

调用方只面对一个深模块：可靠事件发布模块。

它的接口保持很小，事务参与、序列化、持久化、抢占、重试和指标全部隐藏在实现内部。

建议接口：

```java
public interface ReliableEventPublisher {

    EventId publish(ReliableEvent<?> event);
}

public record ReliableEvent<T>(
        String eventType,
        String eventKey,
        T payload,
        Instant availableAt,
        Map<String, String> headers
) {
}
```

接口约束：

- `eventType`：稳定的事件类型，用于映射 RocketMQ Topic 和 Tag；
- `eventKey`：一次业务事件的稳定唯一键，用于防止重复登记；
- `payload`：必须能被配置的 JSON 序列化器处理；
- `availableAt`：不能为空，立即事件使用当前时间；
- `headers`：仅允许字符串键值，不得包含凭据等敏感信息；
- 调用时必须存在活动事务；
- 同一 `eventType + eventKey` 重复发布时返回已有事件，不重复插入。

### 7.2 内部发送接缝

后台发布器通过内部发送接口与消息系统交互：

```java
interface EventSender {

    SendReceipt send(StoredEvent event);
}
```

`0.1.0` 存在两个适配：

- `RocketMqEventSender`：生产适配；
- `FakeEventSender`：测试适配。

发送接缝不暴露给普通业务调用方。

### 7.3 存储接缝

`0.1.0` 只支持 MySQL 8.0，因此不公开通用存储插件接口。存储实现作为模块内部实现存在，避免只有一个生产适配却提前设计抽象。

## 8. 状态机

```text
PENDING ───────→ PUBLISHING ───────→ PUBLISHED
                    │
                    ├──────────────→ RETRY_WAIT ─────→ PUBLISHING
                    │
                    └──────────────→ DEAD

租约过期：PUBLISHING → RETRY_WAIT
达到最大次数：PUBLISHING / RETRY_WAIT → DEAD
```

状态含义：

- `PENDING`：已随业务事务提交，等待首次发布；
- `PUBLISHING`：某个发布实例持有有效租约；
- `PUBLISHED`：RocketMQ发送调用明确返回成功；
- `RETRY_WAIT`：上次尝试失败，等待下一次重试；
- `DEAD`：达到最大尝试次数，需要人工或显式接口处理。

`PUBLISHED` 只表示生产端收到发送成功结果，不表示消费者完成业务处理。

## 9. 数据模型

第一版使用单表：

```sql
CREATE TABLE reliable_event_outbox (
    id                BIGINT NOT NULL AUTO_INCREMENT,
    event_type        VARCHAR(128) NOT NULL,
    event_key         VARCHAR(192) NOT NULL,
    payload           JSON NOT NULL,
    headers           JSON NULL,
    status            TINYINT NOT NULL,
    next_attempt_at   DATETIME(3) NOT NULL,
    first_available_at DATETIME(3) NULL,
    attempt_count     INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL,
    lease_owner       VARCHAR(128) NULL,
    lease_until       DATETIME(3) NULL,
    version           BIGINT NOT NULL DEFAULT 0,
    last_error        VARCHAR(1024) NULL,
    created_at        DATETIME(3) NOT NULL,
    updated_at        DATETIME(3) NOT NULL,
    published_at      DATETIME(3) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_identity (event_type, event_key),
    KEY idx_publish_scan (status, next_attempt_at, id),
    KEY idx_lease_recovery (status, lease_until, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

设计决定：

- `event_type + event_key` 是幂等登记键；
- `next_attempt_at` 同时表达首次可用时间和下一次重试时间，避免扫描条件出现复杂 OR；
- `lease_owner + lease_until` 用于宕机恢复；
- `version` 用于 MySQL 8.0 条件更新抢占；
- `last_error` 保存截断后的异常类名与消息；异常消息仍可能包含敏感文本，运维访问需受控；
- `0.1.0` 不提供自动归档或删除。部署方需监测表增长；删除已发布行会失去该事件键的持久去重记录，后续清理方案必须先解决身份保留与备份问题。

## 10. MySQL 8.0 抢占策略

MySQL 8.0 支持 `SKIP LOCKED`，但 `0.1.0` 仍保留已经实现和验证的候选版本条件更新方案。发布器先按索引分页获取候选 ID，再逐条或小批量执行条件更新：

```sql
UPDATE reliable_event_outbox
SET status = 1,
    lease_owner = ?,
    lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)),
    attempt_count = attempt_count + 1,
    version = version + 1,
    updated_at = ?
WHERE id = ?
  AND status IN (0, 3)
  AND next_attempt_at <= ?
  AND attempt_count < max_attempts
  AND version = ?;
```

受影响行数为 `1` 才算抢占成功。查询候选不等于获得执行权。

保留该方案是当前阶段的明确选择，而不是受数据库版本限制：它已经通过双 Worker 真实并发测试，能够用版本令牌保护后续状态更新，也不要求发送期间持有数据库事务。是否增加基于 `SKIP LOCKED` 的替代策略，必须先有相同环境下的吞吐、锁等待和故障语义对比，不在数据库版本更正时同时改写并发协议。

第一版先保证正确性，再用批量大小、轮询间隔和线程池容量优化吞吐量。

## 11. 发布流程

```text
1. 查询到期候选事件
2. 使用条件更新竞争租约
3. 将抢占成功的事件提交到有界线程池
4. RocketMQ适配发送消息
5. 成功：按 id + leaseOwner + version 条件更新为 PUBLISHED
6. 失败：计算退避时间，更新为 RETRY_WAIT
7. 超过最大次数：更新为 DEAD
```

线程池必须有界。发布器每轮可抢占数量不能长期大于本地可执行容量，避免事件已经获得租约却在本地队列中长时间等待。

## 12. 重试策略

默认策略为带随机抖动的指数退避：

```text
delay = min(initialDelay × 2^(attempt-1), maxDelay) + jitter
```

默认建议：

- `maxAttempts = 8`
- `initialDelay = 1s`
- `maxDelay = 5min`
- `jitter = 0% ~ 20%`

可重试：

- Broker暂时不可用；
- 网络超时；
- 连接失败；
- 服务端可恢复错误。

不可重试：

- 事件类型没有目标映射；
- 消息属性或 Body 在发送适配阶段不合法；
- Topic配置非法；
- 超过Broker允许的消息大小。

不可重试错误直接进入 `DEAD`。

## 13. 租约与宕机恢复

租约必须大于有效的 RocketMQ 单次请求超时，并为发送和状态更新留出余量。排队候选尚未抢占租约。状态更新必须校验租约所有者，避免旧 Worker 在租约失效后覆盖新 Worker 的结果。Payload 序列化失败发生在事务内登记阶段，不会生成一条 `DEAD` Outbox 行。

恢复线程扫描：

```text
status = PUBLISHING
AND lease_until <= UTC_TIMESTAMP(3)
```

将其转为 `RETRY_WAIT`，并记录租约过期原因。

租约恢复可能造成重复消息，这是至少一次语义的一部分，不通过隐藏异常来假装 Exactly Once。

## 14. Spring Boot 自动配置

配置前缀固定为：

```yaml
reliable-event:
  enabled: true
  scheduling-enabled: true
  poll-interval: 1s
  claim-batch-size: 50
  recovery-batch-size: 50
  worker-threads: 8
  worker-queue-capacity: 200
  shutdown-timeout: 20s
  lease-duration: 30s
  max-attempts: 8
  initial-retry-delay: 1s
  max-retry-delay: 5m
  rocketmq:
    endpoints: localhost:8081
    request-timeout: 5s
    mappings:
      order-created:
        destination: orders-topic:created
```

`orders-topic` 是示意值，部署时须创建相应 Topic。活动事务是 `publish` 的固定要求；`0.1.0` 没有 `require-active-transaction` 或 `published-retention` 配置。当前全部配置及默认值见[接入与运维指南](OPERATIONS.md)。

自动配置必须满足：

- 用户自定义同类型 Bean 时默认实现退让；
- 缺少 DataSource 或 RocketMQ依赖时给出明确条件报告；
- 配置非法时启动失败，不在运行中静默降级；
- 应用关闭时停止抢占新事件，并在上限时间内等待在途发送。

## 15. 仓库模块

计划结构：

```text
reliable-event/
├── reliable-event-core
├── reliable-event-jdbc
├── reliable-event-rocketmq
├── reliable-event-spring-boot-autoconfigure
├── reliable-event-spring-boot-starter
├── reliable-event-testkit
├── reliable-event-example
├── benchmark
└── docs
```

职责：

- `core`：公开事件模型、发布接口和状态语义；
- `jdbc`：Outbox写入、扫描、抢占、状态流转和清理；
- `rocketmq`：RocketMQ发送适配和目标映射；
- `autoconfigure`：Spring Boot条件装配及配置校验；
- `starter`：依赖聚合，不包含业务逻辑；
- `testkit`：Fake发送适配、等待断言和测试夹具；
- `example`：完全原创的最小示例，不复制优惠券项目代码；
- `benchmark`：数据构造、压测脚本和报告模板。

编码初期允许先在较少模块中完成闭环，行为稳定后再按上述结构拆分。不得为了目录漂亮而提前制造大量空模块。

## 16. 可观测性

必须提供以下 Micrometer 指标：

- `reliable_event.publish.success`：发布成功计数；
- `reliable_event.publish.failure`：发布失败计数；
- `reliable_event.publish.duration`：发送耗时；
- `reliable_event.publish.lag`：当前时间与首次可用时间的差值；
- `reliable_event.backlog`：待发布和等待重试数量；
- `reliable_event.dead`：死信数量；
- `reliable_event.lease.expired`：租约过期恢复次数。

日志必须包含：

- eventId；
- eventType；
- eventKey；
- attemptCount；
- leaseOwner；
- RocketMQ消息ID（成功时）。

Payload默认不完整打印，避免泄漏手机号、邮箱等敏感信息。

## 17. 测试策略

### 17.1 单元测试

- 状态机合法和非法转换；
- 退避时间计算；
- 错误分类；
- Payload序列化；
- 配置校验。

### 17.2 MySQL集成测试

使用 Testcontainers 启动 MySQL 8.0：

- 事务提交后事件存在；
- 事务回滚后事件不存在；
- 重复事件键不会产生两条记录；
- 两个发布实例竞争同一事件，只有一个抢占成功；
- 租约过期后能够恢复；
- 状态更新必须匹配租约所有者和版本；
- 达到最大次数进入死信；
- 清理任务不会删除未发布事件。

### 17.3 RocketMQ集成测试

- 正常发送并记录消息ID；
- Broker不可用触发重试；
- 发送超时结果不确定时允许重复；
- Topic映射缺失进入不可重试失败；
- 延迟到期前不发送。

### 17.4 故障注入

必须提供可重复执行的脚本或测试：

1. 抢占后、发送前杀死发布实例；
2. 发送成功后、更新数据库前杀死发布实例；
3. 运行中停止 RocketMQ，再恢复；
4. 同时启动两个发布实例；
5. 构造持续失败事件观察退避和死信。

## 18. 基准测试

基准测试至少记录：

- 硬件、JDK、MySQL和RocketMQ版本；
- 总事件数；
- 发布实例数；
- 线程数、批量大小和轮询间隔；
- 吞吐量；
- P50、P95、P99发布延迟；
- 数据库CPU、连接数和锁等待；
- 失败率和重复率；
- Outbox表在1万、10万、100万记录下的扫描执行计划。

禁止在没有报告和复现命令的情况下使用“高性能”“生产级”等表述。

## 19. 已取消的优惠券项目落地计划（历史记录）

2026-09-26 决定取消 M5.2 私有优惠券链路的后续实施与验收。以下是原计划，仅供追溯，不属于 `0.1.0` 范围或发布门槛。此前私有项目中已写入的接入代码与聚焦测试不等于完整业务验收；本仓库不宣称该链路已落地，也不对私有项目执行迁移或灰度。

### 第一阶段：创建发券任务

业务事务同时写入：

```text
t_coupon_task
reliable_event_outbox
```

事件类型：`coupon-task-execute`  
事件键：优惠券任务ID  
可用时间：立即任务使用当前时间，定时任务使用 `send_time`

发布器到期后发送现有 RocketMQ消息，分发服务继续负责实际发券。ReliableEvent不替代 RocketMQ。

### 第二阶段：预约提醒

创建预约记录时，同时登记未来可用的提醒事件。发布失败由Outbox重试，不再把数据库写入和MQ发送作为两个独立成功条件。

### 第三阶段：用户券过期事件

仅在前两个场景稳定后评估。需要先验证事件规模和Outbox表增长，不默认按每张用户券创建一条Outbox事件。

## 20. 与现有方案的关系

### RocketMQ事务消息

需要在文档中对比其事务回查机制与Outbox方案。`0.1.0` 选择Outbox的原因是业务数据与事件记录共享本地数据库事务，状态可查询、可补偿、易于故障注入验证。这不是对所有系统都更优的结论。

### `@TransactionalEventListener(AFTER_COMMIT)`

进程在事务提交后、监听器执行前宕机会丢失事件，不能单独提供持久化保证。

### XXL-Job

XXL-Job适合集中式任务调度。ReliableEvent解决的是业务事务与消息发布之间的一致性，不负责通用任务编排。

### db-scheduler / JobRunr

这些项目解决通用后台任务和调度问题。ReliableEvent只聚焦于事务型消息发布，并保留RocketMQ作为跨服务传输系统。

## 21. 里程碑

### M0：设计和反馈环

- Maven父工程；
- MySQL 8.0 Testcontainers；
- 最小建表迁移；
- 一条能够验证“事务回滚无事件”的测试。

### M1：最小闭环

- `publish` 接口；
- JSON序列化；
- Outbox写入；
- 单实例轮询；
- Fake发送适配；
- 成功状态更新。

### M2：可靠性

- MySQL条件更新抢占；
- 多实例测试；
- 指数退避；
- 最大尝试次数；
- 死信。

### M3：宕机恢复

- 租约；
- 租约所有权校验；
- 过期租约恢复；
- 两个故障注入场景。

### M4：RocketMQ与Starter

- RocketMQ发送适配；
- Topic/Tag映射；
- Spring Boot自动配置；
- 配置校验；
- 优雅停机；
- Micrometer指标。

### M5：落地与证明

- 原创示例应用；
- M5.2 私有优惠券接入已取消；
- 基准测试；
- 架构、故障语义和使用文档；
- `0.1.0` 发布检查。

## 22. `0.1.0` 完成定义

只有以下条件全部满足，才可以对外称为 `0.1.0`：

- 所有成功标准均有对应自动化测试或复现脚本；
- `mvn verify` 在干净环境通过；
- MySQL 8.0和RocketMQ版本明确固定；
- README能够在15分钟内指导用户运行示例；
- 至少一次语义和重复消息窗口写入文档；
- 公开示例只使用原创代码，不包含私有优惠券项目代码；
- 不使用未经验证的性能宣传；
- 原创示例从空环境完成事务登记、真实 Broker 发布和消费幂等验证。

## 23. 简历表述边界

可以表述：

> 设计并实现基于 Transactional Outbox 的 RocketMQ 可靠消息 Spring Boot Starter，使业务数据与待发布事件在同一本地事务中提交；基于 MySQL 8.0 条件更新和任务租约实现多实例事件抢占、失败重试及宕机恢复，并以原创订单示例验证真实 Broker 发布与消费幂等。

不能表述：

- 企业级生产验证；
- 百万QPS；
- Exactly Once；
- 完全杜绝重复消息；
- 自研消息队列；
- 优惠券项目从零自研；
- 未经测试支持所有数据库或所有MQ。

## 24. 暂缓决定

M5.4 已确定 Maven `groupId` 为 `dev.reliableevent`，拟发布父 POM 与五个库模块；源码许可证为 [Apache-2.0](../LICENSE)。公开工件仓库和正式版本尚未确定。

以下事项不阻塞M0，在真正需要时决定：

- 数据库迁移使用 Flyway 还是仅提供SQL；
- 后续版本的已发布事件采用删除还是归档，以及如何保留重复登记身份；`0.1.0` 已确定不自动清理；
- 是否在 `0.2.0` 增加人工重放接口；
- 是否在 `0.2.0` 经过基准对比后增加 `SKIP LOCKED` 替代策略。

这些暂缓项不得扩大 `0.1.0` 的范围。
