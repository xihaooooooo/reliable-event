# M5.2：私有优惠券项目首条业务链路接入（已取消）

> 状态：2026-09-26 已取消。代码接入与聚焦测试保留为历史记录；端到端业务验收不再执行，也不属于 `0.1.0` 发布条件。实施证据见[进度记录](../progress/M5_2_IMPLEMENTATION_PROGRESS.md)。
>
> 基线：M5.1 已完成；ReliableEvent 使用 Java 17、Spring Boot 3.5.16、MySQL 8.0、RocketMQ 5.x 普通消息
>
> 原目标：在私有优惠券项目的“创建发券任务”链路中，让 `t_coupon_task` 与 Outbox 事件在同一个本地事务内提交，由 Starter 在任务到期后发布原有业务消息，并用实际业务结果证明故障恢复与重复消费安全。

以下实施顺序和验收矩阵均为取消前的历史方案，不再作为待办。私有项目中已写入的代码未在本次范围变更中回滚；其运行状态与后续处置应由私有项目单独决定。本仓库不宣称该业务链路已验收。

M5.1 的原创示例已验证 Starter 可由独立应用接入。M5.2 原计划验证私有真实业务链路。私有项目仍位于原仓库；本仓库只保留历史接入协议与进度，不复制其业务源码、数据库数据或凭据。原计划对应[项目方向文档的历史记录](../PROJECT_DIRECTION.md#19-已取消的优惠券项目落地计划历史记录)。

## 已从私有项目代码确认的现状

- `merchant-admin` 的 `CouponTaskServiceImpl.createCouponTask` 已有 Spring 事务，先插入 `t_coupon_task`。立即任务在这个方法内调用旧 RocketMQ Producer；定时任务由 `CouponTaskJobHandler` 扫描到期的 `PENDING` 任务，先改为 `IN_PROGRESS` 再调用同一 Producer。因此切换时要分别关闭立即发送和 XXL-Job 发送入口。
- 旧消息的 Key 是任务 ID，Body 为包含 `keys`、`message`、`timestamp` 的包装对象，其中 `message.couponTaskId` 是消费者读取的字段。`distribution` 的 `CouponTaskExecuteConsumer` 接收这个包装对象，并要求任务状态为 `IN_PROGRESS`；直接发送只有 `couponTaskId` 的裸 JSON 将无法保持现有消费协议。
- 消费入口当前用 Redis 键按任务 ID 防重复，过期时间为 3600 秒；随后读取 Excel 并提交批量工作。这个有限期入口锁不能单独证明故障后或过期后的发券业务效果永久幂等，需进一步验证批次与单用户发券副作用。
- 私有项目为 Java 17、Spring Boot 3.0.7，现有 RocketMQ Spring Starter 使用 NameServer 连接；ReliableEvent 当前构建基线是 Spring Boot 3.5.16，RocketMQ 5.x 发送端使用 gRPC Proxy。不能把两套客户端地址视为可互换，也不能未经集成测试就宣称版本兼容。
- `merchant-admin` 使用 ShardingSphere 逻辑数据源，配置包含两个物理 MySQL 库；`t_coupon_task` 未列入分片规则。接入前必须以事务测试与实际路由确认任务行和 Outbox 行位于同一个物理事务资源。只看到同一个逻辑 `DataSource` Bean 不足以证明原子性。

## 本阶段交付物

1. 私有项目中的一条实际接入链路：通过 Maven 依赖使用 `reliable-event-spring-boot-starter`，在创建发券任务的业务事务中登记 `coupon-task-execute` 事件，移除这条链路原有的直接发送或事务后发送入口，避免同一任务产生两套生产路径。
2. 私有项目的 Outbox 建表或增量迁移、非敏感配置和回退步骤。使用本仓库提供的 [正式建表 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)；已有旧表按实际版本核对迁移，M4.4 表需执行 [M4.5 增量 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m4-5.sql)。
3. 私有项目中的消费者幂等验证。若现有消费链路已有持久化去重机制，证明其去重键和业务更新的事务边界满足本阶段要求；否则在私有项目内补齐。不能仅凭 Broker Message ID 或内存集合去重。
4. 私有环境的自动化测试或可重复执行的故障演练，以及一份脱敏的 M5.2 完成记录：环境版本、执行命令、事件身份、状态转移和业务结果。完成记录不包含私有源码、真实用户信息、连接串或密钥。

## 接入前核对

先从私有项目确认以下事实，并将结论记录在私有项目的接入说明中。尚未核实前不把示例值写成真实业务契约。

| 核对项 | 需要确认的内容 |
| --- | --- |
| 数据库事务 | `t_coupon_task` 和新增 Outbox 行的实际物理库、ShardingSphere 路由、事务管理器与回滚行为；MySQL 服务端版本 |
| 消息兼容 | 实际 Topic 名称及环境后缀、Tag 是否为空、旧 Body 的真实 JSON、旧消费者能否读取新 Producer 的 JSON 与消息属性 |
| 定时状态 | `send_time` 的时区、空值与取消规则；停用 XXL-Job 后，定时任务在消费前如何从 `PENDING` 安全转为 `IN_PROGRESS` |
| 消费副作用 | Excel 批次、单用户发券、下游消息的持久化幂等边界；Redis 去重过期、进程退出或重新投递后是否会重复发券 |
| 运行兼容 | Spring Boot 3.0.7 与 Starter 的依赖兼容；RocketMQ Broker 是否提供可达的 5.x gRPC Proxy；现有 NameServer 消费路径与新发送路径是否共用预期 Topic |

如果真实链路与已冻结的 `coupon-task-execute`、任务 ID 或 `send_time` 语义不符，先在私有接入说明中写明差异及迁移映射，再实施。不要为了满足文档名称而悄悄改变现有消费者的业务协议。

## 业务事务与事件身份

创建任务的业务方法必须有活动的 Spring 数据库事务，`ReliableEventPublisher.publish(...)` 必须使用与 `t_coupon_task` 相同的 `DataSource` 和事务连接。Starter 当前只支持单数据源 MySQL 8.0；若业务记录与 Outbox 不在同一事务资源中，本阶段不能宣称原子提交。

```text
创建发券任务
  └─ 一个 MySQL 本地事务：写入 t_coupon_task + publish(coupon-task-execute)
         ├─ 回滚：任务和 Outbox 事件均不存在
         └─ 提交：任务和一条 PENDING 事件同时存在
                         ↓
              到期后 Starter 抢占并发布原有业务消息
                         ↓
              原有分发服务按业务身份幂等地执行发券
```

- `eventType` 固定为 `coupon-task-execute`；`eventKey` 使用任务 ID 的稳定字符串表示。同一 `eventType + eventKey` 再次登记时，Outbox 返回已有事件 ID，不创建第二行。业务请求是否允许创建两个不同任务，仍由私有项目的业务幂等规则决定。
- 立即任务以当前时刻登记 `availableAt`；定时任务将 `send_time` 按明确时区转换为 `Instant`。只承诺到期后可被扫描，不承诺精确到某一毫秒发送；轮询、数据库负载和本地排队会增加延迟。
- Payload 必须先与现有消费者的包装对象 JSON 契约对齐，至少保留 `keys`、`message.couponTaskId` 和需要的时间字段，并以真实 Broker 消息验证反序列化；不能直接把只有 `couponTaskId` 的事件对象作为 Body。若需修改消费者协议，生产者和消费者的兼容发布顺序必须在私有项目中记录并验证。Headers 只放必要的非敏感字符串；不放手机号、优惠券码、密钥或完整业务对象。
- RocketMQ 适配将 `eventKey` 写为 Message Key，将事件 ID、类型和键写入 `reliable_event_id`、`reliable_event_type`、`reliable_event_key` 属性。Topic/Tag 用实际资源配置映射，不在本仓库猜测名称。消费端使用稳定业务身份去重；重试后的 Broker Message ID 可能不同。
- 创建任务事务中只登记事件，不同步等待 RocketMQ 发送，也不保留第二条直接发送路径。业务响应仅表示任务与 Outbox 已提交，不表示 Broker 或消费者已经完成。

## 消费、故障与切换协议

现有 Redis 入口去重只有有限有效期，消费者还会异步处理 Excel 批次和下游发券消息。接入前必须追踪每个持久化副作用，选定可长期核对的业务去重键，并在各副作用的事务边界保证重复事件不会重复发券；不能只用入口 Redis 键或 Broker Message ID 证明端到端幂等。若同一业务身份携带不同可靠事件 ID，按业务异常显式处理。仅在实际业务效果安全提交后确认相应消息；异步工作需有独立的完成和恢复证据。

旧 XXL-Job 在发送前把定时任务设为 `IN_PROGRESS`，新 Outbox 到期发送本身不会修改 `t_coupon_task`。若直接停用 XXL-Job，现有消费者会因任务仍为 `PENDING` 而拒绝执行。私有改造必须明确新的状态转换位置，并以条件更新保护到期、取消和重复消息竞态；立即任务和定时任务都需覆盖。不能通过创建时就把未来任务置为 `IN_PROGRESS` 来绕过校验。

切换采用可回退的单路径发布：先完成表迁移、Topic/Tag 映射及消费者兼容验证，再用同一业务开关将新任务从旧生产入口切到“事务内登记 Outbox”；不得让两条路径同时处理同一任务。Starter 加入依赖后默认启用，应在切换前确认其配置和启动行为。若需灰度，按业务流量明确分配旧路径和新路径，任何一个任务只能落入其中一条。回退时停止新任务登记到 Outbox，但保留已提交事件的发布与恢复能力，或先确认这些事件已妥善处理；不得直接删除、重置 Outbox 或让旧路径把同一任务再发一遍。

观察故障时区分三个事实：Outbox `PUBLISHED` 仅表示生产端拿到成功回执；实际发券以消费者业务记录为准；发送结果未知可能导致两条不同 Broker Message ID 对应同一事件。`DEAD` 不会自动重放，需告警和人工排查，本阶段不通过手改状态模拟重放。

## 实施顺序

1. 固定改造前的旧消息样本，验证 Spring Boot 依赖兼容、RocketMQ gRPC Proxy 可达性和 ShardingSphere 的真实事务路由。上述任一门槛未通过时，不切换业务流量。
2. 在已确认的同一物理事务资源中创建 Outbox 表，加入 Starter 依赖和 `coupon-task-execute` 的目标映射。验证发送超时与 `lease-duration` 的关系，以及 Spring 关闭阶段超时。
3. 改造任务创建事务：任务 ID 已确定后登记事件，保留原有业务校验；以同一业务开关切换立即发送与 XXL-Job 定时发送。用集成测试证明提交、回滚、重复登记和定时可用时间。
4. 对照旧消息验证真实 Broker 上的 Topic、Tag、Key、包装 Body 与必要属性；补齐定时任务的条件状态转换，并在私有消费链路验证重复投递不会重复产生发券副作用。
5. 做 Broker/Proxy 暂时不可用、发送结果未知和发布进程退出后的恢复演练；使用有截止时间的轮询观察数据库状态与消费结果，不依赖固定 `sleep` 判断成功。
6. 运行私有项目相关回归和本仓库 `mvn verify`。整理脱敏完成记录，更新交接状态；测试和业务证据齐备后才宣称 M5.2 完成。

## 验收矩阵

| 场景 | 必须观察到的事实 |
| --- | --- |
| 创建成功 | `t_coupon_task` 一行与同任务 ID 的 Outbox 一行同事务提交；自动发布后事件到 `PUBLISHED`，原有分发服务完成一次预期业务效果 |
| 创建回滚 | 业务行和 Outbox 行均不存在；没有该任务的 RocketMQ 消息或发券效果 |
| 同一任务重复登记 | `(coupon-task-execute, 任务 ID)` 仍只有一条 Outbox 行并返回原事件 ID；不得借此掩盖业务层重复建任务 |
| 定时任务 | `send_time` 前不发送；到期后可被扫描、发送并处理。核对数据库、应用和业务时区 |
| 定时任务状态 | 停用旧 XXL-Job 发送后，到期消息仍能在未取消的前提下安全转为 `IN_PROGRESS`；重复消息不能重复启动发券 |
| Broker/Proxy 故障 | 创建事务正常提交；发送失败进入 `RETRY_WAIT`；服务恢复并到退避时间后进入 `PUBLISHED`，消费业务效果一次 |
| 发送结果未知或进程退出 | 租约恢复及重试后最终可发布；即使 Broker 收到重复消息，稳定事件身份相同，消费业务效果仍一次 |
| 多实例竞争 | 两个发布实例共用该 Outbox 表时同一轮最多一个获得有效租约；不把重复投递误判为并发抢占失败 |
| 不可重试或耗尽 | 事件进入 `DEAD`，不会继续自动发送；有可定位事件身份和明确的人工处置责任 |

验收证据至少包含：私有项目所用版本与配置项名称、测试或演练命令、关键断言或脱敏 SQL 结果、RocketMQ 消息身份、消费者业务结果、失败后的恢复步骤。全仓测试数和结果以本次实际执行为准，不沿用 M5.1 的 134 个测试作为 M5.2 结论。

## 阶段边界

M5.2 只验证创建发券任务这一条私有业务链路。它不交付私有源码、不替换分发服务、不承诺 Exactly Once 或消息严格顺序，也不新增通用消费者框架、租约续期或 `DEAD` 人工重放接口。预约提醒与用户券过期事件需要单独评估。吞吐、延迟、数据库成本的可复现基准属于 M5.3；发布与运维检查属于 M5.4。

## 参考

- [项目方向文档](../PROJECT_DIRECTION.md)
- [当前交接](../HANDOFF.md)
- [M5.1 实施文档](M5_1_ORIGINAL_EXAMPLE_APPLICATION.md)
- [原创订单示例](../../reliable-event-example/README.md)
