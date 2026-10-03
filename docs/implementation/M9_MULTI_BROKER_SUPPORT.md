# M9：多消息中间件支持实施计划

> 编写日期：2026-10-03（Asia/Shanghai）  
> 状态：M9.1 公共发送契约、M9.2 RocketMQ 模块拆分和 M9.3 Kafka 适配均已实施并验收。Kafka 已通过真实 Broker、MySQL Outbox 状态、独立示例和仓库外安装件消费验证；RabbitMQ 仍未实现。  
> 源码依据：编写时 HEAD 为 `ac5e6ca`。当前产品仍是 `0.1.0-SNAPSHOT`，已实现 RocketMQ；M8.1–M8.5 已完成，M8.6 待实施。

## 1. 目标与交付范围

将 ReliableEvent 从“面向 RocketMQ 的可靠消息 Starter”扩展为“基于 MySQL Transactional Outbox、可选择消息中间件的可靠事件发布组件”。业务方继续在活动的 Spring 数据库事务中调用 `ReliableEventPublisher.publish()`，通过选择适配器 Starter 和配置，将事件发送到选定中间件。

本阶段已交付公共发布引擎、正式发送扩展接口、拆分后的 RocketMQ 适配和 Kafka 适配。RocketMQ 与 Kafka 均完成本阶段验收；RabbitMQ 保留为候选，尚未实现。

本阶段继续使用 Java 17、Spring Boot 3 和单数据源 MySQL 8.0。业务事件模型、业务事务与身份登记协议、版本号抢占、数据库时间租约、退避、死信、受控重放、保留清理和观测能力由公共引擎负责。

本阶段的部署单位是一套逻辑 Outbox 存储：同一套存储的所有发布实例使用同一个中间件、同一个目标集群和一致的事件路由。应用可在不同部署中选择不同中间件；不支持同一套 Outbox 的实例分别向不同中间件竞争发送。

本阶段不交付以下能力：

- 同一个事件同时发送多个中间件、自动跨中间件故障切换或按事件选择中间件。
- 把既有 Outbox 直接改配置迁移到另一种中间件。
- 通用消费者框架、消费结果回写、消费者去重记录的统一管理。
- PostgreSQL 等数据库适配、脱离 Spring 的事务接入、CDC/Binlog 发布。
- Exactly Once、跨数据库与 Broker 的原子事务、严格顺序保证。
- Topic、Exchange、Queue 的生产自动创建、管理后台或新的租约续期机制。

这些边界是本计划的实施建议。用户后续扩大范围时应修订计划及验收，不能把“多中间件支持”自动解释为多目标投递。

## 2. 当前代码依据与改造位置

> 历史基线：本节描述 M9 计划初稿编写时的代码状态，用于说明当时的改造起点；后续 M9.1/M9.2 分阶段验收和当前模块职责分别见第 16–18 节。

现有代码已经将 JDBC Worker 和 RocketMQ Sender 分开，但还未形成独立的、受支持的扩展契约。

- [公共发布 API](../../reliable-event-core/src/main/java/dev/reliableevent/ReliableEventPublisher.java)只负责事件登记，业务调用方没有直接依赖 Broker，适合继续保留。
- [内部 EventSender](../../reliable-event-core/src/main/java/dev/reliableevent/internal/publication/EventSender.java)是已有接缝；其 package 文档明确不是受支持的扩展 API。新增适配器不应长期依赖该内部包。
- [SendReceipt](../../reliable-event-core/src/main/java/dev/reliableevent/internal/publication/SendReceipt.java)强制非空 Message ID。确认成功与 Broker 是否生成 Message ID 必须分开建模。
- [JdbcEventPublicationWorker](../../reliable-event-jdbc/src/main/java/dev/reliableevent/jdbc/internal/publication/JdbcEventPublicationWorker.java)依赖发送结果及失败类别，适合复用状态机；回执日志和追踪字段需要适配。
- [ReliableEventAutoConfiguration](../../reliable-event-spring-boot-autoconfigure/src/main/java/dev/reliableevent/autoconfigure/ReliableEventAutoConfiguration.java)同时装配 JDBC 和 RocketMQ，需要移出 RocketMQ 相关配置。
- [ReliableEventProperties](../../reliable-event-spring-boot-autoconfigure/src/main/java/dev/reliableevent/autoconfigure/ReliableEventProperties.java)直接引用 `RocketMqDestination`，且根配置绑定开启严格未知字段检查；拆分时必须同时处理类型依赖和配置绑定。
- 编写计划时的旧 Starter POM 曾聚合 RocketMQ；该模块在后续用户决定中删除。当前有效入口为 [Base Starter](../../reliable-event-spring-boot-starter-base/pom.xml) 与 [RocketMQ 专用 Starter](../../reliable-event-rocketmq-spring-boot-starter/pom.xml)。
- [Header 约束](../../reliable-event-core/src/main/java/dev/reliableevent/internal/headers/EventHeaderConstraints.java)已被登记追踪与 RocketMQ 使用；公共预算和具体中间件限制需明确分层。
- [默认采样器条件](../../reliable-event-spring-boot-autoconfigure/src/main/java/dev/reliableevent/autoconfigure/DefaultMetricsRuntimeCondition.java)检查默认 Bean 的名称和工厂类。配置拆分不得使周期采样器意外消失或对自定义运行时错误启用。

编写计划时保存的历史回归报告为 34 份 Surefire XML、204 个测试、0 失败、0 错误、0 跳过。它是既有基线，不能代替 M9 最终源码的回归。初稿编写时尚无 M9.1 验收；本轮实际结果见第 16 节。

## 3. 可靠发布协议

### 3.1 保持的事务与状态语义

业务数据、永久身份和 Outbox 行仍在同一物理数据库事务中提交。重复 `(event_type, event_key)` 返回原 `EventId`，不覆盖原 Payload、Headers、可用时间，也不在已清理后重建发送记录。

后台仍按“限量恢复过期租约 → 查询候选 → 有界排队 → 执行线程抢占 → 事务外发送 → 条件状态更新”运行。排队不占租约；完成状态必须验证版本、Owner 和租约有效期。新增适配器不能绕开 Worker 自己更新 Outbox。

`availableAt` 继续由 Outbox 控制首次发送时间，不转换成 Broker 的延时消息功能。`attempt_count` 继续表示成功抢占次数，不表示底层客户端的网络请求数。

### 3.2 统一发送结果

公共发送接口保留同步完成语义：返回成功回执时，适配器已获得其契约规定的发送确认。调用客户端 API 返回、进入客户端缓冲区、生成本地消息 ID，都不能单独判定成功。

结果分为四类：

1. 成功确认：公共引擎尝试更新为 `PUBLISHED`。
2. 可重试失败：已知本次未完成可靠发送且原因可恢复，进入退避等待；尝试耗尽进入 `DEAD`。
3. 不可重试失败：非法消息、缺少本地路由、确定性的权限或配置拒绝等，直接进入 `DEAD`。具体异常映射由适配器冻结并测试。
4. 结果未知：消息可能已被接受，但确认没有可靠到达，按现有策略重试或在耗尽后进入 `DEAD`；下游必须容忍重复。

错误分类基于失败发生阶段及客户端异常类型，不解析异常消息文本。明确在入队/发送前失败与发送后超时分别处理；无法证明未发送时保守使用结果未知。适配器必须包装自己的未知异常，避免依赖现有 JDBC 分类器对普通运行时异常默认视为可重试的行为。

Broker 确认成功后数据库状态提交失败，保留原恢复路径，不能把已知成功回执改成“Broker 没收到”。`PUBLISHED` 表示按适配器契约确认发送并成功记录生产端状态，不证明消费者完成业务。

### 3.3 公共 API 与适配器 SPI

业务 API 保留在 `dev.reliableevent`；新增少量受支持的类型放入 `dev.reliableevent.spi`，优先使用现有 core 模块，不为只有几个类型提前增加独立生产工件。

拟定发送形状如下，名称和字段在 M9.0/M9.1 冻结后才成为正式兼容契约：

```java
public interface EventTransport {
    TransportReceipt send(OutboundEvent event);
}

public record OutboundEvent(
    EventId id,
    String eventType,
    String eventKey,
    String payloadJson,
    Map<String, String> headers
) { }

public record TransportReceipt(
    Optional<String> brokerMessageId,
    Map<String, String> metadata
) { }
```

`OutboundEvent` 是不可变的单次发送视图；Map 必须防御性复制。适配器不能修改持久化事件或把自身客户端对象放进公共 API。Payload 以已有 JSON 文本编码为 UTF-8，不让适配器重新序列化业务对象。

成功回执可以没有 `brokerMessageId`。RocketMQ 返回 Broker Message ID；Kafka 在白名单元数据中记录 topic/partition/offset；RabbitMQ 可记录确认状态及关联信息。应用自行设置的 AMQP message-id、确认序号、Outbox EventId 均不得冒充 Broker 生成的 Message ID。元数据只用于诊断，不用于业务去重，也不成为指标标签。

失败契约可将现有三类异常语义转为 SPI 类型，由内部桥接层转换，避免直接把整个 `internal` 包变成公共 API。第一步可以保留内部 Worker 接缝，用一个桥接层接入新 SPI；公共路径和旧内部自定义路径必须最终汇入同一个 Worker 状态机，不能形成两套重试引擎。

兼容桥接必须允许无 Message ID 的成功回执。不能填造一个字符串以绕过旧 `SendReceipt` 校验；应显式调整内部回执及依赖它的日志、追踪和测试。适配器返回 null 属于协议异常，不能视为确认成功。

## 4. 模块与依赖方案

目标依赖方向为：业务应用 → 对应 Starter → 公共运行时与选定适配器 → 公共 core/SPI。JDBC、公共调度、指标和追踪模块不能依赖 RocketMQ、Kafka 或 RabbitMQ 的客户端类型。

拟定模块职责：

- `reliable-event-core`：保留业务 API，增加发送 SPI 和公共传输模型。
- `reliable-event-jdbc`：保留登记、状态机、恢复、死信及清理；通过内部桥接调用 SPI。
- `reliable-event-spring-boot-autoconfigure`：只保留公共配置、JDBC 装配、发布运行时、指标与追踪。
- `reliable-event-spring-boot-starter-base`：聚合公共 core、JDBC、公共自动配置及 JSON/JDBC 依赖，不包含任何 Broker 客户端。
- `reliable-event-rocketmq`：不依赖 Spring，直接实现 RocketMQ 协议适配、消息构造、路由和错误分类。
- `reliable-event-rocketmq-spring-boot-autoconfigure`：绑定 RocketMQ 属性、创建适配器与默认客户端并管理资源生命周期。
- `reliable-event-rocketmq-spring-boot-starter`：只聚合 Base 与 RocketMQ 自动配置。
- Kafka 新增 `reliable-event-kafka`、`reliable-event-kafka-spring-boot-autoconfigure`、纯依赖 `reliable-event-kafka-spring-boot-starter`；它与 RocketMQ Starter 共用 Base。
- 原 `reliable-event-spring-boot-starter`：本计划初版拟保留的兼容聚合入口；后续用户决定改为删除旧坐标，只保留 Base 和各已实现中间件专用 Starter，当前接入不得再依赖旧坐标。

未选定的适配器不创建空模块或空 Starter。模块数量随实际交付扩展，公共示例和基准模块仍是验证材料。必要的共享适配契约测试在第二个真实适配出现后提取，不先建设通用插件市场或加载器。

新增依赖先核对实际 Spring Boot BOM、Java 17 支持和测试容器兼容性，在 M9.0 固定客户端、Broker、Testcontainers 及镜像版本。Kafka 使用项目 Spring Boot 3.5.16 BOM 管理的 Kafka Client 3.9.2、Testcontainers 1.21.4 和 `apache/kafka:3.9.2`；不得顺带升级既有依赖。

## 5. 配置、选择与启动行为

### 5.1 配置分层

保留公共配置前缀 `reliable-event`，包含现有调度、容量、租约、重试、保留和采样属性。新增 `reliable-event.transport`，支持已实现的 `rocketmq`、选定新增中间件及显式 `custom` 模式。

现有 `reliable-event.rocketmq.*` 键继续兼容；Kafka 使用 `reliable-event.kafka.*`，RabbitMQ 使用 `reliable-event.rabbitmq.*`。具体路由结构由各适配器负责，不定义一个只能表达 `topic:tag` 的通用 destination 字符串。

公共模型保持字符串 Headers、既有保留前缀和追踪预算；Broker 对 Header、Body、属性名称及大小的额外限制由适配器校验。用户声明非法 Header 不能因追踪降级而被吞掉；历史 Headers 解析失败须进入明确的发送失败路径。

根 `ReliableEventProperties` 的严格绑定与适配器子前缀会发生交叉：单纯移走 `rocketmq` 字段会把合法适配配置视为未知字段，单纯改成忽略所有未知字段又会掩盖拼写错误。M9.2 必须实现并测试分层属性校验：根只排除已注册的适配器命名空间，公共未知键及已加载适配器内部未知键仍报错。未实现/未注册的 transport 命名空间不能被泛化 Map 静默接收。

### 5.2 选择规则

- 仅安装 RocketMQ 或仅安装 Kafka 专用 Starter 且未设置 transport 时，从唯一内置适配器选择并记录最终 transport。
- 同时安装 RocketMQ 与 Kafka Starter 时，必须显式选择 `rocketmq` 或 `kafka`；禁止按装配顺序挑选。
- 多个内置适配器同时存在时，必须显式选择 transport。禁止按自动配置先后次序决定获胜者。
- 显式选择已支持适配器但其依赖不存在，或者同一选择产生多个实现时，启动失败并说明缺失模块/冲突 Bean。
- `custom` 模式只接受一个符合 SPI 的自定义实现，并明确发送预算。当前 internal `EventSender` 覆盖方式经单一兼容桥接保留，标记为迁移路径；新旧两种自定义入口同时提供时失败。
- 自定义发送实现存在时，默认 Broker 客户端不应连接。自定义客户端资源不由默认配置关闭；默认创建资源才由模块管理。
- base 单独使用时默认仍要求完整发送实现，避免应用可登记却无人发布。用于显式登记端部署时，要求 `scheduling-enabled=false` 并在文档中指定独立发布实例；无 Sender 的有意登记模式要有单独测试。
- `enabled=false` 时不启动默认发送、扫描或采样；自定义 Observer、Tracer 与运行时的退让行为继续保持。

兼容时允许系统识别旧 RocketMQ 配置，但在显式选择其他中间件时，对非空且可能误用的未选中配置至少给出脱敏提示。配置错误提示不得打印连接凭据或 SASL 配置正文。

### 5.3 配置示意

RocketMQ 与 Kafka 配置由各自适配器严格绑定；下面 RabbitMQ 示例仍是未实现设计草图。Kafka 当前 `reliable-event.kafka` 属性包括 `bootstrap-servers`、`max-block`、`request-timeout`、`delivery-timeout`、`linger`、`custom-producer-send-budget`、`max-body-bytes` 与 `mappings.<eventType>.topic`，未知字段和 mapping 子字段会报错。

```yaml
reliable-event:
  transport: kafka
  lease-duration: 30s
  kafka:
    bootstrap-servers: localhost:9092
    max-block: 1s
    request-timeout: 3s
    delivery-timeout: 5s
    linger: 0ms
    mappings:
      order-created:
        topic: orders
```

```yaml
reliable-event:
  transport: rabbitmq
  lease-duration: 30s
  rabbitmq:
    addresses: localhost:5672
    send-timeout: 15s
    mappings:
      order-created:
        exchange: orders
        routing-key: created
```

Kafka Starter 默认要求显式 Broker 地址和事件类型到 Topic 映射。生产不会创建 Topic；本地 `max-body-bytes` 限制不会调整 Broker/topic `max.message.bytes`，应用需要确认实际 Broker 能接收配置上限内的记录。自定义 Producer 的 `String` key、`byte[]` UTF-8 value、`acks=all`、幂等、非事务和完整发送预算由应用配置；框架只验证 Producer 类型数量、声明预算与 Outbox 租约关系，不会检查该 Bean 的实际 Kafka 配置。

认证、TLS、虚拟主机或 Kafka 安全参数在对应适配文档中定义，通过环境/外部配置提供。受影响的 Spring Boot 原生自动配置与项目专用客户端之间要明确资源所有权，不能假设引入 Starter 后所有用户客户端都由项目接管。

## 6. 三类中间件的适配要求

### 6.1 RocketMQ：先保住既有行为

保留 Event Type 到 Topic/Tag、Event Key 到 Message Key、JSON 到 Body、Headers 到用户属性的映射，以及 `reliable_event_id/type/key` 身份字段。保留当前客户端重试设置、发送异常分类、同步回执和默认 Producer 生命周期。

拆分后的 RocketMQ 必须重新通过已有真实服务测试：映射、Broker 不可用恢复、未知结果重复消息、自动调度、双实例竞争、停机接管及 M8 追踪。框架通用化不等于重新解释旧错误类别。

### 6.2 Kafka（M9.3 选定适配器）

Kafka 适配采用原生 Java Producer，业务键作为 record key，Payload 为 UTF-8 JSON，身份和追踪字段进入 record headers。Topic 由适配器配置映射；新适配器生产默认不主动创建 Topic，测试环境显式创建。支持 key 分区不等于公共引擎承诺事件顺序。

发送契约要求 `acks=all`、`enable.idempotence=true`、`max.in.flight.requests.per.connection<=5`，由客户端在交付超时内完成内部重试，Outbox 管理后续逻辑尝试。默认配置的阻塞预算为 `max.block.ms (1s) + delivery.timeout.ms (5s) + confirmation slack (1s)`；租约必须严格大于 7 秒发送预算加 1 秒状态更新预留。自定义 Producer 必须显式声明 `custom-producer-send-budget`，应用负责保证正确的 String/byte[] 序列化、确认强度、幂等和非事务约束及预算，框架不声称能检查用户 Producer 的实际运行配置。[Kafka 3.9 Producer 配置](https://kafka.apache.org/39/configuration/producer-configs/)

异步 `send()` 必须在适配器内等待最终结果；获得有效 topic/partition/offset 后才返回成功。单一 `System.nanoTime` 截止时间同时覆盖 `send()` 阻塞及 Future 等待；超时和发送后的网络/未知异常返回结果未知，不注册迟到回调写 Outbox，也不二次后台发送。Kafka 的 Producer 幂等不能消除 Outbox 的应用级重发；消费者仍按稳定事件身份去重。[Kafka 3.9 KafkaProducer API](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)

M9.3 针对冻结版本验证异常阶段分类：本地缺路由/编码错误、Broker 明确拒绝、连接不可用、元数据等待和入队后交付超时。异步网络/未知结果不证明发送失败；确定的授权、非法记录及超 Topic 消息限制进入不可重试路径。`UnknownTopicOrPartition` 等错误按实际阶段与配置处理，不能仅凭名称一概判为永久错误。Topic 必须由部署或测试管理显式创建，适配器不创建 Topic。

记录位置放在回执的诊断元数据中，不把 `topic-partition-offset` 称为全系统唯一事件身份。单 Broker 开发测试只证明记录的配置场景；上线还需验证副本、ISR 和可用性设置，不能从单 Broker 测试推导多副本容灾能力。

### 6.3 RabbitMQ 候选

RabbitMQ 适配优先使用与 Spring Boot BOM 相容的 Spring AMQP `RabbitTemplate`，复用其 Channel 管理和关联确认能力，避免多个 Worker 无保护地共享原生 Channel。RabbitMQ 官方建议避免并发发布共享 Channel，并列出已有池化方案。[Java Client 并发说明](https://www.rabbitmq.com/client-libraries/java-api-guide#concurrency)

映射为事件类型到 Exchange/Routing Key，Body 为 UTF-8 JSON，Headers 保留稳定身份与追踪。消息采用持久化发布策略；示例显式声明持久化拓扑。AMQP message-id 可设置为稳定 EventId，但它仍是生产者提供的字段，不是 Broker 生成 ID。

发送必须同时启用 Publisher Confirms 和 mandatory 路由检查。不可路由消息可能既产生 return 又收到 ack；成功判定必须是收到肯定确认且未被退回。持久化确认强度取决于队列类型及拓扑，不能单凭写入 Socket 判定成功。[RabbitMQ 确认语义](https://www.rabbitmq.com/docs/confirms)

每次抢占创建独立 correlation 标识，区分同一 EventId 的不同尝试；通过对应的确认 Future 和 returned message 判定。Spring AMQP 文档规定同时启用 confirms/returns 时，退回信息先于该 Future 的 ack 完成设置；实现仍须以所选版本进行竞态测试，不能用任意等待几十毫秒代替确认协议。[Spring AMQP 关联确认与退回](https://docs.spring.io/spring-amqp/reference/amqp/template.html)

确认超时或连接在确认前断开通常进入结果未知；明确 nack 的原因按可恢复性分类。本地缺路由、确定的权限拒绝或 mandatory return 的路由缺失按冻结策略处理，本计划建议路由缺失进入 DEAD，修复绑定后受控重放。不得把不存在 Exchange 引起的 Channel 异常误判为发送成功。

关联表、连接/Channel 获取、发布阻塞、确认等待和关闭都必须有资源上限；超时后的回调仅完成诊断与释放，不回写 Outbox、不重新发送。禁止引入无界 Future 列表或适配器自行执行后台永久重试。

## 7. 超时、租约与资源生命周期

每个适配器需要声明有效发送预算 `Tsend`。默认配置的租约必须满足 `lease-duration > Tsend + 状态落库预留`；预留是容量配置，不是数据库提交一定按时返回的保证。自定义实现也必须显式声明预算或接受明确的外部协调责任。

`Tsend` 计入连接/资源获取、客户端调用、缓冲/元数据等待、内部重试及确认等待。Kafka 的入队等待与交付等待、RabbitMQ 的资源获取/发布阻塞与确认等待，应按阶段组合核对；不能简单把 RocketMQ 的 `request-timeout` 改名后套用。

RocketMQ 与 Kafka 的预算按各自参数单独冻结；Kafka 默认 7 秒总发送预算加 1 秒数据库状态更新预留，默认 30 秒租约满足严格大于关系。毫秒值会按 Kafka `ConfigDef` 的 32 位整数约束校验，body 上限加记录/header 余量也必须可表示；不满足预算关系时启动失败。自定义 Producer 的运行参数不可由框架观察，应用需按声明预算及确认/编码责任配置。超时后客户端仍可能继续发送，迟到结果不能更新已结束的 Outbox attempt。

保留当前 Scheduler 的停止顺序：停止新抢占、撤销排队候选、按现有预算等待发送及状态更新，再关闭默认创建的客户端。迟到结果、旧版本及过期 Owner 均不能绕过数据库栅栏。

默认客户端的连接、确认关联、缓冲队列和回调线程须有明确容量及关闭责任；适配器不得复制一套无界执行器。优先让已有 Worker 同步等待受限的客户端完成结果，避免改变本地容量口径。

公共 `shutdown-timeout` 仍不等于整个 Spring Context 或所有客户端 close 的总时长。各适配器能提供的 close 预算分别说明；未能停止的调用保持至少一次恢复边界。指标采样器仍按独立预算与当前生命周期管理，不能因适配器调整延长其资源占用。

## 8. 身份、路由、追踪与观测

### 8.1 稳定消息身份与路由

所有适配器统一携带 `reliable_event_id`、`reliable_event_type`、`reliable_event_key`，业务去重依据保持为稳定身份而非 Broker 回执。禁止用户 Header 覆盖保留身份字段。Broker 对 Header 的编码差异由适配器完成。

Outbox 当前没有持久化中间件、目标集群或路由快照。M9 复用现有表结构，因此只能支持固定存储、固定中间件和受控路由配置；不能宣称本地 transport 选择校验能阻止不同实例混跑。

共享库实例的中间件、集群和映射一致性由部署配置管理负责，在启动日志/部署清单记录脱敏的逻辑存储与目标标识，实施验收核对。已有未完成事件和 DEAD 重放仍使用原路由；切换集群或改变类型映射之前需专门核对存量事件。新中间件示例使用独立数据库，不能拿 RocketMQ 示例的库直接启动 Kafka/RabbitMQ Worker。

若后续要求自动防止混跑、跨 Broker 切换或多目标投递，应另行增加存储绑定/持久化路由/独立投递记录协议，并定义迁移。一个 Outbox status 无法分别表达多个目标的成功与失败，不能仅新增 transport 字符串就宣称支持广播。

### 8.2 追踪

登记仍持久化原请求的 W3C 上下文。每次成功抢占创建独立发布 Span，注入本次发送的 Header 副本；不覆盖数据库原 Headers。同一事件重试继续关联原登记上下文。

适配器仅负责准确传递当前 Header，默认不再额外启动第二套发送 Span；启用客户端原生观测时应验证是否重复追踪，并说明关系。无效上下文、缺依赖、关闭追踪或导出失败继续保持原降级行为。

Broker Message ID 可缺失；Kafka 记录位置和 RabbitMQ 确认信息作为诊断字段处理。适配器元数据不得携带认证信息；对任意自定义元数据实行键数、长度和允许键约束。

### 8.3 指标与告警

保留现有公共指标名称与口径：发送回执、发送失败、状态提交进度、DEAD 转换、队列/在途、恢复、清理和整体数据库快照。公共 Timer 测逻辑发送调用，包含客户端内部等待；网络请求次数只能是另行说明的客户端指标。

默认不为旧指标增加新的必选标签，避免破坏 M8.5 查询与已有用户；中间件区分优先使用部署 scrape 标签，值限制为固定集合。EventId、业务键、Message ID、TraceId、offset、错误文本及连接地址不能成为公共指标标签。

共享数据库快照仍按 `outbox_store` 去重、不跨实例求和。新适配器必须核对 Prometheus 输出、M8.5 recording/alert rules 和看板查询；原平台仍保留 RocketMQ 演示，新中间件示例另提供可选 profile，不混用同一数据库。

## 9. 兼容、升级和回退

M9 保留公开 `ReliableEvent`、`ReliableEventPublisher`、`EventId` 和死信操作 API。RocketMQ 配置键、消息身份字段及主要指标继续有效，原示例与基准默认保持 RocketMQ。旧 Starter 坐标最初列为兼容要求，但用户后来决定删除旧模块；当前构建使用 Base、RocketMQ 和 Kafka 专用 Starter，详见第 18 节与第 19 节。

直接使用旧 internal 发送接口不属于既有正式公共兼容承诺，但已存在的自定义 Bean 场景仍提供有测试的迁移桥接。文档列出旧接口、新 SPI、冲突判定及逐步迁移方法，不无限期维护两个可独立运行的发送引擎。

M9 不新增 Outbox/identity/replay_audit 表迁移，也不改变 M7 已有迁移要求。旧版与新版 RocketMQ 实例共用同一表的兼容性须在相同客户端配置下验证；新中间件不与旧 RocketMQ 发布实例混用该存储。

回退框架拆分时保持原 Broker 与路由，只回退应用代码及依赖。M7 已发生数据清理后的旧写入协议回退禁令继续有效。切换 Broker 是单独迁移项目；已删除的旧 Starter 坐标不是当前构建的回退入口，也不能作为跨 Broker 数据回退。

示例部署与测试先停新登记、协调在途任务和旧实例退出，再切换相同中间件的新代码；不能在未知配置一致性的情况下滚动替换并宣称验收。通过滚动升级测试后，文档才可提供对应操作流程。

## 10. 阶段实施与验收门槛

### M9.0：冻结首个适配器与实现基线

交付：首个新增中间件选择、发送确认契约、客户端/Broker 固定版本、配置命名与资源预算、最终兼容目标和执行证据路径。

任务：核对当前 HEAD 与未提交文件；验证候选依赖在 Java 17、现有 Boot BOM 下可用；对照本计划冻结 SPI 名称、回执字段、选择规则、严格属性绑定与超时参数。所有示意值改为经过验证的实现值。

门槛：所选适配器及其契约明确；实施者不会自行把单目标范围扩大为多目标。尚未选适配器时，可先做共同接口与 RocketMQ 拆分，但不能替用户选择并开始候选 Broker 的实现。

### M9.1：公共发送契约与内部桥接

交付：core/SPI、不可变发送视图、统一失败分类、可缺 Message ID 的确认回执、公共引擎桥接和契约测试。

任务：更新 Worker 的回执处理、日志、Tracer、Observer 和测试 Fake；验证 Header 解析失败仍进入现有状态机；保留历史事件和原业务 API；对新旧自定义入口做冲突检查。

门槛：事务登记、重试、DEAD、租约恢复、M7 清理及 M8 观测基线通过；无 Message ID 的合法成功能够落库，null 回执和错误 SPI 实现不能被误记为成功；原 Message ID 诊断字段语义不被伪造。

### M9.2：RocketMQ 配置与 Starter 拆分

交付：Base、RocketMQ 专用自动配置/Starter、适配器选择与属性校验。计划初版的旧 Starter 坐标兼容要求已由后续用户决定取消；当前只支持新的专用 Starter 坐标。

任务：清除公共 Properties 和生产自动配置对 RocketMQ 类型的引用；调整装配顺序；保持默认指标 Bean/采样器归属判断；核对默认与自定义客户端关闭责任；更新配置元数据。

门槛（后续调整前）：旧依赖坐标和旧 YAML 能运行；公共模块无 RocketMQ 生产依赖；无 Broker 类时可按文档使用自定义 Sender/登记模式；多适配器未指定、显式适配缺失、重复 Sender、未知配置字段均给出明确结果；RocketMQ 真实服务全链路回归通过。base 默认调度开启时无 Sender 必须启动失败，登记模式须显式关闭调度并验证事务内登记。旧依赖坐标兼容要求后来经用户决定取消，当前验收要求见第 18 节。

### M9.3：首个新增适配器闭环

交付：Kafka 专用 Starter、独立 Compose/数据库示例、路由与消息构造、确认等待、异常分类、资源关闭和接入说明。

任务：按 Kafka 3.9 协议实现成功发送和稳定身份传播；在真实 Broker 上验证暂停后恢复、Topic 缺失恢复、Broker 永久拒绝及未知结果重投；新示例的业务事务和消费去重与原示例采用相同身份判定。

门槛：仅引入 Kafka Starter 的仓库外最小应用能登记、发布、消费，运行时没有 RocketMQ 类或对其配置的要求；真实收到的 UTF-8 Payload、key、身份 Header 与 Topic 符合契约；成功确认后落库，确认未知时本次 attempt 不落库，后续相同身份可以重试。

### M9.4：故障、并发、追踪与资源验收

交付：自动化故障测试、原始时间线与退出码、至少一次重复窗口、资源预算和观测核对记录。

任务：执行第 11 节场景；覆盖双 Worker、双实例、重试耗尽、独立 JVM 退出、发送成功后落库失败、迟到确认、停机期间确认和连接故障；验证没有增长不受限的客户端缓冲、线程或关联表。

门槛：所有原协议及新增适配语义均有真实服务证据；消息重投时稳定身份不变、消费业务效果一次；错误确认及超时不假报成功；无 Trace 串联泄漏；公开指标与规则保持兼容。

### M9.5：最终工件、运维交接与完成判定

交付：从最终源码构建的工件及依赖检查、配置/迁移/回退指南、M9 完成记录、README/OPERATIONS/HANDOFF 更新及最终回归证据。

任务：按第 12 节执行回归；仓库外项目分别验证 Base、自定义 SPI、RocketMQ 专用 Starter、Kafka-only Starter、双 Starter 选择及登记模式；记录运行版本、依赖树和安装工件哈希。必要时调整 CI 测试预算及报告检查，测试不能跳过后仍算通过。

门槛：至少 RocketMQ 和一个新增中间件均完整验收；当前最终源码的可靠发布测试通过；目标工件确实可被消费。正式版本、公开仓库和发布动作另行通过更新后的发布检查，M9 完成不自动表示已经发布。

下一种中间件以复用 M9.3–M9.5 的方式扩展，不再复制公共状态机；出现 SPI 无法表达的真实差异时先修订协议与兼容策略。

## 11. 测试场景与证据要求

### 11.1 公共协议与装配

- 业务提交/回滚同时影响业务数据、identity 和 Outbox；重复登记和清理后重复登记保持原 ID。
- 同一候选竞争只有一个有效抢占；旧版本、错误 Owner 和过期租约不能更新状态。
- 失败按三类进入 RETRY_WAIT/DEAD；恢复者竞争和旧 Worker 迟到完成保持已有栅栏。
- RocketMQ、Kafka 专用 Starter、base/custom、关闭自动调度、关闭全局功能、缺 Tracer/Registry、自定义 Observer/运行时分别验证。旧 Starter 坐标兼容作为早期计划项，后续按用户决定取消。
- 单一/多个适配器选择、缺依赖、属性拼写错误、默认/用户客户端资源关闭、自动配置顺序及采样器条件分别验证。
- Kafka-only JVM 的真实运行 classpath 中移除 RocketMQ，运行新增适配；仅 mock 掉 Producer 不足以证明依赖隔离。

### 11.2 真实 Broker 与独立进程

每个已支持的中间件都要具备以下证据：

1. 正常事件及未来事件：确认消息身份、Payload、Headers、路由和到期前未发送。
2. Broker 暂时不可用后恢复：记录失败、退避、恢复、最终 PUBLISHED；明确本次失败是发送前确定失败还是结果未知。
3. 确认丢失：先真实获得成功结果，再在测试装饰器中丢弃，确定性制造未知结果；独立消费者核对稳定身份和实际消息数量。这证明应用重投窗口，不伪称完整复现真实网络丢包。
4. 抢占后发送前杀死 JVM、确认后状态落库前杀死 JVM：新实例租约恢复；分别证明恢复能力及可能重复。
5. 状态提交失败、租约失效及迟到回调：回执成功与持久化失败分别记录，不由客户端回调越权提交状态。
6. 双实例共享同一中间件及数据库：验证正常抢占与接管，不把租约安全说成所有故障下只会发送一次。
7. 优雅停止与超时停止：排队候选不发送，在途任务处理有界；默认客户端关闭顺序正确，用户客户端不被擅自关闭。

Kafka 另验证最终记录位置、配置拒绝、入队超时/交付超时、内部重试与应用重试区分。RabbitMQ 另验证 mandatory return 加 ack、明确 nack、不存在 Exchange、确认与 return 的关联、确认迟到以及并发相关状态不串事件。

### 11.3 追踪、指标与开销

跨 HTTP、登记、发布和示例消费核对同一 Trace；每次逻辑尝试 SpanId 不同，数据库上下文保持原值。示例处理失败、幂等跳过和 ACK 失败仍分开记录。

核对发送计数与 afterCommit 状态计数；状态失败不能增加持久化进度。快照仍区分 ready 与连续未完成，陈旧样本不能当作零积压；多实例聚合和全部快照失效规则继续适用。

先在同一最终源码、同一 RocketMQ 环境下比较框架拆分前后影响；新增 Broker 的性能实验单独固定负载、客户端确认强度、线程容量、采样和拓扑。记录登记耗时、发送耗时、排空速率、数据库查询成本、CPU/内存及客户端资源，不套用历史 M5.3 数字或预设提升比例。

证据保存到 `target/evidence/m9.<阶段>-<中间件>-<时间>/`，包含源码标识、dirty 状态、有效配置（脱敏）、镜像/依赖版本、日志、退出码、实际测试数、数据库状态、消费身份及时间线。成功与失败尝试分开保留。

## 12. 验证命令与完成定义

实施阶段使用对应模块的聚焦测试，最终阶段从同一候选源码执行：

```powershell
java -version
mvn -version
git rev-parse HEAD
git status --short
mvn clean verify
mvn -Pobservability clean verify
python -m unittest discover -s reliable-event-benchmark/scripts/tests -v
./observability/scripts/validate-rules.ps1
```

以上是后续阶段的实施验收命令。初稿编写计划时未执行 Broker 故障演练、依赖修改或全仓测试；M9.1 的实际 Maven 验收见第 16 节。后续仍需分别记录 base 与 profile 结果，不用 profile 构建替代 base 构建；规则验证保持真实 promtool 退出码。新适配器容器必须可用，集成测试跳过不算通过。

RocketMQ 现有固定端口测试继续顺序运行；新增服务使用隔离端口、数据库和网络。CI 依据实际测试时长调整 45 分钟预算，不能靠关闭故障测试或隐藏跳过缩短时间。

本阶段只有满足以下事实才算完成：

- 至少两种中间件通过真实服务与故障验收，业务登记代码和 MySQL 状态机共用。
- 新 Starter 不要求 RocketMQ 依赖；公共代码无 Broker 类型耦合。
- 原配置/身份/指标兼容，严格配置校验和客户端关闭责任有测试；旧 Starter 坐标取消，当前包名遵循第 18 节。
- 成功确认、结果未知、无 Message ID 回执、晚到结果及重复消费均被正确处理。
- 所有最终必跑测试为 0 失败、0 错误、0 跳过，并保存实际证据。
- 运维文档明确共享 Outbox 一致配置、单中间件边界、路由变更与跨 Broker 迁移限制。
- 最终工件通过仓库外最小项目验证；公开发布状态如实记录。

## 13. 主要风险与处理顺序

最高优先级是消息确认语义、共享 Outbox 混跑和租约预算。先冻结契约，再验证迟到结果、未知重投与消费者幂等；不能以类加载成功或一个正常发送测试代替可靠性验收。

第二优先级是兼容拆分：严格配置绑定、装配顺序、原 Sender 覆盖、默认采样器 Bean 归属及资源关闭容易在抽模块时回归。RocketMQ 拆分通过后再实现新 Broker，减少同时变化的协议数量。

第三优先级是客户端内部重试和缓冲。公共一次逻辑发送可能包含多次底层请求，必须限制总等待与资源，避免层层重试放大。框架仍没有租约续期，长时间调用依赖原状态栅栏与恢复。

M8.6 的真实观测故障演练及开销检查继续保持待实施状态。M9 可在文档和共同接口层推进，但复用 M8 能力时不能宣称 M8.6 已完成；最终交接分别记录两个阶段的覆盖与未完成项。

## 14. 待确定项与后续执行入口

首个新增中间件方向已由用户选定为 Kafka。RabbitMQ 仍为候选，未获授权，不在本轮实现范围。

公共 SPI 名称已在 M9.1 确定；Kafka 版本与发送预算依据当前 Spring Boot BOM 和 Kafka 3.9 配置约束冻结，剩余实现细节按验收门槛推进。

建议执行顺序为 M9.0 → M9.1 → M9.2 → Kafka M9.3 → M9.4 → M9.5。M9.1 至 M9.3 已完成；M9.4 广泛故障审计与 M9.5 正式发布尚未完成。

## 15. M9.1 已实施边界

M9.1 在 `reliable-event-core` 的 `dev.reliableevent.spi` 提供 `EventTransport`、不可变 `OutboundEvent`、`TransportReceipt` 和 `TransportException`/`TransportFailureType`。业务侧 `ReliableEventPublisher` 保持原样；JDBC Worker 继续消费原内部 `EventSender`，由自动配置中的单一桥接将公共 SPI 接入现有状态机。公共 `EventTransport` Bean 会抑制默认 RocketMQ Sender 和 Producer 创建，因此不会因选择自定义 SPI 而先连接 RocketMQ。旧 `EventSender` 自定义 Bean 仍可单独使用；同时定义两种入口，或提供多个公共 transport Bean，启动失败。

公共适配器可按以下形式注册：

```java
@Bean
EventTransport eventTransport(MyBrokerClient client) {
    return outbound -> {
        // MyBrokerClient/MyBrokerConfirmation are application-provided placeholder types.
        MyBrokerConfirmation confirmation = client.sendAndAwaitConfirmation(outbound);
        // Read the ID from the actual Broker confirmation; return confirmed() if none exists.
        return new TransportReceipt(confirmation.brokerMessageId(), Map.of());
    };
}
```

成功时可用 `TransportReceipt.confirmed()` 返回无 Broker Message ID 的有效确认；`brokerMessageId` 使用 `Optional<String>`，不可用 Outbox EventId、offset、delivery tag 或生产者自设 ID 填充。失败通过 `TransportException` 分类：确定可恢复失败用 `RETRYABLE`，确定永久拒绝用 `NON_RETRYABLE`，不能证明 Broker 未接收用 `RESULT_UNKNOWN`。未分类运行时异常由桥接保守转成 `RESULT_UNKNOWN`，null 回执也不会记作成功。返回前必须完成同步确认，并将发送总耗时限制在适配器自己的发送预算内；公共引擎不替自定义适配器计算客户端内部等待时间。

`OutboundEvent` 的 Payload 是数据库中原始 JSON 文本，适配器应原样编码为 UTF-8；Headers 是不可变副本，且受现有头部规则约束。桥接解析历史 Headers 并在 transport 调用前校验，解析或约束失败按不可重试发送失败走现有状态机。诊断 metadata 只允许 `topic`、`partition`、`offset`、`exchange`、`routing_key`、`delivery_tag` 六个键，每个值不超过 256 UTF-8 字节；metadata 不进公共日志或指标标签。

内部 `SendReceipt.messageId` 仍保留旧字符串访问方式，允许为 null，并增加不可变 metadata；已有单参数构造可继续用于旧 Sender。缺少 ID 时 Worker 不写消息 ID 日志字段，Micrometer trace 不添加消息 ID 属性。M9.1 未新增 transport 属性、模块、Starter 或 Kafka/RabbitMQ 适配器；M9.2 的 RocketMQ 配置、默认 Producer 与消息映射拆分见第 17 节。

相关材料：[当前交接](../HANDOFF.md)、[接入与运维指南](../OPERATIONS.md)、[既有项目方向](../PROJECT_DIRECTION.md)、[M8 追踪与告警计划](M8_TRACING_AND_ALERTING.md)、[发布检查](M5_4_RELEASE_CHECK.md)。既有方向中“第一版只支持 RocketMQ”是历史 `0.1.0` 范围；本文提出后续扩展，不把计划写成当前已实现能力。

示例中的 MyBrokerClient 和 MyBrokerConfirmation 是应用自行实现的占位类型，ReliableEvent 不提供这两个类。必须从适配器等待到的真实 Broker 确认读取 brokerMessageId；若确认不提供 ID，使用 TransportReceipt.confirmed()。

## 16. M9.1 验收记录

使用 Java 17.0.12、Maven 3.9.9 和独立测试代理端口 `18081` 验证。源工作区 base `mvn "-DreliableEvent.test.rocketmqProxyPort=18081" verify` 与 `mvn -Pobservability "-DreliableEvent.test.rocketmqProxyPort=18081" verify` 均运行 216 个测试，失败、错误、跳过均为 0；但两次在所有相关测试通过后，均于示例模块 Spring Boot `repackage` 阶段因正在运行的示例进程占用目标 JAR 而未能完成 reactor。该既有进程未被停止。

为验证完整打包，在 `target/m9.1-verification/repo` 建立未纳入 Git 的隔离源码副本；原工作区与副本的 146 个 Maven POM/`src` 文件 SHA-256 比较无差异。隔离副本的 base `mvn -o "-Dmaven.repo.local=C:\Users\20659\.m2\repository" "-DreliableEvent.test.rocketmqProxyPort=18081" verify` 完整 BUILD SUCCESS，216 个测试全部通过、无跳过；observability profile 使用相同副本执行 `mvn -o "-Dmaven.repo.local=C:\Users\20659\.m2\repository" -Pobservability "-DreliableEvent.test.rocketmqProxyPort=18081" -DskipTests verify`，8 个模块均 BUILD SUCCESS。该 profile 检查只验证编译与打包，profile 的完整测试证据来自前述源工作区运行。

日志保存在 `target/evidence/m9.1-base-verify-20261003.log`、`target/evidence/m9.1-observability-verify-20261003.log`、`target/evidence/m9.1-isolated-base-verify-20261003.log` 和 `target/evidence/m9.1-isolated-observability-package-20261003.log`。隔离 profile package 使用 `-DskipTests`，不计作测试通过记录。

## 17. M9.2 已实施边界与验收记录

> 历史记录：本节记载首次 M9.2 拆分验收时的实现和证据。旧 Starter 当时保留为兼容入口；在后续结构深化中，用户决定删除该模块和坐标。当前代码与后续完整验收以第 18 节为准，本节的报告数字和当时事实不作改写。

M9.2 将构件拆为 `reliable-event-spring-boot-starter-base`、`reliable-event-rocketmq-spring-boot-autoconfigure` 和 `reliable-event-spring-boot-starter-rocketmq`。既有 `reliable-event-spring-boot-starter` 保留为 RocketMQ 兼容聚合入口，继续引入 RocketMQ Starter。公共 core、JDBC、公共 Spring Boot auto-configuration 与 base Starter 的生产依赖和类型引用不包含 RocketMQ；RocketMQ 属性绑定、路由、默认 Producer 创建及其生命周期由专用 auto-configuration 承担，适配仍接入 M9.1 公共 SPI 和既有 JDBC Worker 状态机。业务发布 API、旧 Starter 坐标、`reliable-event.rocketmq.*` 配置键及原有消息、失败与资源所有权语义保持兼容。

`reliable-event.transport` 支持 `rocketmq` 和 `custom`。只有 RocketMQ 一个内置适配器时，未显式配置继续选 RocketMQ；用户提供公共 `EventTransport` 或旧 `EventSender` 时默认退让至 custom，不创建默认 Broker 客户端。选择及注册在客户端和发布运行时装配前完成；新旧用户入口冲突、重复或多个可用 SPI、未知 transport、未安装适配器却显式选择，以及多个注册适配器无法唯一选择时均启动失败。`EventTransportAdapter` 描述契约提供 `transportName()`、单段 `configurationNamespace()` 和 `runtimeBeanNames()`，后者声明该内置适配器运行时拥有的 Bean 名称，以区分框架组件和用户 SPI。描述注册会拒绝非法/空名称、保留的 `custom` transport 名、公共 scalar 字段冲突及重复 namespace。适配器 namespace 用于严格根属性校验的有限放行；公共拼错字段、namespace 内部拼错字段、环境变量形式错误和动态 mapping 子对象的未知字段仍会拒绝，未安装适配器时 `rocketmq.*` 不会因通用 Map 放行。非选中但已注册 namespace 存在非空设置时只记录 transport 和 namespace，不输出配置值或 credentials；最终选择日志不含敏感参数。

Base Starter 默认启用调度，因此没有 Sender 且没有可选中的 SPI 时启动失败。显式设 `reliable-event.scheduling-enabled=false` 可运行只登记实例并允许无 Sender 启动；同一 Outbox 需要另有发布实例负责消费。Base 加自定义 `EventTransport` 或旧 `EventSender` 时可以保留默认调度。自定义 Sender 对自身 Broker 内部重试和确认等待时间的总预算负责。

本次完整验收在 `target/m9.2-verification/repo` 的隔离源码副本顺序执行，避免原工作区正在运行的示例进程占用其 JAR。副本由工作区 Git 跟踪文件及新增源码、POM 和资源组成，不复制 `.git` 或 `target`；最终 264 个源文件哈希与工作区逐一比较，0 差异。完整 base 与 observability 均为 39 个 XML suite、232 项测试，失败、错误和跳过均为 0。命令在隔离副本根目录运行：

```powershell
mvn -o "-Dmaven.repo.local=C:\Users\20659\.m2\repository" "-DreliableEventTest.rocketmqProxyPort=18081" verify
mvn -o "-Dmaven.repo.local=C:\Users\20659\.m2\repository" "-DreliableEventTest.rocketmqProxyPort=18081" -Pobservability verify
```

测试代理端口通过 `reliableEventTest.rocketmqProxyPort` 传递；它位于产品 `reliable-event` 前缀之外，避免 Spring relaxed binding 将夹具参数当成产品配置。首次误用旧测试属性 `reliableEvent.test.rocketmqProxyPort` 的记录保留在 `target/evidence/m9.2-base-verify-attempt1.log`，该参数被严格根校验正确拒绝，属于测试调用错误，不是产品缺陷。尝试日志也保留在 evidence 目录，不替代下述成功证据。源码快照逐文件结果为 `target/evidence/m9.2-source-snapshot-final-check.txt`，摘要为 `target/evidence/m9.2-base-summary.txt` 与 `target/evidence/m9.2-observability-summary.txt`；完整输出分别为 `target/evidence/m9.2-base-verify.log` 和 `target/evidence/m9.2-observability-verify.log`。全 Reactor 测试源码编译预检记录在 `target/evidence/m9.2-test-compile.log`。

另在 `target/m9.2-verification/consumers` 建立四个独立 Maven 消费项目，依赖从隔离源码构建并安装到本机 Maven 仓库的 Starter JAR 解析，使用 `@EnableAutoConfiguration` 从 JAR 元数据发现自动配置；最终 JAR 哈希及本地安装件比对记录在 `target/evidence/m9.2-artifact-hashes.txt`。旧 Starter 和新 RocketMQ Starter 两个项目分别验证旧、新坐标的适配器装配、自定义 SPI 选择及 RocketMQ 客户端类可见性；二者都选 custom，没有连接真实 Broker。Base custom 项目使用模拟 JDBC Bean 验证自定义 SPI、Worker 与无 RocketMQ 运行时类。Base registration 项目用 Testcontainers MySQL 执行真实事务内登记并查询到一条 Outbox 记录，确认无 RocketMQ 类；它同样没有 Broker。四场景合计 4 项测试、失败/错误/跳过均为 0，日志为 `target/evidence/m9.2-external-consumers.log`，依赖树为 `target/evidence/m9.2-consumer-dependency-trees.log`。依赖树显示旧及新 RocketMQ Starter 包含 `rocketmq-client-java:5.2.1`，base custom 依赖树没有 RocketMQ 构件或 Starter。

本阶段没有实现 Kafka 或 RabbitMQ 适配器，也没有创建空的相关模块或 Starter；没有提交、推送或发布构件。以上验收只记录 M9.2 范围，不代表 M9.3、M9.4、M9.5 或 M8.6 完成。外部验证期间曾有直接手工导入自动配置类的初版 fixture 失败；最终消费项目改为由 `@EnableAutoConfiguration` 发现安装 JAR 元数据后四项全部通过，该初版失败不作为产品故障或通过证据。

## 18. M9.2 后续结构深化与当前验收

用户随后决定删除旧 `reliable-event-spring-boot-starter` 模块和坐标，只保留 `reliable-event-spring-boot-starter-base` 与已实现中间件的专用 Starter。RocketMQ 入口统一命名为 `reliable-event-rocketmq-spring-boot-starter`；root、example、benchmark、当前接入文档和发布模块清单均已更新。第 17 节保存的是此前旧 Starter 仍存在时的历史验收，不代表当前还构建或兼容该坐标。

协议实现现位于不含 Spring 的 `reliable-event-rocketmq`：默认路径直接从 `OutboundEvent` 构造 RocketMQ 消息，并返回公共 `TransportReceipt` 或抛出公共 `TransportException`，不再经 `StoredEvent` 和旧 Sender 绕行。RocketMQ auto-configuration 负责选择、属性和 Bean 装配以及默认 Producer 生命周期；保留的 `RocketMqEventSender` 是旧 `EventSender` API 的薄兼容包装，负责边界上的异常转换、历史 JSON Headers 解析与旧输入验证。旧 core SPI 中引入但未发布的 `EventTransportAdapter` 已移除；Spring 装配描述契约 `dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor`（`transportName()`、单段 `configurationNamespace()`、`runtimeBeanNames()`）归公共 auto-configuration 所有。公共 `EventTransport`、`OutboundEvent`、Receipt 和 Failure 契约保持不变。Base Starter 保持纯聚合；“调度开启但无 sender 则启动失败”的检查由公共自动配置登记。

本轮在 `target/m9.2-followup-verification/repo` 冻结隔离源码副本，未复制 `.git` 或 Maven `target`。副本包含的 263 个 Git 跟踪或未忽略源文件逐一 SHA-256 与工作区一致，POM/`src` 子集为 162 个文件；清单见 `target/evidence/m9.2-followup-source-snapshot-final-check.txt`。使用 Java 17 与 Maven 3.9.9 的全 Reactor `-DskipTests test-compile` 预检通过，10/10 模块成功，日志为 `target/evidence/m9.2-followup-test-compile-final.log`。完整 base 与 observability 在该副本依次运行，均为 40 个 Surefire XML suite、239 项测试、0 失败、0 错误、0 跳过。执行命令为：

```powershell
mvn -o "-Dmaven.repo.local=C:\Users\20659\.m2\repository" "-DreliableEventTest.rocketmqProxyPort=18081" verify
mvn -o "-Dmaven.repo.local=C:\Users\20659\.m2\repository" "-DreliableEventTest.rocketmqProxyPort=18081" -Pobservability verify
```

base 与 observability 输出分别保存在 `target/evidence/m9.2-followup-base-verify.log` 和 `target/evidence/m9.2-followup-observability-verify.log`；各自 XML 已复制至 `target/evidence/m9.2-followup-base-surefire-reports` 和 `target/evidence/m9.2-followup-observability-surefire-reports`，摘要分别为 `target/evidence/m9.2-followup-base-summary.txt` 和 `target/evidence/m9.2-followup-observability-summary.txt`。测试使用独立系统属性 `reliableEventTest.rocketmqProxyPort=18081`，不占用用户运行中的 8081 broker。先前带错前缀的测试夹具参数日志仍单独留存，严格绑定拒绝它是调用错误，不属于产品失败证据。

隔离副本构建的 7 个 Maven 库工件通过本地 `install` 安装，仅用于仓库外消费验证；`target/evidence/m9.2-followup-install.log` 记录命令结果，`target/evidence/m9.2-followup-artifact-hashes.txt` 记录安装 JAR 与副本产物逐个 SHA-256 相同。四个独立 Maven consumer 位于 `target/m9.2-verification/consumers`，通过 `@EnableAutoConfiguration` 元数据导入 Starter 自动配置：`rocket-starter-custom` 验证 Rocket 专用 Starter 仍允许单一自定义 SPI；`rocket-starter-selection` 注入 mock Producer 并验证默认最终选择与 Worker 使用的确为协议模块 `RocketMqEventTransport`；`base-custom` 在无 RocketMQ 类的 classpath 验证 custom SPI 与 Worker；`base-registration` 在无 RocketMQ 类的 classpath 使用 Testcontainers MySQL 执行事务内登记并确认一条 Outbox 行。四项 consumer 测试均通过，无失败、错误或跳过。前三个场景使用 mock JDBC 或 mock Producer，不连接 Broker；仅 Base 登记场景启动独立真实 MySQL 容器，因此这不是四项真实 Broker 集成测试。运行日志为 `target/evidence/m9.2-followup-external-consumers.log`，公共模块依赖树为 `target/evidence/m9.2-followup-external-dependency-trees.log`，RocketMQ 客户端依赖证据为 `target/evidence/m9.2-followup-rocketmq-client-dependency-tree.log`。两份树共同显示 Rocket 专用 Starter 经 Rocket auto-configuration 和协议模块传递 `rocketmq-client-java`，两个 Base consumer 则只引入公共 auto-configuration、core 与 JDBC。

在 M9.2 深化验收当时，没有提交、推送或发布工件，也没有停止用户服务、原有七个容器或示例 Java 进程；截至该阶段，Kafka、RabbitMQ、其他存储引擎及 M9.3 以后工作均未实现。Kafka 随后由用户选定并进入 M9.3，当前状态及证据见第 19 节。

## 19. M9.3 Kafka 实现边界与验收记录

本节记录 M9.2 深化后启动并已完成验收的 Kafka 实现。本节证据覆盖协议、自动配置、真实 Broker、Outbox 状态、消费去重和最终安装件消费。公共 SPI 仍保持在 `reliable-event-core`；新增 `reliable-event-kafka` 协议工件（不含 Spring）、`reliable-event-kafka-spring-boot-autoconfigure` 和纯依赖 `reliable-event-kafka-spring-boot-starter`，并新增独立 `reliable-event-kafka-example`。Kafka Starter 引用 Base 与 Kafka auto；Base 与公共模块没有 Kafka 客户端生产依赖。旧 generic Starter 不恢复。

Kafka 3.9.2 由 Spring Boot 3.5.16 BOM 管理，测试镜像为 `apache/kafka:3.9.2`，Testcontainers 1.21.4。适配器以 event key 作 Kafka record key，将数据库原始 JSON 编码为 UTF-8 `byte[]`，传递用户 Headers 与 `reliable_event_id/type/key`，返回合法 topic/partition/offset 诊断元数据且不伪造 Broker Message ID。生产适配不创建 Topic。默认 Producer 配置 `acks=all`、幂等、受限的 max-in-flight 和 Kafka 自动重试；发送截止时间同时覆盖同步 `send()` 阻塞和 Future 等待，超时或不明确的发送后异常归为结果未知，不允许迟到结果更新已结束的 Outbox attempt。Kafka `max.request.size` 预留 body 上限与 32 KiB 记录/header 开销，Broker/topic `max.message.bytes` 仍由部署负责。

默认发送预算为 `max.block=1s + delivery.timeout=5s + confirmation slack=1s`；租约严格大于 7 秒发送预算加 1 秒状态更新预留。使用唯一 `Producer<String, byte[]>` 自定义 Bean 时必须声明 `custom-producer-send-budget`；应用负责实际的 String/byte[] 序列化、`acks=all`、幂等、非事务语义、完整调用预算与资源关闭。框架不读取或声称验证用户 Producer 的实际 Kafka 配置，不增加不可验证的确认布尔开关。双内置适配器必须显式选择 `rocketmq` 或 `kafka`；只安装 Kafka Starter 时可选择唯一适配器；custom sender 保持退让。

聚焦验证已完成：协议单元测试 7 项通过（包括同步 SASL、异步 SSL 认证异常的永久错误分类与诊断脱敏），自动配置单元测试 11 项通过；真实 Kafka 协议 Broker 测试 2 项通过。示例 MySQL+Kafka E2E 为 6 项通过，覆盖真实发布与消费、同一 EventId 重投、测试注入的首条 Broker 成功后确认丢失、暂停本测试 Kafka 容器后的 `RETRY_WAIT` 与恢复 `PUBLISHED`、Topic 创建恢复、真实 Broker 超大消息拒绝进入 `DEAD`、业务事务失败后的 offset 重放与身份去重。首条确认丢失由测试在 Broker 已接收记录后注入，不代表网络层真实丢 ACK；Broker 中断用例暂停且仅暂停本测试容器。日志分别为 `target/evidence/m9.3-kafka-protocol-unit.log`、`target/evidence/m9.3-kafka-autoconfiguration-unit.log`、`target/evidence/m9.3-kafka-broker-protocol.log` 和 `target/evidence/m9.3-kafka-example-e2e-rerun.log`；首轮测试辅助时钟比较失败的日志另存为 `target/evidence/m9.3-kafka-example-e2e.log`，不作为通过证据。

最终验收在排除 `.git` 和所有 `target` 的隔离源码副本 `target/m9.3-verification/repo` 顺序完成。Base 和 `observability` 两次完整 14 模块 `verify` 均为 45 suites、266 tests、0 failure、0 error、0 skipped；日志为 `target/evidence/m9.3-base-verify.log` 与 `target/evidence/m9.3-observability-verify.log`，独立报告和摘要位于同名前缀的 `m9.3-base-surefire-reports`、`m9.3-observability-surefire-reports`、`m9.3-base-summary.txt`、`m9.3-observability-summary.txt`。两次命令均使用 Java 17、Maven 3.9.9 和 `-DreliableEventTest.rocketmqProxyPort=18081`，顺序执行且未停止现有 RocketMQ 服务。


仓库外消费者验证基于上述本地安装的最终 snapshot 工件，使用标准 `@EnableAutoConfiguration` 导入 Starter metadata。Kafka-only 外部应用 3 项通过，依赖树含 Kafka Starter/Base/protocol/client 且无 RocketMQ artifact；dual-broker 外部应用 5 项通过，验证双 Starter 必须显式选择并包含两个真实客户端依赖。两套测试均 0 failure/error/skipped；这是安装件自动配置/依赖边界验证，Producer 用测试提供的实例，不替代前述真实 Broker E2E。依赖树、测试与工件哈希证据分别见 `target/evidence/m9.3-external-kafka-only-dependency-tree.log`、`m9.3-external-dual-broker-dependency-tree.log`、`m9.3-external-kafka-only-test.log`、`m9.3-external-dual-broker-test.log`、`m9.3-external-final-summary.txt` 和 `m9.3-external-final-snapshot-installed-hash-compare.txt`。10 个库 JAR 均与最终 snapshot 构件 SHA-256 相同，列表为 `target/evidence/m9.3-installed-library-jar-sha256.txt`。

独立 Compose/HTTP 烟测使用 Kafka 示例自有 MySQL 端口 13307、Kafka 19092 和 HTTP 8082；实际事务登记并发布后查询到 Outbox `PUBLISHED`、`handledCount=1`、身份去重行 1 和业务效果 1。Compose 配置校验与烟测日志归档为 `target/evidence/m9.3-kafka-compose-http-smoke.log`。最终源码快照含 1,177 个非 `.git`/`target` 文件（包含文档、配置和示例），逐文件 SHA-256 无差异、无额外文件；其中用于代码审查的 Maven POM 与 `src` 文件为 190 个。完整比较清单见 `target/evidence/m9.3-source-snapshot-final-check.txt`。完整 verify 与本地 install 均仅操作隔离快照及本地 Maven 仓库；没有提交、推送或发布。测试期间保留用户原有 7 个容器和 Java 示例 PID 27372；仅关闭本阶段创建的 Testcontainers 与 Kafka 示例自己的 PID/Compose 服务。M9.4 广泛故障审计、M9.5 正式发布及 M8.6 仍待后续实施。