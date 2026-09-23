# ReliableEvent 竞品与替代方案分析

> 调研日期：2026-09-22  
> 调研范围：仅使用项目自身文档，以及各方案的官方文档、官方代码仓库  
> 结论适用边界：以 ReliableEvent 当前完成的 M2 和已冻结的 `0.1.0` 方向为准

## 1. 结论摘要

ReliableEvent 所解决的“业务数据与待发送消息无法共享原子提交”问题并不新，市场上已经存在多种 Transactional Outbox 实现和替代机制。这证明问题真实存在，但也意味着项目不应以“发明 Outbox”或“市场唯一”为定位。

从与 ReliableEvent 的接近程度看：

1. **`gruelbox/transaction-outbox` 是最直接的代码级同类参考**：它也是嵌入 Java 应用、在业务事务中持久化工作、由后台过程重试执行的库，但它以“序列化方法调用”为核心抽象，并支持更多数据库和框架。[官方仓库](https://github.com/gruelbox/transaction-outbox)
2. **Spring Modulith Event Publication Registry 是最接近的 Spring 生态参考**：它在原业务事务内登记监听器事件发布，提供持久化、失败状态、陈旧任务识别和重提交 API；但其核心对象是 Spring 应用事件到监听器的发布记录，官方的 Broker 外部化模型也不是专门面向 RocketMQ 的 Outbox Starter。[官方文档](https://docs.spring.io/spring-modulith/reference/events.html)
3. **Eventuate Tram 和 Debezium Outbox Event Router 是相邻方案**：两者都采用 Outbox，但把 Relay/CDC 做成独立基础设施，运维边界和产品范围明显大于 ReliableEvent。[Eventuate Tram 官方仓库](https://github.com/eventuate-tram/eventuate-tram-core)；[Debezium 官方文档](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
4. **Apache RocketMQ 事务消息是替代机制，不是 Outbox 同类库**：它通过 Half Message、二阶段提交/回滚和 Broker 发起的事务状态回查协调本地事务与消息可见性。[官方文档](https://rocketmq.apache.org/docs/featureBehavior/04transactionmessage/)

因此，ReliableEvent 的合理定位是：

> 面向 Spring Boot 3、MySQL 8.0 和 RocketMQ 的轻量 Transactional Outbox Starter；以明确的状态机、MySQL 8.0 多实例条件抢占、租约恢复和可复现故障测试证明至少一次投递行为。

这是一条有实际区分度的窄路线，但不是“没有同类产品”的原创类别。真正的项目价值来自约束下的设计选择、完成度和证据，而不是模式名称。

## 2. 目的与非目标

### 2.1 目的

本文用于：

- 确认 ReliableEvent 与成熟方案的关系，避免重复造轮子却不知道已有实践；
- 从一手资料中提取可用于 M3 及后续阶段的设计经验；
- 明确可以借鉴的思想、需要独立设计的部分以及不应扩入 `0.1.0` 的能力；
- 为 README、架构文档和面试说明提供可核验的方案选择依据。

### 2.2 非目标

本文不用于：

- 宣称 ReliableEvent 比所有成熟方案更好；
- 用功能清单代替基准测试、故障测试或生产验证；
- 复制其他项目的源码、表结构、API 命名或文档措辞；
- 改变 [PROJECT_DIRECTION.md](PROJECT_DIRECTION.md) 已冻结的 `0.1.0` 范围；
- 为尚未实现的 RocketMQ、租约恢复、Starter 或指标能力背书。

## 3. ReliableEvent 当前基线

对比竞品前，必须区分“已经实现”和“路线图计划”。[README](../README.md)、[HANDOFF](HANDOFF.md) 与 [M2.4 完成记录](progress/M2_4_COMPLETED.md) 的当前结论如下。

### 3.1 已实现并有测试覆盖

- 在活动的 Spring 数据库事务中登记事件，业务回滚时事件也回滚；
- JSON Payload 与字符串 Header 持久化；
- 使用 `event_type + event_key` 防止重复登记；
- 按 `next_attempt_at` 过滤尚未到期的首次事件和重试事件；
- 使用候选版本与条件更新完成 MySQL 8.0 多 Worker 抢占；
- 将抢占、外部发送和结果更新拆为短事务，Fake Sender 调用发生在数据库事务外；
- 实现 `PENDING → PUBLISHING → PUBLISHED / RETRY_WAIT / DEAD` 状态流转；
- 实现带抖动的指数退避、最大总尝试次数和显式不可重试错误；
- 使用真实 MySQL 8.0 Testcontainers 验证双 Worker 竞争，同一候选版本只有一个 Worker 调用 Sender；
- M2 完成时共有 10 个单元测试和 17 个 MySQL 集成测试。

上述“唯一执行权”只覆盖当前抢占竞争，不等于消息只会发送一次，也不覆盖抢占成功后进程退出的恢复窗口。

### 3.2 已规划但尚未实现

- M3：`lease_owner`、`lease_until`、租约所有权校验、过期租约恢复和宕机故障注入；
- M4：真实 RocketMQ Sender、Topic/Tag 映射、后台调度、有界并行执行、Spring Boot 自动配置、优雅停机和 Micrometer 指标；
- M5：原创示例、私有业务接入、基准测试和 `0.1.0` 发布检查；
- `DEAD` 事件的人工重放接口仍被暂缓到后续版本评估。

当前表中已经预留租约字段，但字段存在不代表租约行为已经完成。当前 Sender 仍是测试用 Fake 适配，因此不能把项目描述为已经接入 RocketMQ 或已经具备宕机恢复。

### 3.3 明确不进入 `0.1.0`

根据 [PROJECT_DIRECTION.md](PROJECT_DIRECTION.md)，第一版不做 Exactly Once、消息顺序保证、CDC/Binlog 发布、多数据库、多数据源、多消息中间件、管理后台、通用任务编排或分布式事务协调器。

## 4. 对比维度

本文按以下维度比较，而不是只比较“有没有 Outbox 表”：

1. **原子登记方式**：事件如何与业务数据进入同一本地事务；
2. **发布架构**：应用内轮询、外部 Relay、CDC，还是 Broker 协调；
3. **并发与宕机恢复**：如何取得执行权、识别卡住任务和恢复；
4. **失败治理**：重试、退避、失败终态、告警和人工重放；
5. **投递语义**：是否承认重复窗口，以及消费者如何去重；
6. **编程模型**：事件信封、Spring 应用事件、方法调用、命令/事件平台；
7. **基础设施边界**：需要哪些数据库、Broker、Connector 或独立服务；
8. **可观测性与可测试性**：状态是否可查，是否提供指标、测试夹具或运维 API；
9. **与当前技术栈的贴合度**：Spring Boot 3、MySQL 8.0、RocketMQ。

## 5. 总览

| 方案 | 类型 | 发布路径 | 主要抽象 | 官方列出的 Broker/目标 | 与 ReliableEvent 的关系 |
| --- | --- | --- | --- | --- | --- |
| ReliableEvent（当前 M2） | 应用内 Outbox 库 | MySQL 候选扫描与条件抢占，当前仅 Fake Sender | `ReliableEvent` 事件信封 | RocketMQ 为 M4 目标，尚未实现 | 本项目 |
| `gruelbox/transaction-outbox` | Java 应用内 Outbox 库 | 提交后执行，并通过 `flush()` 处理陈旧工作 | 被序列化的方法调用 | 目标由被调用方法决定 | 最直接的通用型同类 |
| Spring Modulith Event Publication Registry | Spring 事务事件发布日志 | 事务监听器、持久化 Registry、失败/陈旧发布重提交 | 应用事件到监听器的发布记录 | 官方外部化表列出 Kafka、AMQP、JMS、Spring Messaging | Spring 生态近邻 |
| Eventuate Tram | 事务消息平台 | 独立 Eventuate CDC Service，Binlog/WAL 或轮询 | 消息、领域事件、命令 | Kafka、ActiveMQ、RabbitMQ、Redis Streams | 更重、更广的相邻平台 |
| Debezium Outbox Event Router | CDC Outbox 路线 | Debezium Connector 捕获 Outbox 变更并由 SMT 路由 | Insert-only Outbox 事件 | Kafka Connect 记录/Topic | 架构替代路线 |
| RocketMQ 事务消息 | Broker 事务消息 | Half Message、本地事务、Commit/Rollback、事务回查 | Broker 事务消息 | RocketMQ | 非 Outbox 的直接替代机制 |

表中第三方能力来自各自的[官方仓库或文档](#11-来源清单)。“未列出 RocketMQ”只表示本文查阅的官方支持矩阵未列出一等适配，不等价于技术上绝对无法扩展。

## 6. 逐项分析

### 6.1 `gruelbox/transaction-outbox`

#### 工作方式

该项目把 Java 方法调用通过代理序列化，并在业务数据库事务中写入 Outbox；调用方使用 `outbox.schedule(SomeClass.class).someMethod(...)` 登记工作。官方 README 说明它支持 Spring DI/Spring Transaction、Guice、jOOQ，以及 MySQL 5/8、PostgreSQL、Oracle、SQL Server 和 H2。[官方 README：定位与支持范围](https://github.com/gruelbox/transaction-outbox#transaction-outbox)；[官方 README：基本配置](https://github.com/gruelbox/transaction-outbox#basic-configuration)

失败工作可由后台线程反复调用 `TransactionOutbox.flush()` 重新处理；达到阈值后进入 blocked 状态，可通过监听器告警，并由 `unblock()` 重新启用。[后台 Worker](https://github.com/gruelbox/transaction-outbox#set-up-the-background-worker)；[Dead Letter 管理](https://github.com/gruelbox/transaction-outbox#managing-the-dead-letter-queue)

其官方文档还明确指出：`flush()` 在支持 `SKIP LOCKED` 的数据库上适合并发调用，但 MySQL 5.7 不支持该能力，多实例并发 `flush()` 可能产生锁超时和日志噪声，因此可能需要限制为单实例运行。[并发说明](https://github.com/gruelbox/transaction-outbox#set-up-the-background-worker)

#### 相同点

- 都把待执行工作与业务修改写入同一数据库事务；
- 都在提交后执行外部副作用，并对失败工作重试；
- 都需要承认重试可能导致重复副作用；
- 都考虑延迟执行、失败封锁/死信和多实例处理。

#### 差异

- 它的核心抽象是“序列化并重放方法调用”，ReliableEvent 的核心抽象是稳定的事件信封和 RocketMQ 发布；
- 它面向多框架、多数据库和任意副作用，ReliableEvent `0.1.0` 有意收窄到 Spring Boot 3、MySQL 8.0、RocketMQ；
- 它在 MySQL 5.7 上建议规避并发 `flush()`；ReliableEvent 的当前基线是 MySQL 8.0，但仍选择 `id + status + next_attempt_at + version` 条件更新，并用双 Worker 测试证明同一候选版本的唯一执行权；
- 它支持命名 Topic 的 FIFO 处理、嵌套 Outbox、方法参数序列化扩展等能力，而 ReliableEvent 第一版明确不保证顺序，也不做通用后台任务系统。[顺序能力说明](https://github.com/gruelbox/transaction-outbox#topics-and-fifo-ordering)

#### 可借鉴

- blocked/dead 事件的监听器通知与人工解除流程；
- 对工作队列饱和、临时失败、最终阻塞使用不同日志和告警语义；
- 通过唯一请求 ID 做入口去重，并设置去重记录保留期；
- 把序列化安全作为显式设计问题，避免对不可信多态数据进行无限制反序列化；
- 为时钟、事务管理、持久化和执行器提供可测试替身。[测试替身说明](https://github.com/gruelbox/transaction-outbox#stubbing-in-tests)

#### 不应照搬

- 不应把 `ReliableEventPublisher.publish(event)` 改成动态代理方法调用 API；这会模糊“消息发布”边界并扩大序列化攻击面；
- 不应为了功能对齐而在 `0.1.0` 增加 FIFO Topic、嵌套任务或任意远程调用；
- 不应只因 MySQL 8.0 提供 `SKIP LOCKED` 就立即替换已经验证的条件更新协议；切换前必须比较吞吐、锁等待、事务边界和故障语义；
- 不应复制其表结构、配置命名或源码实现。即使许可证允许复用，任何代码级复用也必须单独审查许可证、保留声明并记录来源。

### 6.2 Spring Modulith Event Publication Registry

#### 工作方式

Spring Modulith 在发布应用事件时识别事务事件监听器，并在原始业务事务中为每个目标监听器写入发布记录；监听器成功后记录完成，失败时记录保持未完成，以便后续重试。[Event Publication Registry](https://docs.spring.io/spring-modulith/reference/events.html#events.event-publication-registry)

当前官方文档描述了 `PUBLISHED`、`PROCESSING`、`COMPLETED`、`FAILED`、`RESUBMITTED` 生命周期，并提供 Staleness Monitor，把长时间停留在处理中或等待中的发布标记为失败；重提交选项支持批量大小、最大在途数、最小年龄和过滤条件。[生命周期与陈旧检测](https://docs.spring.io/spring-modulith/reference/events.html#events.event-publication-registry.lifecycle)

官方还提供已完成、未完成和失败发布的管理 API，可执行重提交和清理；文档提醒，如果不清理已完成记录，持久化表会持续增长。[管理 Event Publications](https://docs.spring.io/spring-modulith/reference/events.html#events.event-publication-registry.management)

对于 Broker 外部化，官方支持表列出 Kafka、AMQP、JMS 和 Spring Messaging，没有列出 RocketMQ 一等适配。当前文档同时明确提醒：原生基于异步事务监听器的外部化是实用但简单的方案，缺少真正 Outbox 实现通常需要的关键能力；Spring Modulith 2.1 因此增加了委托给 Namastack Outbox 或 JobRunr 的模式。[外部化与支持基础设施](https://docs.spring.io/spring-modulith/reference/events.html#events.externalization)

#### 相同点

- 都在业务事务内留下持久化发布记录；
- 都把“记录已经提交”和“外部处理已经成功”分开；
- 都需要处理失败、重提交、陈旧/卡住记录以及历史清理；
- 都属于 Spring 应用内可复用基础能力。

#### 差异

- Spring Modulith Registry 的记录单位是“事件到某个监听器的发布”，ReliableEvent 的记录单位是“一个待发送到 RocketMQ 的事件”；
- Spring Modulith 与应用模块事件和 `@TransactionalEventListener` 深度结合，ReliableEvent 公开的是独立、小型 `ReliableEventPublisher` API；
- Spring Modulith 的官方外部化支持矩阵未列 RocketMQ，ReliableEvent 则把 RocketMQ 作为唯一首版生产适配；
- ReliableEvent 当前已显式实现退避、最大尝试次数和 `DEAD`，但尚未实现 Spring Modulith 现有的陈旧处理、批量重提交和清理管理能力。

#### 可借鉴

- 将“正在处理但已陈旧”和“明确发送失败”分开建模；
- 恢复或重提交必须支持批量上限、最大在途数和最小年龄，防止恢复风暴；
- 为已完成事件提供按时间清理策略，为失败事件提供只读查询和受控操作；
- 文档明确区分内部应用事件、外部消息发布和完整 Outbox 能力，不把它们混为一谈。

#### 不应照搬

- 不应把 ReliableEvent 强绑定到 Spring Modulith 的模块模型或注解扫描；
- 不应让一个业务事件因监听器数量隐式展开为多条发布记录，除非未来明确支持该语义；
- 不应为了复用 Registry 而引入与 RocketMQ 无关的 Broker 外部化模块；
- 不应把当前 M2 的 `PUBLISHING` 直接改名套用 Spring Modulith 状态，状态名称相似不代表事务边界和恢复规则相同。

### 6.3 Eventuate Tram

#### 工作方式

Eventuate Tram 是事务消息平台，提供消息、领域事件和异步命令等抽象。生产端在更新业务数据的 ACID 事务中把消息写入 `OUTBOX` 表，独立的 Eventuate CDC Service 再发布到 Broker。[官方仓库：How it works](https://github.com/eventuate-tram/eventuate-tram-core#how-it-works)

其 Relay 可以对 MySQL 使用 Binlog、对 PostgreSQL 使用 WAL，也可以对其他数据库使用轮询；官方列出的 Broker 包括 Kafka、ActiveMQ、RabbitMQ 和 Redis Streams。[官方支持范围](https://github.com/eventuate-tram/eventuate-tram-core#supported-technologies) Eventuate CDC 是独立 Spring Boot 服务，并涉及 Reader、Pipeline、Offset Store、Publisher，以及集群部署下的领导者选举。[CDC 配置文档](https://eventuate.io/docs/manual/eventuate-tram/latest/cdc-configuration.html)

#### 相同点

- 都使用本地数据库事务原子写入业务数据和待发布消息；
- 都把真正的 Broker 发布放在业务事务之后；
- 都需要稳定消息 ID、消费者幂等、失败恢复和积压观测；
- 都可服务于微服务间可靠消息传递。

#### 差异

- Eventuate Tram 是包含生产、消费、领域事件、命令及 Saga/CQRS 周边的较大平台，ReliableEvent 只解决生产端 Outbox 发布；
- Eventuate 通常需要独立 CDC Service 和相关基础设施，ReliableEvent 计划作为应用内 Starter 运行；
- Eventuate 在 MySQL 上优先使用 Binlog Relay，ReliableEvent 明确不使用 CDC，而是应用内扫描与条件抢占；
- Eventuate 官方支持列表未列 RocketMQ，ReliableEvent 的目标 Broker 是 RocketMQ。

#### 可借鉴

- 清晰拆分“事务内写入”和“事务外 Relay”两个责任边界；
- 为 Relay 定义健康状态、积压/延迟、发布数、重试数和重复数等运维信号；Eventuate CDC 官方文档列出了连接状态、复制延迟、事件年龄、发布数、重复数和重试数等指标。[CDC 指标](https://eventuate.io/docs/manual/eventuate-tram/latest/cdc-configuration.html#monitoring-the-cdc-service)
- 在示例和测试工具中同时展示生产者幂等键和消费者去重，而不是只展示发送成功；
- 将消息信封、目的地路由和 Broker 适配分层，避免 JDBC 代码直接依赖 RocketMQ 客户端类型。

#### 不应照搬

- 不应把 Saga、CQRS、异步命令/回复和消费者框架加入 `0.1.0`；
- 不应为追求“架构完整”而新增独立 CDC 服务、Offset Store 或领导者选举；
- 不应在当前阶段扩展 Kafka、RabbitMQ、ActiveMQ 等多 Broker 适配；
- 不应使用 Eventuate 的术语和公共 API 冒充本项目原创设计。

### 6.4 Debezium Outbox Event Router

#### 工作方式

Debezium 路线要求 Connector 捕获 Outbox 表变更，并通过 `EventRouter` Single Message Transformation 将变更转换、路由到消息 Topic；官方建议应用 Outbox SMT 的 Connector 只捕获 Outbox 表。[官方文档：Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)

默认模型围绕事件 ID、聚合类型、聚合 ID、事件类型和 Payload；事件 ID 会放入消息 Header，可用于消费者去重，聚合 ID 可作为消息 Key。[默认 Outbox 表](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html#basic-outbox-table)

该表在官方模型中是 Insert-only Queue：更新操作被视为异常，可配置为 warning、error 或 fatal，删除变更会被 SMT 过滤。[配置项](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html#configuration-options)

#### 相同点

- 都要求业务数据和 Outbox 事件在同一本地事务中写入；
- 都需要稳定事件 ID/Key、Payload、类型和路由信息；
- 都承认下游需要利用事件 ID 做重复检测；
- 都把外部发送与业务请求解耦。

#### 差异

- Debezium 通过数据库日志捕获和 Connector/SMT 发布，ReliableEvent 通过应用内查询、条件抢占和 Sender 发布；
- Debezium 的默认 Outbox 表强调插入后不更新，ReliableEvent 依赖同一行的状态、版本、尝试次数、租约和错误摘要更新；
- Debezium 的重试与可用性主要落在 Connector、Kafka Connect 和下游平台运维上，ReliableEvent 计划把每个事件的发布生命周期保存在业务数据库中；
- Debezium 路线需要额外 Connector/Connect 运维边界，ReliableEvent 的目标是一个可嵌入 Starter；
- ReliableEvent 第一版明确不做 CDC/Binlog。

#### 可借鉴

- 规范事件信封：稳定事件 ID、业务聚合/事件 Key、事件类型、发生时间、Payload 和可选 Header；
- 将事件 ID 传到 RocketMQ 消息属性，明确要求消费者以此去重；
- 在文档中单列 JSON/Avro 等 Schema 演进问题，不把 Java 类序列化结果直接当作长期协议；
- 将“事件内容”和“路由配置”分开，避免业务 Payload 携带基础设施目的地。

#### 不应照搬

- 不应在 `0.1.0` 引入 Binlog、Debezium Connector 或 Kafka Connect；
- 不应照搬 Insert-only 表模型，因为当前状态机、租约和人工诊断都依赖行更新；
- 不应照搬面向 Kafka Partition 的顺序承诺；项目第一版明确不保证顺序；
- 不应仅因为 CDC 能避免轮询就声称其一定更快或更可靠，二者需要在相同环境下基准和故障测试后才能比较。

### 6.5 Apache RocketMQ 事务消息

#### 工作方式

RocketMQ 事务消息先向 Broker 写入对消费者不可见的 Half Message，再执行本地事务，并根据结果 Commit 或 Rollback。若 Broker 没收到第二次确认或状态为 Unknown，Broker 会向 Producer 集群发起事务状态回查；Producer 查询本地事务结果后再次提交状态。[官方工作机制](https://rocketmq.apache.org/docs/featureBehavior/04transactionmessage/#working-mechanism)

官方示例要求 Producer 配置 Transaction Checker，并根据业务标识查询本地事务是否提交；官方也提醒，应避免大量 Half Message 超时或长期返回 Unknown，因为频繁回查会降低性能并延迟事务处理。[官方示例与注意事项](https://rocketmq.apache.org/docs/featureBehavior/04transactionmessage/#example)

#### 相同点

- 都试图解决本地数据库提交与消息可见性之间的一致性窗口；
- 都要求有稳定业务标识，以便判断或去重；
- 都不能把消费者业务完成等同于生产端发送成功；
- 异常窗口下都需要补偿逻辑和清晰的运维观测。

#### 差异

- RocketMQ 事务消息由 Broker 保存 Half Message 并主动回查生产者；ReliableEvent 由数据库 Outbox 保存事实，并由应用 Worker 主动扫描；
- RocketMQ 事务消息要求业务方实现可靠的本地事务状态查询，ReliableEvent 通过 Outbox 行自身表达发布生命周期；
- RocketMQ 事务消息的恢复控制面在 Broker 与 Producer 回查协议，ReliableEvent 的恢复控制面在数据库状态、租约和重试调度；
- RocketMQ 事务消息与 RocketMQ 强绑定，ReliableEvent 同样选择 RocketMQ，但把 Broker 交互隐藏在 Sender 适配之后。

#### 可借鉴

- 每条事件都必须携带能够关联本地业务事实的稳定 Key；
- 文档必须说明“发送结果未知”的窗口，而不是把超时简单解释为 Broker 未接收；
- 指标应区分发送失败、结果未知、重试、租约恢复和最终死信；
- 集成测试应验证 Broker 不可用、发送超时和 Producer 重启等情况。

#### 不应照搬

- 不应默认在 Outbox Worker 内再发送 RocketMQ 事务消息；这会叠加两套状态机和恢复协议，却不能自动获得 Exactly Once；
- 不应把 Transaction Checker 接口混入 `ReliableEventPublisher` 公共 API；
- 不应笼统声称 Outbox 一定优于事务消息。选择取决于团队是否希望由数据库状态驱动恢复，还是接受 Broker Half Message 与回查协议；
- 不应以 RocketMQ 返回成功为消费者完成业务的证明。

## 7. 可借鉴项与不应照搬项汇总

| 来源 | 可借鉴到 ReliableEvent | 建议阶段 | 不应照搬 |
| --- | --- | --- | --- |
| `transaction-outbox` | blocked/dead 告警、受控 unblock、队列饱和信号、序列化安全、测试替身 | M4/M5；人工重放评估放到 0.2.0 | 方法调用代理、通用任务执行；未经基准对比直接切换到 `SKIP LOCKED` |
| Spring Modulith | 陈旧处理、失败与处理中分离、限流重提交、已完成记录清理 API | M3/M4/M5 | 绑定模块事件模型、按监听器展开、照搬状态命名 |
| Eventuate Tram | Producer/Relay 边界、健康与延迟指标、生产与消费测试样例 | M4/M5 | CDC 服务、Saga/CQRS、命令平台、多 Broker 扩张 |
| Debezium | 稳定事件信封、事件 ID 去重、Schema 演进、路由与 Payload 分离 | M4/M5 | CDC/Binlog、Insert-only 表、Kafka 顺序模型 |
| RocketMQ 事务消息 | 业务 Key、未知结果语义、Broker 异常测试、方案选择文档 | M4/M5 | Outbox 与事务消息默认叠加、回查 API 污染公共接口 |

借鉴应落实为“本项目自己的约束、状态转换、测试和文档”，而不是把成熟项目的功能列表全部搬进来。只要借鉴的是公开设计思想并独立实现、明确引用来源，就不构成项目价值的削弱；相反，它说明设计经过了同类方案校验。

## 8. 当前定位与可陈述边界

### 8.1 当前定位

截至 M2，ReliableEvent 更准确的描述是：

> 一个已经完成事务内事件登记、MySQL 8.0 条件抢占、双 Worker 竞争验证、退避重试和死信终态的 Transactional Outbox 核心原型；目标是演进为 Spring Boot 3 + MySQL 8.0 + RocketMQ Starter。

它目前还不是：

- 已发布或经过生产验证的成熟库；
- 已完成 RocketMQ 集成的 Starter；
- 已解决 Worker 宕机恢复的实现；
- Exactly Once 方案；
- 通用消息平台或 RocketMQ 替代品。

### 8.2 可以形成差异的地方

- **约束明确**：只面向 Spring Boot 3、MySQL 8.0、RocketMQ，不假装首版支持所有组合；
- **MySQL 8.0 并发路径明确**：当前使用候选版本和条件更新竞争执行权，不因数据库升级自动改变并发协议；
- **状态可解释**：首次待发、处理中、重试等待、成功、死信均有持久化状态；
- **测试优先于宣传**：事务、并发、退避和死信已有真实 MySQL 测试，后续继续用故障注入证明崩溃窗口；
- **公共 API 收窄**：业务方只登记事件，不接触租约、重试、线程池或 RocketMQ 客户端细节。

这些都不是单项“行业首创”。项目最终竞争力取决于组合是否实现完整、API 是否易用、失败语义是否准确、证据是否可复现。

## 9. M3 及后续设计建议

### 9.1 M3：租约和宕机恢复

1. **让租约成为抢占令牌的一部分。** 抢占成功后返回不可变令牌，至少包含 `eventId`、`claimVersion`、`leaseOwner` 和 `leaseUntil`。成功、重试和死信更新都同时校验 `id + PUBLISHING + version + leaseOwner`。
2. **区分发送失败与处理陈旧。** Sender 抛错进入现有失败分类；Worker 消失导致的租约过期，应记录独立恢复原因，再进入 `RETRY_WAIT`。不要伪造一个 Broker 失败。
3. **明确时间来源。** 租约判断应避免不同应用实例时钟漂移造成过早回收。可以统一使用数据库时间，或继续使用可注入时钟但增加跨实例时钟偏差测试；选型应写入设计文档。
4. **恢复必须限量。** 借鉴 Spring Modulith 的 batch size、max in-flight、min age 思路，过期租约扫描每轮有上限，避免实例恢复时形成重放风暴。
5. **保留至少一次语义。** “RocketMQ 已接收、数据库尚未标记成功时 Worker 宕机”必然可能重复发送。M3 测试应证明最终可恢复，而不是错误地断言只发送一次。
6. **覆盖两个关键故障窗口。** 抢占后发送前退出；发送成功后状态更新前退出。测试应验证旧 Worker 不能在租约失效后覆盖新 Worker 的状态。

### 9.2 M4：RocketMQ 与 Starter

1. **保持普通消息适配为默认路线。** 将 RocketMQ 事务消息作为替代方案记录在架构决策中，不与 Outbox 默认叠加。
2. **冻结消息信封。** 在写生产适配前确定事件 ID、事件类型、事件 Key、Payload、Header、创建时间及 Schema 版本如何映射到 RocketMQ 消息；消费者去重键必须稳定。
3. **明确结果分类。** 区分明确成功、明确不可重试、明确可重试和结果未知。结果未知应按至少一次原则重试并记录专门指标。
4. **限制本地在途容量。** 每轮抢占数不得长期超过有界线程池可执行容量，避免任务拿到租约后在本地队列中等到过期。
5. **启动时校验配置。** Topic/Tag 映射缺失、消息大小超限或配置非法应在可判断时快速失败；不要在运行中静默降级。
6. **建立核心指标。** 至少包括成功、失败、结果未知、重试、死信、租约恢复、积压、发布延迟、发送耗时和状态更新冲突；日志携带事件 ID、业务 Key、尝试次数、租约 Owner 和 Broker Message ID，但默认不打印完整 Payload。
7. **保留替换接缝。** 业务方可覆盖 Sender 或关键 Bean，但 JDBC 状态机不要暴露为公共插件体系，避免只为“可扩展”制造不稳定 API。

### 9.3 M5：落地与证明

1. **提供面向语义的 Testkit。** 支持等待事件到达指定状态、注入 Sender 成功/失败/阻塞、固定时钟和读取测试事件，而不是要求调用方直接操作表。
2. **补齐历史清理。** 已发布记录按保留期小批量清理；不得删除 `PENDING`、`PUBLISHING`、`RETRY_WAIT` 或 `DEAD` 记录。用执行计划验证不同表规模下的扫描索引。
3. **发布运维手册。** 说明积压、持续重试、死信、租约过期、数据库不可用和 Broker 不可用时如何判断与处置。`0.1.0` 可先提供查询与告警，不必为了对齐竞品提前加入人工重放 API。
4. **做端到端重复验证。** 示例消费者使用事件 ID 实现幂等，并通过故障注入展示重复消息不会重复改变业务结果。
5. **所有性能结论可复现。** 对事件数、并发实例、线程数、批量、轮询间隔、数据库版本和硬件留档；在报告前不使用“高性能”“生产级”等词。

### 9.4 暂缓到 `0.2.0` 再评估

- 受控人工重放与审计；
- 经过基准对比的 `SKIP LOCKED` 替代策略；
- 顺序分组；
- Schema Registry 或 Avro；
- 第二种数据库或消息中间件；
- CDC Relay。

这些能力可以从竞品获得设计启发，但没有真实需求和测试预算时不应进入首版。

## 10. 最终判断

ReliableEvent 已经借鉴了 Transactional Outbox 这一行业模式，也自然使用了状态机、重试、死信和幂等键等成熟思想；从当前代码与文档看，它走的是在 MySQL 8.0 上自行设计和验证版本条件更新协议的实现路线，而不是对某个项目公共 API 或源码的直接复刻。

接下来最值得借鉴的不是更多功能，而是成熟项目对以下问题的处理纪律：

- 卡住任务与明确失败如何区分；
- 重提交如何限流；
- Dead/Blocked 如何告警与审计；
- 历史记录如何清理；
- 事件 ID 和 Schema 如何长期稳定；
- 如何用测试把重复窗口和故障恢复讲清楚。

因此，不建议改变当前技术路线。建议保持窄范围，完成 M3 至 M5，并把“为什么没有选择 Debezium、Eventuate Tram 或 RocketMQ 事务消息”作为架构取舍写进最终文档。这样既承认行业已有方案，也能准确说明 ReliableEvent 的工程贡献。

## 11. 来源清单

### 11.1 ReliableEvent 项目内资料

- [项目 README](../README.md)
- [项目方向文档](PROJECT_DIRECTION.md)
- [当前交接](HANDOFF.md)
- [M2.1 完成记录](progress/M2_1_COMPLETED.md)
- [M2.2 完成记录](progress/M2_2_COMPLETED.md)
- [M2.3 完成记录](progress/M2_3_COMPLETED.md)
- [M2.4 完成记录](progress/M2_4_COMPLETED.md)

### 11.2 外部一手资料

- Gruelbox：[`transaction-outbox` 官方仓库与 README](https://github.com/gruelbox/transaction-outbox)
- Spring Modulith：[Working with Application Events](https://docs.spring.io/spring-modulith/reference/events.html)
- Spring Modulith：[EventPublicationRegistry API](https://docs.spring.io/spring-modulith/docs/current/api/org/springframework/modulith/events/core/EventPublicationRegistry.html)
- Eventuate Tram：[官方核心仓库与 README](https://github.com/eventuate-tram/eventuate-tram-core)
- Eventuate Tram：[About Eventuate Tram](https://eventuate.io/docs/manual/eventuate-tram/latest/about-eventuate-tram.html)
- Eventuate Tram：[Configuring the Eventuate CDC Service](https://eventuate.io/docs/manual/eventuate-tram/latest/cdc-configuration.html)
- Debezium：[Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
- Apache RocketMQ：[Transaction Message](https://rocketmq.apache.org/docs/featureBehavior/04transactionmessage/)
- Apache RocketMQ：[Transactional Message Sending（4.x）](https://rocketmq.apache.org/docs/4.x/producer/06message5/)
- Apache RocketMQ：[官方仓库中的事务消息设计说明](https://github.com/apache/rocketmq/blob/develop/docs/cn/design.md#5-%E4%BA%8B%E5%8A%A1%E6%B6%88%E6%81%AF)
