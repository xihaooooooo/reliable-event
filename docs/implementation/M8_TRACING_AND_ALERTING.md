# M8：自动链路追踪与可直接使用的告警计划

> 状态：M8.1–M8.5 已完成；M8.6 待实施。计划编写日期：2026-10-01；M8.1–M8.2 实施日期：2026-10-01；M8.3–M8.4 实施日期：2026-10-02；M8.5 完成日期：2026-10-03。M8.5 的实现与验收记录见 [完成记录](../progress/M8_5_COMPLETED.md)。
>
> 本计划基于当前 M7 源码和工作区已有的自适应轮询改动。阶段编号为规划编号；不表示功能已经交付，也不改变 M5.4 暂缓正式发布的结论。

## 目标

让接入方能从一个业务请求定位到事件登记、每次发送尝试和示例消费者的处理结果；当发布停滞、到期事件长期积压、扫描失败或监控数据失效时，能够收到有明确处理步骤的告警。

交付结果包括可选的自动追踪集成、补充运行指标、带追踪的原创订单示例，以及可在本地运行的 Prometheus、Grafana、Tempo 和 Alertmanager 示例配置。可靠发布继续使用原事务、抢占、租约、重试和死信协议。

## 当前依据

- `JdbcReliableEventPublisher.publish()` 已在业务事务内序列化 Payload 和 Headers；Headers 随 Outbox 持久化，适合保存追踪上下文。
- `RocketMqMessageFactory` 已把 Headers 转为 RocketMQ 用户属性，并限制属性数量、格式和大小。自动注入必须与这些限制相容。
- `JdbcEventPublicationWorker` 先调用 Sender，再更新数据库状态。当前成功计数代表发送回执，状态更新仍可能失败；追踪与新增指标必须分别表达两件事。
- `PublicationObserver` 主要提供发送完成后的回调，不能单靠回调覆盖发送前的上下文恢复和线程作用域清理。
- 当前自动配置在没有自定义 `PublicationObserver` 时装配 Micrometer 实现。新增追踪不能通过另一个 Observer Bean 意外替换既有指标。
- `MicrometerPublicationObserver` 已记录发送、延迟、积压、死信、租约恢复及清理指标；数据库快照刷新失败会保留旧值。已有最老待清理年龄不等于最老待发布年龄。
- M4.5 约定手动 `runOnce()` 不启动自动采样线程；当前 `JdbcEventPublicationCycle` 在 `finally` 中刷新快照，已有 Starter 测试在调用返回后立即检查 Gauge。新增周期采样必须保留这条手动路径。
- M4.4 约定默认发布运行时按单调时钟截止时间停止，不能无限等待不响应中断的数据库调用。新增采样任务须独立管理资源，不延长该截止时间。
- `ExampleConsumerLoop` 在事务性 `OrderMessageHandler.handle()` 返回后 ACK，适合分别演示业务处理与 ACK 结果。

## 本阶段范围

- 生产端自动提取并持久化 W3C 追踪上下文；后台发布恢复关联，每次有效发送尝试单独记录。
- 首个验收组合为 Spring Boot 3.5、Micrometer Observation / Tracing、OpenTelemetry Bridge、OTLP 和 Tempo；版本由现有 Spring Boot BOM 管理，实施时核对实际解析版本。
- 示例消费者提取上下文，记录业务事务结果、幂等跳过及 ACK 结果。
- 分别观察当前可领取事件与首次已到期但尚未发布完成的事件，补充连续的未完成年龄、本地运行任务、调度健康、数据库快照时效和关键状态更新结果指标。
- 提供看板、告警规则、告警处置说明及可复现故障演练。

本阶段不交付通用消费者框架、消费结果回写、完整事件历史库、自动重放、管理后台或全套日志平台。示例消费者的追踪不等于所有接入方的消费者已经具备追踪；生产端 `PUBLISHED` 仍不代表消费完成。数据库身份、状态和重放审计继续承担事实记录职责。

## 追踪语义与兼容原则

### 登记与上下文保存

1. 开启集成且追踪组件可用时，围绕登记创建短生命周期的 Observation / Span，提取它的上下文，合并到 Headers 后序列化入库。业务请求的当前上下文作为上游；没有上游时允许创建根 Span。
2. 持久化仅包括 `traceparent` 和需要时的 `tracestate`；第一版不自动传播 Baggage，不保存 SDK 对象、Span 实例或线程本地对象。不新增数据库列。
3. 自动生成的上下文优先于调用方 Headers 中同名的追踪字段，并保留其他业务 Header。只修改副本，不修改调用方 Map；合并后仍遵守现有属性预算。自动注入若失败或超出预算，跳过注入并记录受控诊断，不让追踪错误使原本合法的业务事件失败。
4. 重复登记仍返回原 EventId，不覆盖已保存的 Payload、Headers、追踪上下文或计划时间。登记 Span 表示本次登记调用成功或失败，不提前声称外层业务事务已经提交；事务回滚时 Outbox 和身份仍一起回滚。

### 发布尝试与消息属性

1. 在线程开始执行并成功抢占后，才创建有效发布尝试的 Span。排队候选和抢占失败不计为发送尝试。
2. 从持久化 Header 提取上下文，自动发布与重试以登记上下文为上游，各次尝试为独立 Span。不把数据库等待、退避或停机期间包进一个长期未结束的 Span。
3. 在单次尝试内分别记录 Sender 回执和状态更新结果：发送成功、明确失败、结果未知，以及落库成功、落库失败或所有权校验拒绝。Span 覆盖本次执行，细分操作可采用事件或子 Span；指标保留各自口径。
4. 发送消息时使用本次尝试的上下文覆盖消息副本中的追踪字段，让消费者关联到实际收到的那次尝试。Outbox 中保存的登记上下文不随重试修改；不要直接把旧登记 Header 原样当成本次发送上下文。
5. 旧事件没有上下文或上下文损坏时，允许创建新的根 Span 并继续发布；保留 EventId 关联。线程作用域在正常、异常及中断路径都必须关闭，不能让后续任务继承前一事件的上下文。
6. 本阶段人工重放仍按原登记上下文创建新的尝试；不建立操作者请求的完整追踪关系。跨很长时间的链路可能超出追踪后端保留窗口，这一限制写入运维指南。后续若采用新 Trace 加 Span Link，另行明确关联协议。

### 属性、失败隔离与开关

- Trace 和受控日志可以记录 EventId、attemptCount、MessageId、发送结果及状态更新结果；不自动复制 Payload、完整 Headers 或 eventKey。
- EventId、TraceId、SpanId、MessageId、业务键和错误全文不能成为指标标签。事件类型或目标仅在受控、有限的取值集合中作标签，其余归入 `other`。
- 保持现有公开发布接口和构造路径可用；新增协作接口提供 NOOP。无追踪依赖、无 Registry、无 Tracer 或关闭开关时，既有发布能力保持可用。
- 推荐配置为 `reliable-event.tracing-enabled=true`，由条件装配决定是否生效；关闭时保留原 Header 行为。明确区分“默认允许集成”和“实际配置了追踪后端”。
- 采用小型内部追踪协作接口连接登记、尝试作用域和发送 Header 注入；Micrometer 实现放在自动配置层，Core / JDBC 不直接依赖 OpenTelemetry SDK。既有指标 Observer 与追踪协作接口独立组合，不互相替换。
- 追踪组件的提取、注入、记录和导出故障采用尽力而为策略；业务和数据库异常保持原分类及传播，不能被追踪异常覆盖。导出异步进行，不在发送临界路径同步访问追踪后端。
- Starter 不强制安装采集器或 Exporter，不创建第二套 SDK；与应用提供的 Registry、Tracer 和配置协作。首版只验收上述 OTel 组合，不提前声称其他 Bridge 均兼容。

## 指标口径与采集方式

以下名称为 Micrometer 名称。M8.3 已用 PrometheusMeterRegistry 实际抓取确认导出名称、计数器和标签；既有指标继续保留，新增 Observation 不重复注册同名计数器或重复统计既有指标。

### 数据库快照

- `reliable_event.ready`：`PENDING` 或 `RETRY_WAIT` 且 `next_attempt_at <= 数据库当前时间` 的记录数。未来事件、尚未到重试时间的事件不计入。
- `reliable_event.ready.oldest_age`：上述可领取记录中，从最早非空 `first_available_at` 到数据库当前时间的秒数，用于观察领取阶段等待。事件被抢占或进入退避后会离开该集合，因此该指标不用于判定持续发布超时或发布停滞。
- `reliable_event.unfinished.overdue`：状态为 `PENDING`、`PUBLISHING` 或 `RETRY_WAIT`，且非空 `first_available_at <= 数据库当前时间` 的记录数。过滤依据为首次可用时间，不使用当前 `next_attempt_at`；事件进入处理中、结果未知或下一次重试尚未到期时仍属于这个集合，直到成功落库为 `PUBLISHED` 或进入终态 `DEAD`。
- `reliable_event.unfinished.overdue.oldest_age`：上述未完成集合中，从最早 `first_available_at` 到数据库当前时间的秒数，用于持续超时告警。集合为空时为 0；同一事件在 `PENDING → PUBLISHING → RETRY_WAIT` 间切换不重置累计年龄。首次尚未到期的未来事件不计入；进入 DEAD 后交给死信告警处理，不解释为成功发布。
- `reliable_event.unfinished.timestamp_missing`：三种未完成状态中缺少 `first_available_at` 的记录数。这些存量行无法准确判定是否首次到期或计算年龄，不能用 `created_at` 或重试时间伪造；单独提示数据不完整。`ready` 中全部记录都缺少首次时间时，其年龄为 `NaN`；部分缺失时只计算已知记录并同时展示缺失数量。没有任何记录时年龄才为 0。
- `reliable_event.snapshot.last_success_timestamp`：最近一次数据库快照成功采集的应用 UTC Unix 秒；未成功采集过时为 0，配合启动宽限判定。
- `reliable_event.snapshot.failure`：数据库快照采集失败次数。失败后保留上次快照，同时暴露陈旧状态。

同一次成功采样由单条聚合 SQL 计算所有字段；MySQL 的 `UTC_TIMESTAMP(3)` 在同一语句内保持一致，以不可变快照整体替换旧值。查询失败不推进成功时间、不发布半份快照。查询不读取 Payload 或 Headers。MySQL 8.0.36 的 `EXPLAIN` 检查到 `type=ALL`、`key=NULL`、估计 1 行；表上已有 `idx_publish_scan`、`idx_lease_recovery`、`idx_dead_list`、`idx_published_retention`，但它们不能避免对各状态和时间条件的整表聚合。当前未增加索引：小表计划读取 1 行；数据增长时该口径成本随 Outbox 行数线性增长，应按部署数据量监控查询耗时并重新评估覆盖索引或独立汇总方案。

### 自动采样与手动刷新的兼容协议

- 推荐新增 `metrics-snapshot-enabled=true` 和 `metrics-snapshot-interval=15s`，前者只控制新增的后台周期采样。创建任务须同时满足 ReliableEvent 启用、存在 MeterRegistry、装配默认 Micrometer 指标实现、默认自动发布运行时启用且由 Starter 管理，以及该采样开关开启；缺少任一条件时不创建任务。自定义 Observer 或自定义发布运行时不隐式获得新的后台线程，仍由接入方协调。
- `scheduling-enabled=false` 时，本阶段不创建周期采样任务，即使 `metrics-snapshot-enabled=true`。保留 `JdbcEventPublicationCycle.runOnce()` 的 `finally` 刷新和 `JdbcExpiredLeaseRecovery.refreshSnapshot()` 显式入口；手动刷新不受 15 秒频率限制，调用返回前完成本次刷新尝试，成功时立即可读取新 Gauge，失败时保留旧快照，且不覆盖原发布异常。手动调用的停机协调责任仍由调用方承担。
- 自动模式启用周期任务后，默认指标实现在自动发布轮次的采样接缝不再重复执行聚合 SQL；显式手动刷新仍有效。关闭周期采样时，默认指标实现回到自动轮次采样，并按 `metrics-snapshot-interval` 限频。无论是否有周期任务，自定义 Observer 的原轮次回调语义保留；可增加默认委托原方法的自动限频接缝，不能直接删除旧刷新回调。
- 后台和手动刷新共用同一个不排队的采样准入门，同一实例最多一份快照查询在执行。周期任务遇忙跳过；手动入口遇已有查询在执行时记录受控的未刷新结果并立即返回，保留旧值，不等待锁或重复发起查询。该并发降级行为写入运维说明并有专项测试；无竞争且查询正常时保留原来的即时刷新行为。
- 增加 `reliable_event.snapshot.periodic_enabled` 展示周期模式是否生效。手动模式的示例告警须配置预期手动调用周期；主动关闭全部刷新入口的部署不能沿用自动模式的时效阈值。仅关闭周期任务但仍有自动轮次采样时，仍需要监控快照时效。

### 采样资源、超时与停机协议

- 周期采样使用独立调度资源，不在发布 Worker、租约恢复事务或 Scheduler 生命周期锁中执行查询。自动采样最多一项查询在执行，不累积候选、查询或重试队列；借用应用 DataSource 的连接最多一条。不得修改共享 JdbcTemplate、连接池全局超时或应用现有任务执行器的配置。
- 配置为 `metrics-snapshot-query-timeout=2s`、`metrics-snapshot-timeout=5s`、`metrics-snapshot-shutdown-timeout=2s`。查询超时只设于采样自己的 Statement；JDBC 的整数秒 timeout 向上取整小于一秒的正值。总预算覆盖自动采样的连接获取、查询和结果收集，以单调时钟计算；关闭预算独立约束采样器停止等待。参数均为正，总预算不小于查询预算，关闭预算不大于既有 `shutdown-timeout`。真实 MySQL 锁表测试确认 Statement timeout 能结束被阻塞的聚合查询；它仍不限制连接池获取时间。
- 自动采样的预算等待与 JDBC 执行分离：使用固定的单查询执行资源和无排队准入，等待方到总预算即判本次失败、请求取消并返回；底层若忽略取消，执行槽仍由原查询占用，后续轮次只记录跳过，不释放槽去创建更多线程或连接。采样资源使用可关闭的守护线程，查询真正结束后在 `finally` 释放自身借用的连接。不能把 Future 取消成功表述为数据库调用已经停止。
- 手动入口保留调用线程同步刷新的职责，采用采样局部的 Statement 超时；连接获取受应用原连接池策略约束，不承诺新增的 5 秒自动预算适用于手动调用。它不启动周期任务，也不为保证即时 Gauge 新建一个无界后台执行器。需要硬截止时间的手动运行由调用方协调，符合原 M4.4 边界。
- Context 停止时先关闭采样准入和下一轮调度，废弃未开始的任务；采样器与发布 Scheduler 在同一停止阶段启动各自的有界停止，独立执行回调，不串行叠加或重置原发布停机截止时间。采样器按自身关闭预算等待，到期发出取消并完成回调，使后续 Bean 销毁可继续；共享 DataSource、MeterRegistry 和 Producer 由其原所有者管理。
- 采样持有生命周期代次标记；超时、停止、关闭或旧 Context 的迟到结果不能写入快照、推进成功时间或重新注册 Meter。新 Context 的 Gauge 重新从未知值开始。Meter 的移除沿用默认指标 Bean 的资源所有权，不能因采样器先停止而提前移除在途发布仍使用的指标。
- 不宣称共享数据库故障时采样完全不占资源，也不宣称整个 Context 关闭都受本项目截止时间约束。M8.3 必须验证采样额外占用有上限、调用方控制权按时返回、停止后无新查询，以及不改变原发布停机时序。

### 本实例运行情况

- `reliable_event.worker.inflight`：本实例已成功抢占且尚未完成状态处理的任务数；用 `finally` 释放统计，不包含排队候选。
- `reliable_event.worker.queued`：本地等待执行、尚未抢占的候选数；与在途任务分别展示。
- `reliable_event.scheduler.enabled` / `running`：区分配置关闭、正常运行和异常停止。
- `reliable_event.scheduler.last_success_timestamp`：最近一次自动发布轮次正常完成的时间。空闲或本地容量满但轮次正常完成时也更新，避免健康空闲被误报；数据库扫描或恢复失败的轮次不更新。
- `reliable_event.scheduler.failure`：轮次失败次数，以有限的阶段值区分候选扫描、租约恢复和重新调度等错误。手动执行失败与自动调度失败区分模式。
- `reliable_event.publication.state_update_failure`：发送之后状态更新失败次数；与 Sender 错误分开。
- `reliable_event.publication.persisted`：成功提交 `markPublished` 的事务后增加的计数，用于观察生产端落库进度。Sender 成功但状态更新失败、租约失效或事务回滚时不增加；与原 `publish.success` 回执计数分开。该计数是尽力而为的进度信号，进程在提交后、计数前退出可能漏计，不作为持久审计或唯一事件账本。
- `reliable_event.dead.entered`：本实例成功提交进入 `DEAD` 的状态转换次数，覆盖发送失败耗尽和过期租约恢复。竞争失败或事务回滚不增加；人工重放后再次进入 DEAD 是新的转换。

数据库快照按同一 Outbox 存储去重，不能跨实例求和；实例计数器先求各自 rate / increase 再聚合，在途数按需要求和。示例 Prometheus 配置提供稳定的逻辑 `outbox_store` 标签。快照聚合仅使用新鲜样本，并提供“没有新鲜样本”的告警，不能用一个陈旧实例掩盖监控失效。不同存储不能混为一组。采集时钟与数据库时钟的不同口径及允许偏差写入说明。

## 实施阶段与验收门槛

### M8.1：可选追踪接缝与登记上下文

交付内部协作接口、NOOP、Micrometer 条件装配、开关和登记端上下文持久化。先用当前 BOM 的实际依赖验证兼容性与自动配置顺序，保证指标 Bean 不因追踪集成消失。

验收：真实 MySQL 中业务提交后上下文存在、回滚后双表都不存在；重复登记不覆盖原上下文；无上下文、错误 Header、预算边界和提取异常可控。分别验证无追踪类、只有指标、有 Tracer、关闭开关及自定义 Observer 的装配行为。

### M8.2：逐次发送追踪和上下文传播

状态：M8.2 已实施。详细实现和验收证据见 [`M8_2_COMPLETED.md`](../progress/M8_2_COMPLETED.md)。M8.3 单独交付；后续阶段见下文。

交付 Worker 尝试作用域、发送 Header 副本注入、回执和落库结果记录、线程作用域清理。

验收：同一事件两次尝试产生不同 SpanId，均关联到原登记上下文；消息携带当前尝试的上下文，数据库 Header 保持原值；覆盖发送成功、明确失败、结果未知、状态更新失败和租约失效。单线程连续处理不同事件不串 Trace。无上下文旧事件继续发布。禁用或故障的追踪组件不改变原状态机、退避、最大尝试次数和停机行为。

### M8.3：运行指标、采样兼容与有界停止

状态：M8.3 已实施。实现、真实 MySQL 状态序列、事务提交、资源关闭、Prometheus 抓取和索引计划证据见 [M8.3 完成记录](/D:/trae/scp/reliable-event/docs/progress/M8_3_COMPLETED.md)。

交付可领取与连续未完成两种口径的指标、落库进度计数、周期采样开关及超时配置、整体快照、手动刷新兼容路径、独立采样生命周期和查询成本说明。明确与 M4.5 自动采样频率的变化，保留原显式刷新入口和自定义 Observer 行为，避免自动轮次与后台任务重复执行聚合。

验收分为以下几组：

- **连续积压口径**：真实 MySQL 验证同一事件跨过首次可用时间后，在 PENDING、PUBLISHING、尚未到重试时间的 RETRY_WAIT 中均计入未完成量，累计年龄不重置；发布成功或进入 DEAD 后退出；未来事件排除、时间字段缺失单独提示。用可控时间和逐次快照验证整条状态路径，而非仅检查一张最终快照。
- **刷新兼容**：保留并运行现有手动 Starter Gauge 断言和 Cycle 失败传播测试；无竞争时 `runOnce()` 返回即可读取新快照，失败后旧值保留且原异常不变。`scheduling-enabled=false`、无 Registry、自定义 Observer、自定义运行时及全局关闭场景不创建默认周期任务；关闭周期任务时自动轮次限频刷新，手动入口不限频。并发刷新遇忙按约定降级，不并行查询。
- **统计与一致性**：在途、排队数量不为负且最终归零，空闲轮次心跳正常；Sender 成功但落库失败不增加 `publication.persisted`。快照各字段整体替换，失败不推进成功时间；核对实际 Prometheus 输出和双实例聚合。
- **资源与停止**：覆盖连接池耗尽、慢查询、查询忽略中断、刷新超时后迟到返回、Context 关闭与手动刷新并发、Context 重建。自动总预算内等待方返回，底层未退出时最多保留一个占用槽和一条借用连接，后续不累积线程或任务；采样关闭回调按时完成，发布 Scheduler 的停止时序不被延长，迟到结果不写回或复活 Meter。

### M8.4：原创订单示例的端到端追踪

状态：M8.4 已实施。实现与验收证据见 [M8.4 完成记录](/D:/trae/scp/reliable-event/docs/progress/M8_4_COMPLETED.md)。Tempo、Prometheus 看板和告警仍留在 M8.5。

仅在示例应用增加消费侧追踪，围绕事务性 handler 调用记录处理结果，另行记录 ACK 成功或失败；相同事件重复收到时，记录幂等跳过。消费者只读取 RocketMQ properties 中大小写不敏感的 W3C `traceparent` / `tracestate`；不会自动提取 baggage。无效或缺失的上下文创建显式 root span；提取或开始 span 出错时降级为无追踪处理。正常处理期间关闭 Scope 并恢复此前 ambient Context。追踪缺依赖、关闭或记录/关闭 API 故障不影响 handler、ACK、业务异常传播和重投行为。

处理结果在 Spring 事务代理的 `OrderMessageHandler.handle()` 返回后记录，以便只把已提交事务标记为 `processed` / `idempotent_skip`。handler 异常独立记录为处理失败且不 ACK。ACK 在事务返回之后独立执行和记录；ACK 异常保留此前已经提交的业务结果，Broker 重投仍由既有去重约束保护。此逻辑限定在原创示例，没有增加通用消费者 API 或消费结果回写协议。

示例依赖 Boot Actuator 和 Micrometer OTel Bridge，W3C 传播与 100% 采样用于本地演示；生产采样由接入应用调整。M8.4 的链路验收用内存 SpanExporter 读取由 Spring Boot 装配的 SDK，没有验证 OTLP/Tempo 后端连接或查询。Starter 不强制 exporter；平台部署归 M8.5。

验收：真实 RocketMQ 消息能够关联 HTTP 请求、登记、实际发送尝试和消费处理；handler 事务已提交但 ACK 失败时不能记录成业务回滚；重复消息依然只有一次业务效果。旧消息或无追踪属性的消息可以正常消费。示例采用 100% 采样便于验证，生产采样由接入方选择。

### M8.5：看板、告警规则与故障处置说明

状态：已完成。独立 `observability/` Compose、Prometheus 抓取配置与规则、Alertmanager 本地路由、Tempo OTLP 接收配置、Grafana provisioning、PowerShell 脚本及运维说明均已交付。官方 Prometheus 3.14.0 `promtool` 检查通过 16 项规则和 20 个场景；Java 17 全仓 `-Pobservability clean verify` 通过。真实平台验收验证了订单 Trace 的四个 spans、Grafana datasource 与 16 个查询结果，以及 Alertmanager firing/resolved 到本地 receiver。14 面板看板通过匿名 Viewer 浏览器验收；在顶部 Trace ID 输入框粘贴 trace ID，即可在第 14 面板查看 Tempo trace。完整记录见 [M8.5 完成记录](../progress/M8_5_COMPLETED.md)；旧进度及失败尝试保留在 [M8.5 历史进度](../progress/M8_5_PROGRESS.md)。

第一版告警包括：

1. **积压超时**：新鲜快照中 `unfinished.overdue > 0`，`unfinished.overdue.oldest_age` 超过阈值并持续一段时间；处理中和退避等待均保持条件，不以 `ready > 0` 作门槛。未来事件排除，时间数据不完整时单独提示，进入 DEAD 后由死信规则接续。
2. **发布调度异常**：配置应运行但调度未运行，或成功轮次心跳过期；告警结合启动宽限和实际 poll-interval，主动关闭调度时不误报。
3. **发布停滞**：新鲜快照持续显示 `unfinished.overdue > 0`，且 `publication.persisted` 在窗口内没有进度。在途数量只用于辅助排障，不作为必须为零的触发条件；在途为零也可能正常等待退避，处置说明结合重试等待、失败计数和调度心跳判断，不直接归因为调度故障。规则窗口结合退避策略和业务时效选择；即使其他事件仍成功、全局进度在增加，积压超时规则仍可发现长期未完成的事件。
4. **新增死信**：`dead.entered` 在窗口内增加；用 DEAD Gauge 辅助判断当前待处理量。
5. **发送异常**：窗口内失败比例升高，设置最小尝试量，避免极低流量下单次失败触发比例告警。
6. **状态落库异常**：状态更新失败计数增加，提示可能存在重复投递窗口。
7. **监控失效**：目标不可抓取、快照指标缺失或全部快照过期；与积压为 0 区分。

初始演示阈值可采用未完成年龄 60 秒且持续 5 分钟、15 秒快照连续 60 秒未更新；正式规则参数和时间窗口必须与指标口径、采集间隔、退避策略及业务时效共同确认，不把演示阈值作为生产承诺。手动模式另行配置预期调用周期；无周期任务但仍有自动轮次采样时按实际刷新频率判定时效，不能仅因 `periodic_enabled=0` 就屏蔽快照失效告警。

每条规则提供严重级别、摘要、影响、排查入口、恢复判定与 Runbook 链接；看板给出积压与时效、发送和落库进度、错误类型、任务容量、心跳、死信及清理情况，并提供按 TraceId 查看 Tempo 的操作。日志通过 traceId/spanId 关联，第一版不承诺一键跳转完整日志平台。Alertmanager 先使用本地可验证的接收端，不替用户配置外部通知凭证或发送真实通知。

验收：`promtool check rules` 和规则测试覆盖触发、恢复、无数据、未来事件、空闲、采样关闭、手动模式及双实例重复快照。重点构造单事件反复失败退避、长期 PUBLISHING，以及其他事件持续成功但单事件长期未完成的时间序列，证明积压超时条件不被状态切换重置；转 PUBLISHED 后恢复，转 DEAD 后接续死信提示。告警样本必须分别验证“触发”和“恢复”。看板能导入且显示真实数据，Tempo 能查询示例 Trace，Alertmanager 能路由到本地测试接收端。

### M8.6：真实故障演练、开销检查与交接

交付以下可复现演练及证据：

- Broker 短时不可用，观察失败、退避、积压及恢复；结果未知重投产生独立 Span，消费者幂等保持有效。
- 抢占后进程退出，由新实例从数据库恢复追踪关联；原实例被强杀时可能没有结束 Span，文档如实说明。
- 数据库快照失败，验证旧值显示为陈旧并触发监控失效，数据库恢复后恢复告警。
- 单条事件连续失败并进入长退避，或停留在 PUBLISHING，验证未完成年龄持续增长及超时告警；同时发送其他健康事件，验证全局进度不能掩盖单事件超时。
- 采样连接等待或查询阻塞期间关闭 Context，验证单查询资源上限、关闭预算、迟到结果丢弃及新 Context 初始化；采样故障不延长原发布 Scheduler 的停止截止时间。
- 状态更新故障，验证发送回执成功与落库失败被分别记录。
- 追踪后端不可用，验证业务事务、发布和恢复不依赖导出成功；故障结束后新数据可继续导出。
- 空闲、未来计划事件、关闭自动调度、应用启动和双实例场景均不误报停滞。

持续超时的真实演练须显式配置测试用最大尝试次数、退避和告警窗口，确保事件在触发前仍处于未完成集合，而非已耗尽进入 DEAD；这些调整仅用于隔离测试环境，参数和时间线随证据保存，不修改产品默认值。规则单元测试与真实演练分别报告结果。

从同一最终源码分别运行追踪关闭、追踪开启的受控基准，固定负载、快照间隔和采样率，记录登记耗时、发送耗时、排空速率、数据库查询开销、CPU 和内存。结果与历史 M5.3 数字分开，不预设固定性能提升或无开销。若开销明显异常，定位并修复后再验收。

执行 Java 17 下全仓 `mvn verify`、现有 Python 报告测试及新增告警规则测试；保留退出码和原始证据。更新 README、OPERATIONS、HANDOFF 和 M8 完成记录，说明依赖、开关、采样、数据保留、未追踪旧事件、多实例聚合和故障边界。生产推广和正式工件发布仍另行通过发布检查。

## 审阅重点与建议执行顺序

建议按 M8.1 → M8.2 → M8.3 → M8.4 → M8.5 → M8.6 推进，每个阶段通过自己的验收后再进入下一阶段。M8.2 是“单条事件可解释”的门槛；M8.3 是“运行状态可信”的门槛；M8.5 是“可以主动告警”的门槛。

本轮需要审阅的主要取舍是：追踪依赖可选、复用 Headers、不覆盖重复登记的上下文、发送结果与落库结果分开、消费者只扩展示例，以及把监控设施作为独立可运行材料交付。影响检查的三项修订已纳入：连续超时使用首次已到期的未完成集合，手动轮次保持结束刷新且不创建周期线程，新增自动采样按独立资源与停止预算管理。可观测性平台的部署和业务方消费者接入责任需要在文档中明确。

本计划的完成标准是：从一条真实订单事件出发，能定位各次发布和消费处理；通过故障注入得到可触发、可恢复、可解释的告警；关闭追踪或后端故障时，原可靠发布协议仍通过回归。写入计划和配置样例本身不构成功能完成。

## 一手参考

- [Spring Boot 3.5 追踪集成](https://docs.spring.io/spring-boot/3.5/reference/actuator/tracing.html)：Micrometer Tracing、OpenTelemetry Bridge、OTLP、采样及日志关联。
- [Micrometer Observation](https://docs.micrometer.io/micrometer/reference/observation/introduction.html)：Observation 生命周期、Handler 和高低基数属性。
- [Micrometer Tracing API](https://docs.micrometer.io/tracing/reference/api.html)：上下文提取/注入与作用域。
- [OpenTelemetry 上下文传播](https://opentelemetry.io/docs/concepts/context-propagation/)：W3C traceparent 的传递及执行关联。
- [OpenTelemetry 消息 Span 约定](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)：消息创建、发送、消费的父子或 Link 关系；实施时核对所用版本的约定，避免承诺覆盖全部约定。
- [Prometheus 告警规则](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/)：表达式、持续时间与告警元数据。

## 项目关联

- [当前交接](../HANDOFF.md)
- [接入与运维指南](../OPERATIONS.md)
- [M4.5 指标与发布语义](M4_5_OBSERVABILITY_AND_M4_ACCEPTANCE.md)
- [M6 死信重放](M6_DEAD_LETTER_OPERATIONS.md)
- [M7 身份保留与清理](M7_PUBLISHED_EVENT_RETENTION.md)
