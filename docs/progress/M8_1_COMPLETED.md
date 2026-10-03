# M8.1：可选追踪接缝与登记上下文

状态：M8.1 已实施。M8.2–M8.6 尚未实施；本记录不代表 M8 整体完成。

M8.1 增加了 JDBC 内部 `RegistrationTracer` 协作接口及 NOOP 默认实现。`JdbcReliableEventPublisher` 保留已有构造器，并在 `publish()` 的登记路径创建短追踪作用域；追踪开始、Header 注入、成功/失败记录和关闭异常都不会覆盖业务序列化或数据库异常。登记 Span 使用当前业务请求的活动上下文作为父级，没有活动上下文时创建根 Span。调用方 Header 只作为业务数据输入，旧 `traceparent` / `tracestate` 不用作父上下文，成功注入时被新值覆盖；只向持久化副本加入 W3C 字段，不修改调用方 Map，也不注入 Baggage。

`reliable-event.tracing-enabled` 默认为 `true`，表示允许可选集成。只有 Micrometer Tracing 类以及应用提供的 `Tracer`、`Propagator` 同时存在，且 `reliable-event.enabled=true` 时，才装配默认追踪实现。关闭追踪开关时，即使应用提供了自定义 `RegistrationTracer`，Publisher 仍使用 NOOP 并原样保留输入 Header。Starter 不安装 Tracing Bridge、OpenTelemetry SDK 或 Exporter。

追踪 Header 和 RocketMQ 发送共用 Core 中的 `EventHeaderConstraints`，共享属性数量、键名、单值和总字节限制。自动注入后的整份 Header 超过限制时，放弃本次追踪注入并保留原值。`RocketMqMessageFactory` 仍将相同验证错误包装成原来的不可重试发送异常，避免改变既有消息校验语义。

## 验收证据

- Spring Boot OTel Bridge Context 用真实自动配置的 `Tracer` 创建请求 Span A，通过自动装配的 `ReliableEventPublisher` 写 SQL 参数；即使输入含伪造 Span B 的 `traceparent`，持久化 Header 仍关联 Span A。Publisher 返回后活动上下文恢复；没有活动请求时，下一次登记创建并传播独立根 Trace。覆盖默认 `MicrometerPublicationObserver` 与追踪同时装配，以及自定义 Observer 不被替换。
- 条件装配测试覆盖缺少 Tracing 类、仅有指标 Registry、关闭追踪、自定义追踪器在关闭时被忽略、应用 OTel Tracer/Propagator 可用。无追踪组件的 Publisher 仍可装配。
- Header 测试覆盖业务 Header 保留、追踪字段覆盖、调用方 Map 不变、不注入 Baggage、64 个属性边界、4 KiB 单值边界、16 KiB 总字节预算、注入失败、Span start/tag/error/scope close 故障隔离，以及 SQL/序列化异常保持原分类。
- 真实 MySQL 使用测试 `RegistrationTracer` 验证提交后的 Outbox Header、事务回滚时业务表/identity/outbox 三者都没有记录，以及重复登记保留第一条追踪上下文。既有 `ReliableEventIntegrationTest` 共 47 项通过，其中 M8.1 新增 2 项。
- 全仓 Spring Boot BOM 依赖树解析为 Boot `3.5.16`，`micrometer-tracing` `1.5.12`；测试使用 `micrometer-tracing-bridge-otel` `1.5.12` 与 OpenTelemetry SDK `1.49.0`。这些版本由 BOM 管理。
- Boot OTel 自动配置测试证明 Tracer 与登记追踪接缝按正确顺序装配，生产端未创建或替换 SDK。此验收不包含 Tempo/OTLP 后端端到端演示。

本阶段尚未实现 Worker 发布尝试 Span、发送 Header 注入、消费者追踪、运行指标扩展、周期采样、告警配置或故障演练；这些仍属于后续阶段。

## 命令与原始日志

运行环境为 JDK `17.0.12`、Maven `3.9.9`、Docker Desktop `29.1.3`，编译目标 Java 17。

- `mvn -ntp '-Dmaven.repo.local=C:\Users\20659\.m2\repository' -pl reliable-event-spring-boot-autoconfigure -am '-Dtest=MicrometerRegistrationTracerTest,ReliableEventAutoConfigurationTest,JdbcReliableEventPublisherTracingTest' '-Dsurefire.failIfNoSpecifiedTests=false' test`：退出码 0；JDBC 单测 3 项、追踪实现单测 4 项、自动配置测试 26 项，合计 33 项全部通过。完整 Maven 日志见 `reliable-event/target/evidence/m8.1-focused-tests.log`。
- `mvn -ntp '-Dmaven.repo.local=C:\Users\20659\.m2\repository' -pl reliable-event-jdbc -am '-Dtest=ReliableEventIntegrationTest' '-Dsurefire.failIfNoSpecifiedTests=false' test`：退出码 0；Testcontainers 使用 MySQL `8.0.36`，既有集成类 47 项全部通过，其中 M8.1 新增 2 项。完整 Maven 日志见 `reliable-event/target/evidence/m8.1-mysql-integration.log`。
- `mvn -ntp '-Dmaven.repo.local=C:\Users\20659\.m2\repository' -pl reliable-event-spring-boot-autoconfigure '-Dincludes=io.micrometer:micrometer-tracing,io.micrometer:micrometer-tracing-bridge-otel,io.opentelemetry:opentelemetry-sdk,org.springframework.boot:spring-boot-actuator-autoconfigure' dependency:tree`：退出码 0。完整输出见 `reliable-event/target/evidence/m8.1-dependency-tree.log`。
- `mvn -ntp '-Dmaven.repo.local=C:\Users\20659\.m2\repository' verify`：退出码 0；全仓 176 项通过，0 失败、0 错误、0 跳过。既有 `RocketMqMessageFactoryTest` 的 4 项通过，确认抽取公共预算规则后保持发送 Header 校验。全仓日志见 `reliable-event/target/evidence/m8.1-full-verify.log`。

上述测试没有验证 Tempo 导出或发布尝试 Span；它们不属于 M8.1 的交付范围。
