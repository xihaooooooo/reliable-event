# M8.2：逐次发送追踪和上下文传播

状态：M8.2 已实施。M8.3–M8.6 尚未实施；本记录不代表 M8 整体完成。

## 实现

JDBC 增加内部 `PublicationTracer` / NOOP 接缝，并保留已有 `JdbcEventPublicationWorker` 构造路径。Worker 只在事件成功抢占、进入发送路径后创建本次尝试作用域；候选扫描和抢占失败不会创建 Span。Micrometer 实现从持久化的登记 Header 提取 W3C 父上下文，每次调用单独创建 Span。缺失、格式错误或全零 traceparent 会显式设为无父级，即使当前线程有其他活动 Span，也创建新根 Span。

发送时，Tracer 将本次尝试的 `traceparent` / `tracestate` 合并到新的 `StoredEvent` 副本并继续使用 Core 既有 Header 数量、格式和字节预算校验。若上下文提取失败，会创建根 Span 并尝试注入该根 Span 的新上下文。Header JSON 无法解析、实际注入失败或合并后超预算时保留原事件副本；Sender 仍按原逻辑识别并处理非法旧 Header。不会修改 Claim 或数据库登记 Header，也不读取或注入 Baggage。提取 Getter 和注入 Setter 都只允许 W3C traceparent / tracestate。

Span 分开记录 Sender 成功、明确失败、结果未知，以及数据库状态更新执行结果。落库成功、失败和 typed `StaleEventClaimException` 所表示的所有权拒绝分别表达；该异常继续继承原来的 `IllegalStateException`，兼容调用方。同步事务属于新事务时标记状态更新已完成；参与外层事务时标记提交仍待外层事务完成。尝试 Span 不等待外层事务提交。Span scope 在正常、异常和中断路径通过 Worker 的 `finally` 关闭。

默认 Micrometer PublicationTracer 与 M8.1 RegistrationTracer 都要求应用提供 Tracer 和 Propagator；PublicationTracer 还要求 ObjectMapper。不创建 SDK、Registry 或 Exporter。`reliable-event.tracing-enabled=false` 时 Worker 使用 NOOP，包括应用自行提供的 `PublicationTracer`；指标 Observer 的注入和回调保持独立。追踪故障均被隔离：begin 失败使用 NOOP，注入失败使用原对象，结果记录和关闭失败被跳过；这些故障不覆盖 Sender 异常分类、重试、终态、状态更新异常或线程停止行为。

## 验收证据

- 使用真实 Micrometer OpenTelemetry Bridge、OpenTelemetry SDK 与内存 SpanExporter 验证重试关系：对同一个 StoredEvent 的两次尝试得到不同 SpanId，TraceId 相同且各自 ParentSpanId 等于登记 SpanId；消息副本保留 EventId、类型、Key 和 Payload，并携带当前尝试 SpanId，原 StoredEvent 的 `headersJson` 不变。
- 在同一线程保持一个无关活动 Span，分别发布缺失上下文、损坏 traceparent 和全零 traceparent 的事件。三者都开启独立 Trace，关闭尝试后原活动 Span 仍为当前 Span。恶意 Propagator 请求 baggage 时，Getter 返回 null。
- Worker SpanData 覆盖 Sender 成功、明确可重试失败、结果未知、状态更新故障、所有权拒绝，以及外层事务 `commit=pending`。故障回调、trace start、发送副本注入、结果记录、scope close 和发送中断路径均验证发布结果仍按原协议处理；中断后真实 Micrometer scope 关闭且 Span 已结束。
- 真实 MySQL `ReliableEventIntegrationTest` 使用测试 `PublicationTracer` 验证 Worker 两次调用传递不同的尝试 Header 副本，第一次结果未知进入 `RETRY_WAIT`、第二次成功进入 `PUBLISHED`，SQL 中的 traceparent 始终是登记值。对已完成事件再次 `publishCandidate` 不创建新尝试 Span。真实 OTel Span 父子关系由单独的内存 SDK 测试验证。
- 真实 RocketMQ 5.5.0 与 MySQL 8.0.36 Testcontainers 使用测试 `PublicationTracer` 构造尝试 traceparent，验证 Worker 发送的 Header 实际到达 RocketMQ 消息 properties，业务 Header 同时保留，SQL Outbox 的登记 traceparent 不变。此验证覆盖 Worker、序列化和 Broker 属性传播；OTel Span 父子关系单独由内存 SDK 测试覆盖。
- 自动配置验证启用 OTel 后 Worker 得到 Micrometer PublicationTracer；关闭追踪后自定义 PublicationTracer 不被 Worker 使用。M8.1 指标 Observer 与追踪同时存在时仍各自装配。

## 命令与原始日志

运行环境：JDK `17.0.12`（`C:\Program Files\Java\jdk-17`）、Maven `3.9.9`、Docker Desktop `29.1.3`。Maven 命令通过进程局部 `JAVA_HOME` / `PATH` 选择 JDK 17，依赖目录为 `C:\Users\20659\.m2\repository`。

- 自动配置及 OTel Worker 聚焦测试：退出码 0，30 项通过（Tracer 1、Worker 2、自动配置 27）。原始日志：`target/evidence/m8.2-worker-tracing-tests.log`。
- 真实 MySQL 集成：退出码 0，`ReliableEventIntegrationTest` 48 项通过。原始日志：`target/evidence/m8.2-mysql-integration.log`。
- 真实 MySQL / RocketMQ 集成：退出码 0，`RocketMqPublicationIntegrationTest` 6 项通过。原始日志：`target/evidence/m8.2-rockemq-integration.log`。
- 全仓 Java 17 `mvn clean verify`：退出码 0；从 clean 后产生的 32 份 Surefire XML 求和，182 项通过，0 失败、0 错误、0 跳过。原始日志：`target/evidence/m8.2-full-verify.log`。
- 全仓验证后，因仅增加了 OTel 提取故障开根和注入故障回退断言，再执行一次带 reactor 依赖的 `-pl reliable-event-spring-boot-autoconfigure -am -Dtest=MicrometerPublicationTracerTest` 聚焦命令：退出码 0，1 项通过。生产代码和依赖没有再变更。原始日志：`target/evidence/m8.2-last-tracer-check.log`。

本阶段没有验证 Tempo / OTLP 后端端到端导出、消费者处理 Span、运行指标扩展或告警；这些属于后续阶段。
