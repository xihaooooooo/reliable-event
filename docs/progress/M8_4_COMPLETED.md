# M8.4：原创订单示例端到端追踪

状态：M8.4 已实施。实施日期：2026-10-02（Asia/Shanghai）。M8.5–M8.6 尚未实施；本记录不表示 M8 整体完成。

## 实现

示例消费 loop 通过可选的 Micrometer `Tracer` / `Propagator` 读取 RocketMQ properties 中大小写不敏感的 W3C `traceparent` 和 `tracestate`。只有标准 W3C v00 `traceparent` 且 trace/span ID 非全零时才提取远程 parent；缺失或非法 parent 时显式新建 root span；上下文提取或 span 开始异常时降级为无追踪处理。提取只允许两个 W3C trace 字段，不自动传播 baggage。span 只记录 Broker Message ID，不记录 event ID、Payload、业务键或完整 headers。

消费者 Span 围绕一次同步 handler 及 ACK 流程。业务结果只在 Spring 事务代理 `OrderMessageHandler.handle()` 返回后记录：`true` 表示事务提交的处理，`false` 表示已有去重记录、幂等跳过；handler 异常记为业务处理失败并维持原有不 ACK 行为。ACK 单独记录成功或失败。ACK 异常不会将此前 handler 提交的事务改记为业务失败，Broker 重投后原身份约束继续保证最多一次示例业务效果。生产协议、handler boolean 语义、事务和构造调用路径均保持原样。

追踪结果记录、Scope 关闭和 Span 结束使用 best-effort 隔离；缺少追踪 Bean或 `reliable-event.tracing-enabled=false` 时继续使用未追踪消费路径。每条正常开始的消息都会关闭 Scope，使正常、业务异常和 ACK 异常后都恢复 ambient Context。新增实现只在原创示例，不提供通用消费者框架或消费回写能力。

示例增加 Boot Actuator 与 `micrometer-tracing-bridge-otel`，显式启用 W3C 传播，并把 demo 采样率默认设为 100%，可用 `EXAMPLE_TRACE_SAMPLING_PROBABILITY` 覆盖。没有在示例或 Starter 中强制安装 OTLP exporter；接入方配置 exporter 和采样率。Tempo/Compose、Prometheus 看板和告警留在 M8.5。

## 验收证据

- 真实 E2E 使用已有 MySQL 8.0.36、RocketMQ 5.5.0、随机端口 Spring Boot HTTP server 和 Starter 自动 Worker/consumer。测试从 HTTP 请求设置固定上游 `traceparent`，内存 SpanExporter 接入 Boot 自动配置的同一个 OpenTelemetry SDK。断言真实 HTTP SERVER Span 的远程 parent、登记 Span 的 parent、实际自动发布 attempt 的 parent、RocketMQ probe consumer 读取到的 `traceparent` 中的 attempt SpanId，以及消费 Span 的 trace/parent 与 `processed`、ACK success 属性；消费效果只落一行。E2E 通过。
- 真实 MySQL consumer-loop fixture 使用真实事务代理 handler 和无追踪的旧式消息属性。非法业务消息因事务失败不 ACK；下一条消息提交处理后模拟 ACK 异常；再次处理同身份消息记录 `idempotent_skip` 并 ACK，数据库仍只有一次消费身份和一次业务效果。消费 Span 分开表达业务结果和 ACK 结果。fixture 通过。
- 6 项轻量消费追踪测试覆盖缺失/无效 parent 的显式 root、大小写不敏感 W3C 提取且不读取 baggage、追踪开始/记录/关闭故障后的 handler 和 ACK、原构造路径无 Tracer、配置关闭时不解析可选追踪 Bean，以及使用真实 Micrometer OTel Bridge 和 OTel SDK 内存 exporter 验证正常/handler 异常后 ambient Span 都恢复且消息 Span 不以 ambient 为 parent。测试通过。
- focused 单测和两条真实服务验收在全仓回归前均已通过；一次后续 focused 验收进程被中断，其临时控制台文件未保留，因此不作为证据引用。最终全仓 clean verify 覆盖全部新增单测与两条 E2E。clean 前保留 M8.1–M8.3 的 `target/evidence` 证据。
- 端到端测试没有连接 Tempo 或验证 OTLP exporter 后端；只证明了真实业务链路和内存 SpanData 的 parent/trace 关系。生产部署后端的验证由 M8.5 负责。

最终 Java 17 全仓 `mvn -o clean verify`：**通过**（JDK 17.0.12，8 个模块，34 份 Surefire XML，共 204 个测试，0 失败、0 错误、0 跳过；2026-10-02，用时 5 分 54 秒）。日志和退出码：[m8.4-full-verify.log](/D:/trae/scp/reliable-event/target/evidence/m8.4-full-verify.log)、[m8.4-full-verify.exit-code.txt](/D:/trae/scp/reliable-event/target/evidence/m8.4-full-verify.exit-code.txt)。

## 运行边界

消费者 Trace 不是业务事实账本。操作员仍以业务表、去重表、Broker 和 Outbox 状态核对消费结果；生产端 `PUBLISHED` 不代表消费提交。外部消费者需自行实现事务性幂等和消费追踪。当前示例演示 W3C traceparent/tracestate，不自动传播 baggage；关闭采样或没有 exporter 只影响追踪数据，不改变订单业务和 ACK 协议。
