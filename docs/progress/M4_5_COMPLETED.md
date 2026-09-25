# M4.5 阶段完成记录

## 已实现

- 在 Outbox 中保存不可变的 `first_available_at`，新表 SQL 与旧表增量迁移 SQL 均已提供。存量行可以为 `NULL`；这类行正常发布，但不生成失真的发布延迟样本。
- JDBC 发布和租约恢复通过可选的内部观测回调接入 Micrometer。发送成功、失败和耗时按单次 Sender 调用统计；成功延迟从首次计划可用时间起算；租约恢复仅在条件更新成功后计数。
- `PENDING + RETRY_WAIT` 积压量及 `DEAD` 数量由数据库状态聚合查询采样，Gauge 读取缓存快照。首次采样前为 `NaN`；没有 `MeterRegistry` 时使用无操作回调，不影响发布。
- 生产日志加入可检索的事件名、事件 ID/类型/键、尝试次数、租约 Owner、发送结果和成功 Message ID。Broker 回执成功与 Outbox `PUBLISHED` 完成分别记录；异常仅记录分类或类名，不输出 Payload、Header、凭据及完整异常消息。
- 默认自动扫描和手动 `runOnce()` 都会刷新 Gauge 快照；禁用 Starter 不创建指标运行时。指标注册随 Spring Context 关闭释放。

## 验证

- 真实 MySQL 8.0.36 测试验证首次可用时间在重复登记和改写下次尝试时间后不变，旧表迁移保留存量未知值，以及积压、死信按数据库状态计数。
- `SimpleMeterRegistry` 测试验证成功、结果未知、耗时、发布延迟、两种租约恢复结果和 Gauge；Context 测试验证存在 Registry 时自动接入。观测回调抛异常时，真实 JDBC 发布仍写为 `PUBLISHED`，且日志不包含 Payload/Header 哨兵值。
- 真实 RocketMQ 5.5.0 测试验证 Starter 发送完成后成功计数、耗时、延迟、Gauge 与 Outbox `PUBLISHED` 对应；此前的 Broker 不可用、结果未知重投、双实例抢占、停机及租约恢复测试保持通过。
- 使用 JDK 21 运行、Java 17 字节码目标执行 `mvn clean verify`：全仓 **127 个测试，0 失败、0 错误、0 跳过**。

## 接入与当前边界

- 已有 M4.4 表须先执行 [`reliable-event-outbox-m4-5.sql`](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m4-5.sql)，新建表使用更新后的 [`reliable-event-outbox.sql`](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)。存量事件没有可信的首次可用时间，不能据此回填延迟。
- 只有应用提供 `MeterRegistry` 时才注册 Micrometer 指标；项目不自动配置监控导出端点。Gauge 是同一数据库的快照，多实例上的值不能相加。
- `publish.success` 是 Sender 成功回执次数，不等于唯一事件数、Outbox 已完成次数或消费者处理次数；结果未知后仍可能重复投递。`publish.failure` 包含结果未知的发送尝试。
- `runOnce()` 仍不参与默认调度器的容量和停机协调；租约续期、死信人工重放、消费者幂等、业务私有接入和基准测试仍未实现。

## 下一步

进入 M5：原创最小示例、优惠券场景私有接入、可复现基准测试和 `0.1.0` 发布检查。语义及验收口径见 [M4.5 实施文档](../implementation/M4_5_OBSERVABILITY_AND_M4_ACCEPTANCE.md)。
