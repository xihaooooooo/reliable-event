# M6 死信查询与受控重放完成记录

> 2026-09-29，M6.1 至 M6.4 完成。以下结果针对当前工作区源码；正式 `0.1.0` 发布仍按 [M5.4 检查](../implementation/M5_4_RELEASE_CHECK.md)另行决定。

## 交付范围

- JDBC 模块提供只读 `DEAD` 列表、详情与分页，以及带预期版本的单条重放；成功重新入队和审计插入在同一事务中提交。列表不包含业务键和错误摘要，详情不读取 Payload 或 Headers。
- Starter 仅在 `reliable-event.dead-operations-enabled=true` 时装配 `DeadEventOperations` Java Bean；没有管理 HTTP 端点，调用应用负责鉴权、真实操作者身份和操作原因。
- 重放保留事件 ID、类型、业务键、Payload、Headers 和原 `max_attempts`，清零 `attempt_count` 后交还现有扫描、抢占、发送与重试链路。`first_available_at` 不重置，发布延迟仍从最初可用时间计算。
- [正式 Outbox SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)包含死信查询索引；已有表使用 [M6.1 增量 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m6-1.sql)。重放还需预建[审计表](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-replay-audit-m6-2.sql)。库不自动迁移。

## 可复现验收

在 Java 17、Docker Engine 可用且本机 8081 端口空闲的环境，从仓库根目录执行：

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-17' # 按本机 JDK 17 路径调整
mvn -pl reliable-event-example -am '-Dtest=ExampleApplicationEndToEndTest' '-Dsurefire.failIfNoSpecifiedTests=false' test
mvn clean verify
python -m unittest discover -s reliable-event-benchmark/scripts/tests -v
```

端到端测试使用 Testcontainers 启动 MySQL 8.0.36 与 RocketMQ 5.5.0，创建 Topic、消费者组、Outbox 和审计表，启动只依赖 Starter 的公开订单应用。操作步骤编码在 [ExampleApplicationEndToEndTest](../../reliable-event-example/src/test/java/dev/reliableevent/example/ExampleApplicationEndToEndTest.java)：

1. `twoOperatorsCompeteToReplayADeadEventAfterRepairingItsDestination`：通过缺失目标映射使新订单事件进入 `DEAD`；经 Starter 的 `DeadEventOperations` 列表与详情取得事件及版本；两个操作者同时请求重放。断言恰好一次 `Replayed`、一次 `NotDead`，审计表只有一行且记录旧/新版本及实际操作者。以正确映射重启应用并打开 Starter 调度，观察原事件 `PUBLISHED`、消费者去重行一条、业务效果一份。
2. `replayAfterUnknownResultKeepsIdentityAndConsumerEffectIdempotent`：真实发送成功后丢弃回执，使本轮最大尝试次数为 1 的事件进入 `DEAD`。先从 Broker 探针接收第一条消息并核对消费者效果，再用公开接口重放，显式推进原发布 Worker。探针接收第二条消息：两条 Broker Message ID 不同，稳定事件 ID、类型和业务键相同；Outbox 最终 `PUBLISHED`，消费者业务效果仍只有一次。

第二条路径是确定性故障注入，模拟成功回执丢失，不代表真实 Broker 总会丢失回执。测试中的人工操作绕过任何 HTTP 层；实际接入应按[运维指南](../OPERATIONS.md)在应用层授权、核对外部事实并追踪审计 ID。重放可能造成重复投递，不提供恰好一次保证。

## 验证结果

- JDK 17.0.12 下，M6.4 所在的公开示例端到端测试 4/4 通过，包括原有的普通自动发送和结果未知自动重试两条路径。
- 全仓 `mvn clean verify`：8 个 Maven 模块构建成功；27 份 Surefire 报告共 148 个测试，0 失败、0 错误、0 跳过。覆盖 JDBC、独立 JVM 故障、真实 RocketMQ、Starter 和公开示例。
- 基准报告契约测试：3/3 通过。M5.3 的 26 轮性能矩阵是 2026-09-26 的历史结果，本次没有重跑。

## 剩余边界与后续

- 端到端验收使用单 Broker 测试环境。缺失映射和丢弃成功回执由测试确定性注入；没有声称覆盖所有生产网络、权限或 Broker 故障组合。
- 第一条路径验证了重放后 Starter 自动调度；第二条路径为精确核对两次消息而显式调用现有 JDBC Worker。没有提供通用消费者框架、自动/批量重放、管理界面或内置权限系统。
- 审计表和 Outbox 不会自动清理；操作者、原因和旧错误摘要需要受限访问及保留策略。
- M6 新代码和迁移不属于此前 M5.4 发布候选检查的源码。`0.1.0-SNAPSHOT` 尚未公开发布；若要正式发布，须固定最终 commit、重做 M5.4 检查并验证外部工件下载。
