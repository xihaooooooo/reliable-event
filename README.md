# ReliableEvent

ReliableEvent 是一个面向 Spring Boot 3 与 RocketMQ 的可靠消息 Starter。

它通过 Transactional Outbox 模式，让业务数据与待发布事件在同一个 MySQL 本地事务中提交，再由后台发布器完成消息发送、失败重试、租约恢复和死信处理。

当前仓库已完成 M0 至 M5.1、独立的 M5.3 基准测试、M6.1 至 M6.4 死信操作，以及 M8.1 至 M8.4 追踪和运行指标；M5.2 私有优惠券链路已取消，不属于 `0.1.0` 发布条件。业务方可以在活动事务中登记事件，模块会持久化 JSON Payload、避免重复登记。Starter 启动后按固定延迟恢复过期租约并扫描到期事件，将候选放入有界本地执行器；候选只有在线程开始执行时才使用版本号条件抢占，在事务外通过 RocketMQ 5.x gRPC 适配同步发送，再按版本、租约 Owner 和有效期更新状态。Context 关闭时会停止新抢占、撤销排队候选，并在有界时间内等待在途发送和状态更新。存在 `MeterRegistry` 时还会记录发布、延迟、积压、死信及租约恢复指标；默认 Starter 自动运行时还可使用独立的 Outbox 聚合快照采样器。

版本号条件抢占已经通过真实 MySQL 8.0 双 Worker 并发竞争测试。发送失败后事件会按照带随机抖动的指数退避进入 `RETRY_WAIT`；达到最大尝试次数或发生明确不可重试错误时进入 `DEAD`，不再自动扫描。租约使用数据库时间计算，错误 Owner、旧版本和过期租约都不能完成状态更新；过期的 `PUBLISHING` 事件可以限量、按快照条件恢复为 `RETRY_WAIT` 或 `DEAD`。独立 JVM 故障测试验证了至少一次语义窗口；真实 RocketMQ 5.5.0 测试进一步验证了 Topic/Tag/Key/Body/属性映射、Broker 不可用恢复，以及结果未知后重投产生不同 Message ID 的预期重复消息。

## 已确定的方向

- Java 17
- Spring Boot 3
- MySQL 8.0
- RocketMQ
- 至少一次投递语义
- 多实例安全抢占
- 指数退避重试
- 租约与宕机恢复
- 死信和 Micrometer 指标

完整范围、语义和验收标准见 [项目方向文档](docs/PROJECT_DIRECTION.md)。

文档导航及当前第一步见 [项目文档](docs/README.md)。

可按 [原创订单示例](reliable-event-example/README.md) 从空 MySQL 与 RocketMQ 环境启动，观察业务事务、自动发布、Broker 故障恢复和消费幂等。

M8.5 的独立 Prometheus、Grafana、Tempo 与 Alertmanager 本地演示见[观测栈说明](observability/README.md)；它复用该订单示例及其 MySQL/RocketMQ Compose 服务。

可按 [基准测试模块](reliable-event-benchmark/README.md) 在独立 MySQL 与 RocketMQ 环境运行 M5.3 实验并生成原始样本、执行计划和报告。本次 26 轮实测、结果解释及原始证据见 [M5.3 完成记录](docs/progress/M5_3_COMPLETED.md)。

## 验证当前实现

M7 在原发布链路之外加入永久身份表与默认关闭的已发布行清理。M5.3 的旧性能数字尚未按新登记协议复测，M7 也未进入此前的正式发布候选检查。

启动 Docker 后执行：

```bash
mvn verify
```

## 持续集成（CI）

[GitHub Actions 配置](.github/workflows/ci.yml)在分支推送、Pull Request 和手动触发时执行验证。运行环境为 Ubuntu 24.04、Java 17 和 Python 3.13；Testcontainers 自动创建测试用 MySQL 与 RocketMQ 容器，无需配置外部数据库、消息服务或发布凭据。

流水线检查 Docker 可用性，运行基准报告的 Python 测试和全仓 `mvn clean verify`，并检查 Maven 报告中的测试数、失败、错误与跳过数。没有测试报告、没有执行任何测试或存在跳过测试时，CI 会失败。RocketMQ 测试使用固定端口 `8081`，因此 Maven 模块保持顺序运行。

将配置随代码提交并推送到 GitHub 后，在仓库 **Actions → CI** 查看结果。运行详情的 **Artifacts** 提供测试报告与日志；全部检查成功后还会保存 JAR。日志和报告保留 14 天，JAR 保留 7 天。同一分支或 PR 的新提交会取消旧的运行。

首次运行成功后，可以在 GitHub 仓库的分支规则中将 `Build and test` 设置为合并前必须通过的检查。CI 验证自动化回归，正式发布仍须完成[发布检查](docs/implementation/M5_4_RELEASE_CHECK.md)中的其他验收项。

## 工件与许可

源码采用 [Apache License 2.0](LICENSE)。当前 Maven 版本仍为 `0.1.0-SNAPSHOT`，尚未发布到公开工件仓库；接入前可在本仓库根目录运行 `mvn -pl reliable-event-spring-boot-starter -am -DskipTests install` 安装本地工件。正式发布与版本号以 [M5.4 发布检查](docs/implementation/M5_4_RELEASE_CHECK.md)的结论为准。

计划对外提供父 POM `dev.reliableevent:reliable-event-parent` 和五个库工件：`reliable-event-core`、`reliable-event-jdbc`、`reliable-event-rocketmq`、`reliable-event-spring-boot-autoconfigure`、`reliable-event-spring-boot-starter`。示例与基准模块是源码仓库中的验证材料，不作为库工件发布。

## 当前 Starter 接入方式

M7 起，新建库的正式建表 SQL 同时创建 Outbox 和永久登记身份表。已有 Outbox 表的部署必须先按[运维指南](docs/OPERATIONS.md#建表和迁移)暂停事件登记、回填及核对身份，再切换所有应用实例；不能仅重新执行 `CREATE TABLE IF NOT EXISTS` 完成升级。

引入 `dev.reliableevent:reliable-event-spring-boot-starter:0.1.0-SNAPSHOT`，配置应用的数据源并创建 [Outbox 表](reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)。已有 M4.4 表先执行 [M4.5 增量 SQL](reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m4-5.sql)。然后提供 RocketMQ 5.x Proxy 地址与事件目标：

```yaml
reliable-event:
  rocketmq:
    endpoints: localhost:8081
    mappings:
      order-created:
        destination: orders-topic:created
```

`orders-topic` 是配置示意，使用前须创建目标 Topic。在业务事务内调用 `ReliableEventPublisher.publish(event)`，Starter 默认自动持续发布。可通过 `reliable-event.poll-interval`、`worker-threads`、`worker-queue-capacity` 和 `claim-batch-size` 控制扫描与本地容量。可选的 `adaptive-polling-enabled=true` 使有在途任务或本轮提交任务时按 `active-poll-interval` 继续扫描，空闲及扫描失败时回到 `poll-interval`；默认保持固定延迟扫描。`shutdown-timeout` 控制停机时等待在途任务的上限，默认 `20s`，应小于 Spring 的 `spring.lifecycle.timeout-per-shutdown-phase`。设置 `scheduling-enabled=false` 后保留显式调用 `JdbcEventPublicationCycle.runOnce()` 的方式。完整配置、迁移和排障步骤见 [接入与运维指南](docs/OPERATIONS.md)；容量协议见 [M4.3 实施文档](docs/implementation/M4_3_SCHEDULING_AND_BOUNDED_CONCURRENCY.md)，停机语义见 [M4.4 实施文档](docs/implementation/M4_4_GRACEFUL_SHUTDOWN.md)。

提供 Micrometer `MeterRegistry` 后，Starter 注册 [M4.5 指标](docs/implementation/M4_5_OBSERVABILITY_AND_M4_ACCEPTANCE.md)；不提供 Registry 时发布功能照常运行。`reliable_event.publish.success` 表示生产端成功回执次数，不代表唯一事件数或消费者处理完成。数据库积压和死信 Gauge 在多实例上展示同一份表的快照，不应跨实例求和。

处理 `DEAD` 事件时，先执行[审计表迁移](reliable-event-jdbc/src/main/resources/schema/reliable-event-replay-audit-m6-2.sql)，再设置 `reliable-event.dead-operations-enabled=true`，即可向应用注入 `DeadEventOperations` 查询与单条重放接口。默认不开启管理 Bean，也不提供 HTTP 端点。调用方须鉴权、核对 Broker 和消费者事实，并保存真实操作者与原因；步骤见[运维指南](docs/OPERATIONS.md)和[公开示例](reliable-event-example/README.md#人工处理-dead-事件)。

M7 将 `(event_type, event_key) → EventId` 永久保存在 `reliable_event_identity`。完成迁移核对后，可显式配置 `published-retention-enabled=true` 与正数 `published-retention`，按批次清理到期的 `PUBLISHED` 行。清理默认关闭；清理后重复登记仍返回原 ID，不产生新消息。清理一旦执行，就不能回退到旧版只写 Outbox 的发布器。M7 的当前源码尚未纳入此前的正式发布检查。

## 项目原则

1. 不自研消息代理，不取代 RocketMQ。
2. 不承诺 Exactly Once，下游必须按事件键实现幂等。
3. 不用功能数量证明价值，用事务测试、并发测试、故障注入和基准测试证明行为。
4. 第一版只支持单数据源 MySQL 8.0 和 RocketMQ。
5. 原创示例承担公开端到端验证；M5.2 私有优惠券链路已取消，不把未经完整验收的私有接入作为项目成果。
