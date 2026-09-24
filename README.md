# ReliableEvent

ReliableEvent 是一个面向 Spring Boot 3 与 RocketMQ 的可靠消息 Starter。

它通过 Transactional Outbox 模式，让业务数据与待发布事件在同一个 MySQL 本地事务中提交，再由后台发布器完成消息发送、失败重试、租约恢复和死信处理。

当前仓库已完成 M0 至 M4.2：业务方可以在活动事务中登记事件，模块会持久化 JSON Payload、避免重复登记，并由发布器扫描到期事件。发布器使用候选版本进行条件抢占，在事务外通过 RocketMQ 5.x gRPC 适配同步发送，再按抢占版本、租约 Owner 和有效期更新状态。Starter 已能自动装配所需 Bean；当前仍需显式执行一次发布周期，后台常驻调度留在 M4.3。

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

当前进度对应的简历表述见 [简历项目文案](docs/RESUME_PROJECT.md)。

文档导航及当前第一步见 [项目文档](docs/README.md)。

## 验证当前实现

启动 Docker 后执行：

```bash
mvn verify
```

## 当前 Starter 接入方式

引入 `dev.reliableevent:reliable-event-spring-boot-starter:0.1.0-SNAPSHOT`，配置应用的数据源并创建 [Outbox 表](reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)，然后提供 RocketMQ 5.x Proxy 地址与事件目标：

```yaml
reliable-event:
  rocketmq:
    endpoints: localhost:8081
    mappings:
      coupon-task-execute:
        destination: coupon-task-topic:execute
```

在业务事务内调用 `ReliableEventPublisher.publish(event)`；当前由调用方显式调用 `JdbcEventPublicationCycle.runOnce()` 完成一轮租约恢复和到期事件发送。Starter 不在应用启动后自动循环发送。配置项和覆盖规则见 [M4.2 实施文档](docs/implementation/M4_2_SPRING_BOOT_AUTOCONFIGURE_AND_STARTER.md)。

## 项目原则

1. 不自研消息代理，不取代 RocketMQ。
2. 不承诺 Exactly Once，下游必须按事件键实现幂等。
3. 不用功能数量证明价值，用事务测试、并发测试、故障注入和基准测试证明行为。
4. 第一版只支持单数据源 MySQL 8.0 和 RocketMQ。
5. 优惠券项目仅作为私有落地验证，不复制受版权保护的代码到本仓库。
