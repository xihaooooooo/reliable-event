# ReliableEvent

ReliableEvent 是一个面向 Spring Boot 3 与 RocketMQ 的可靠消息 Starter。

它通过 Transactional Outbox 模式，让业务数据与待发布事件在同一个 MySQL 本地事务中提交，再由后台发布器完成消息发送、失败重试、租约恢复和死信处理。

当前仓库已完成完整 M2、M3.1 和 M3.2：业务方可以在活动事务中登记事件，模块会持久化 JSON Payload、避免重复登记，并由发布器扫描到期事件。发布器使用候选版本进行条件抢占，在事务外通过 Fake 发送适配完成发送，再按抢占版本、租约 Owner 和有效期更新状态。

版本号条件抢占已经通过真实 MySQL 8.0 双 Worker 并发竞争测试。发送失败后事件会按照带随机抖动的指数退避进入 `RETRY_WAIT`；达到最大尝试次数或发生明确不可重试错误时进入 `DEAD`，不再自动扫描。租约使用数据库时间计算，错误 Owner、旧版本和过期租约都不能完成状态更新；过期的 `PUBLISHING` 事件可以限量、按快照条件恢复为 `RETRY_WAIT` 或 `DEAD`。多恢复者并发接管验证将在 M3.3 完成。

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

## 项目原则

1. 不自研消息代理，不取代 RocketMQ。
2. 不承诺 Exactly Once，下游必须按事件键实现幂等。
3. 不用功能数量证明价值，用事务测试、并发测试、故障注入和基准测试证明行为。
4. 第一版只支持单数据源 MySQL 8.0 和 RocketMQ。
5. 优惠券项目仅作为私有落地验证，不复制受版权保护的代码到本仓库。
