# ReliableEvent

ReliableEvent 是一个面向 Spring Boot 3 与 RocketMQ 的可靠消息 Starter。

它通过 Transactional Outbox 模式，让业务数据与待发布事件在同一个 MySQL 本地事务中提交，再由后台发布器完成消息发送、失败重试、租约恢复和死信处理。

当前仓库处于设计冻结阶段，尚未进入编码。

## 已确定的方向

- Java 17
- Spring Boot 3
- MySQL 5.7
- RocketMQ
- 至少一次投递语义
- 多实例安全抢占
- 指数退避重试
- 租约与宕机恢复
- 死信和 Micrometer 指标

完整范围、语义和验收标准见 [项目方向文档](docs/PROJECT_DIRECTION.md)。

当前进度对应的简历表述见 [简历项目文案](docs/RESUME_PROJECT.md)。

文档导航及当前第一步见 [项目文档](docs/README.md)。

## 项目原则

1. 不自研消息代理，不取代 RocketMQ。
2. 不承诺 Exactly Once，下游必须按事件键实现幂等。
3. 不用功能数量证明价值，用事务测试、并发测试、故障注入和基准测试证明行为。
4. 第一版只支持单数据源 MySQL 5.7 和 RocketMQ。
5. 优惠券项目仅作为私有落地验证，不复制受版权保护的代码到本仓库。
