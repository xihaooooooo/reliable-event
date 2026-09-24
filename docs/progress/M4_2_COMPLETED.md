# M4.2 阶段完成记录

## 已实现

- 新增 `reliable-event-spring-boot-autoconfigure` 与只聚合依赖的 `reliable-event-spring-boot-starter`；通过 Spring Boot 3 的 `AutoConfiguration.imports` 发现配置。
- `reliable-event.*` 属性绑定提供严格未知字段校验、启动期值校验、`topic[:tag]` 映射解析和配置元数据。尚未支持的调度、线程池、清理等属性不会被静默当成已生效。
- 在单数据源和事务管理器可确定时装配 JDBC Publisher、退避策略、租约恢复、Worker 与手动执行的单轮编排。新增公开的可配置 `maxAttempts` Publisher 构造器，原有事务校验保持不变。
- 按条件装配 RocketMQ Provider、ClientConfiguration、Producer、Resolver 和 Sender。默认 Producer 从映射去重预声明 Topic，SDK `maxAttempts` 固定为 `1`，Context 关闭时释放；用户提供的同类型 Bean 优先。
- 支持可选的 Access Key、Secret Key 或用户提供的凭据 Provider。缺少数据源或客户端类时自动配置退让；启用且本地配置错误时启动失败。
- Starter 明确引入 JSON starter，保证最小 Spring Boot 应用能获得 `ObjectMapper`。

## 验证

- 15 个自动配置 Context 测试覆盖禁用开关、用户 Bean 覆盖、缺少依赖、YAML Event Type 绑定、Producer 单次尝试与关闭、凭据优先级、非法配置和未来属性拒绝。
- 1 个 Starter 端到端集成测试通过真正的 Spring Boot 自动配置，在 MySQL 8.0.36 事务内登记事件，显式运行 `JdbcEventPublicationCycle.runOnce()`，并从 RocketMQ 5.5.0 消费、核对消息与 Outbox `PUBLISHED` 状态。
- `mvn clean verify` 全仓通过；最终测试总数为 107 个，0 失败、0 错误、0 跳过。

## 当前边界

- 应用启动不会自动扫描或发布，必须显式调用单轮编排；常驻调度与有界并发属于 M4.3。
- 默认 Producer 的 Bean 关闭已实现；停止抢占并等待在途发送的优雅停机属于 M4.4。
- Micrometer 指标和结构化生产日志属于 M4.5。
- `DEAD` 人工重放、租约续期、消费者幂等以及真实生产集群故障演练仍未实现。
- 仍为至少一次投递，Broker 已收消息但 Outbox 未更新时可能重复；消费者必须按稳定事件 ID 或业务 Key 去重。

## 下一步

进入 M4.3：在现有单轮编排之上增加常驻调度和有界并发，并把每轮抢占规模限制在本地实际执行容量内。
