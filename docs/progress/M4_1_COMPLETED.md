# M4.1 阶段完成记录

## 这一阶段做了什么

- 新增 `reliable-event-core`，把公共事件 API 和 Sender 内部协议从 JDBC 模块中拆出，避免 RocketMQ 适配反向依赖 JDBC 实现；
- 新增 `reliable-event-rocketmq`，固定使用 `rocketmq-client-java 5.2.1`；
- 实现 `RocketMqEventSender`、`RocketMqDestination`、`EventDestinationResolver` 和不可变的 `MapEventDestinationResolver`；
- 固定普通消息映射：Event Type 映射 Topic/Tag，Event Key 映射 RocketMQ Key，数据库 Payload 原文作为 UTF-8 Body，安全 Headers 映射用户属性；
- 增加 `reliable_event_id`、`reliable_event_type` 和 `reliable_event_key` 三个保留属性，供下游稳定识别和幂等；
- 在调用 Producer 前校验 Topic、Tag、Body 字节数和 Header 的名称、数量、值大小及总大小；
- 扩展发送失败协议，显式区分 `RETRYABLE`、`NON_RETRYABLE` 和 `RESULT_UNKNOWN`；
- Producer 使用同步 `send`，SDK 内部最大尝试次数固定为 `1`，持久化重试仍由 Outbox 状态机负责；
- 生产 Sender 不拥有 Producer 生命周期，为后续 M4.2 的 Spring Bean 生命周期装配保留清晰边界。

## 真实环境验证

测试固定使用 MySQL 8.0.36、`apache/rocketmq:5.5.0`、内置 Proxy 和 RocketMQ 5.x SimpleConsumer。测试夹具先等待 Broker 在 NameServer 中注册，再创建 Topic 和消费组；Proxy 的 gRPC 端口保持为对外可达的 8081，避免路由返回地址绕过随机端口映射。

已验证：

- 事务内登记的 Outbox 事件经 Worker 发送后进入 `PUBLISHED`，`attempt_count = 1`；
- 消费端观察到的 Topic、Tag、Key、Body、用户属性和三个保留属性均符合映射；
- 缺失 Event Type 映射时不调用 Producer，事件第一次尝试即进入 `DEAD`；
- Body 超过显式限制时不调用 Producer，错误摘要只记录大小而不泄漏 Body；
- Producer 建立连接后暂停 Broker/Proxy 容器，发送在有界超时内失败并进入 `RETRY_WAIT`；恢复容器并推进到 `next_attempt_at` 后，第二次发送成功并进入 `PUBLISHED`；
- 第一次真实发送成功后，测试装饰器丢弃 Receipt 并抛出 `RESULT_UNKNOWN`；Outbox 重试后，消费者观察到两条事件 ID、业务 Key 和 Body 相同，但 Broker Message ID 不同的消息；
- 所有启动、路由可见性、接收、故障和进程操作都有上限，没有使用固定睡眠猜测状态。

## 验证结果

执行 `mvn clean verify`，以 Java 17 字节码目标完成 91 个测试：

- 51 个单元测试；
- 40 个 MySQL、独立 JVM 或真实 RocketMQ 集成测试；
- 0 个失败、0 个错误、0 个跳过。

其中 RocketMQ 模块包含 15 个映射、校验和异常分类单元测试，以及 6 个真实 Broker 集成测试。

## 当前边界

- 尚未提供 Spring Boot 自动配置、外部属性绑定和 Producer Bean 生命周期管理；
- Worker 仍需显式调用，没有常驻调度和有界并行执行；
- 没有优雅停机、租约续期、Micrometer 指标和结构化生产日志；
- Broker Message ID 没有持久化到 Outbox；
- `DEAD` 暂无人工重放接口；
- 没有实现消费者幂等，调用方仍必须使用稳定事件 ID 或业务 Key 去重；
- 当前验证是单 Broker 测试环境，不代表已经完成生产集群网络分区或多副本故障演练。

因此当前可以表述为已经实现并用真实 Broker 验证 RocketMQ 普通消息发送适配，但不能表述为完成 Starter、后台自动发布、生产可观测性或 Exactly Once。

## 下一步

进入 M4.2，实现 Spring Boot 自动配置与 Starter：配置属性、条件装配、Producer 生命周期、目标映射绑定和启动期校验。M4.2 只负责装配，不重新打开 M4.1 已冻结的消息映射与发送语义。
