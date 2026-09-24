# M4.2：Spring Boot 自动配置与 Starter

> 状态：已完成
>
> 基线：M4.1 已完成；Java 17、Spring Boot 3.5.16、RocketMQ Java Client 5.2.1、MySQL 8.0
>
> 目标：业务应用引入 Starter 并提供必要配置后，自动获得 `ReliableEventPublisher`、RocketMQ Producer、目标映射及可手动执行的单轮发布组件；配置错误在启动时暴露。此阶段不启动后台循环。
>
> 实施结果与回归数据见 [M4.2 阶段完成记录](../progress/M4_2_COMPLETED.md)。

## 为什么单独实施

M4.1 已用真实 Broker 验证 `RocketMqEventSender`，但集成测试仍手动创建 Producer、映射、Publisher、Worker 和恢复组件。M4.2 将这些既有对象接入 Spring Boot 的属性绑定、条件装配和 Bean 生命周期。它只改变对象如何创建，不改变发送消息、数据库状态转换或至少一次投递语义。

阶段顺序保持为：

```text
M4.2 自动配置与 Starter → M4.3 常驻调度与有界并发
                        → M4.4 生命周期与优雅停机
                        → M4.5 指标、日志与 M4 总验收
```

M4.2 管理 Producer Bean 的创建和关闭；M4.4 再处理停止抢占、等待在途发送等运行时优雅停机问题。

## 模块与依赖

新增两个模块：

```text
reliable-event-spring-boot-autoconfigure
  ├── reliable-event-core
  ├── reliable-event-jdbc
  └── reliable-event-rocketmq

reliable-event-spring-boot-starter
  └── reliable-event-spring-boot-autoconfigure
      + JDBC、RocketMQ 等运行所需依赖
```

- `autoconfigure` 放属性类型、校验和 Bean 定义；对可选技术依赖使用 Maven `optional`，让单独引用该模块的应用能够按类路径条件退让。引用 RocketMQ 或 JDBC 类型的配置放在受 `@ConditionalOnClass` 保护的独立嵌套配置中，避免缺类时提前加载失败。
- `starter` 是依赖聚合，不放业务逻辑、组件扫描入口或定时任务。它传递 JDBC、RocketMQ 客户端以及自动配置所需依赖。
- 使用 Spring Boot 3 的 `@AutoConfiguration`，在 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 注册；不依赖业务包扫描发现配置类。
- 生成并检查配置元数据，使 IDE 能显示本阶段实际支持的属性。

生产依赖仍保持 `jdbc` 和 `rocketmq` 各自只依赖 `core`，由自动配置模块组合二者。

## 本阶段支持的配置

沿用 `reliable-event` 前缀。以下示例仅列 M4.2 实际生效的属性：

```yaml
reliable-event:
  enabled: true
  claim-batch-size: 50
  recovery-batch-size: 50
  lease-duration: 30s
  max-attempts: 8
  initial-retry-delay: 1s
  max-retry-delay: 5m
  rocketmq:
    endpoints: localhost:8081
    request-timeout: 5s
    ssl-enabled: false
    max-body-bytes: 4194304
    mappings:
      coupon-task-execute:
        destination: coupon-task-topic:execute
      coupon-remind:
        destination: coupon-reminder-topic
```

| 属性 | 默认值 | 约束与用途 |
| --- | --- | --- |
| `enabled` | `true` | `false` 时不创建本项目的默认 Bean，也不连接 Broker |
| `claim-batch-size` | `50` | 正整数；传给 `JdbcEventPublicationWorker` |
| `recovery-batch-size` | `50` | 正整数；传给 `JdbcExpiredLeaseRecovery` |
| `lease-duration` | `30s` | 至少 1 ms，且能安全转换为现有 SQL 使用的微秒数 |
| `max-attempts` | `8` | 正整数；新登记事件写入 Outbox 时使用，已有记录不追溯修改 |
| `initial-retry-delay` | `1s` | 至少 1 ms |
| `max-retry-delay` | `5m` | 不小于初始退避时间 |
| `rocketmq.endpoints` | 无 | 默认 Producer 路径必填，指向 RocketMQ 5.x Proxy 的 gRPC 接入地址 |
| `rocketmq.request-timeout` | `5s` | 正时长；传给客户端配置 |
| `rocketmq.ssl-enabled` | `false` | 是否对 RocketMQ gRPC 连接启用 TLS |
| `rocketmq.max-body-bytes` | `4194304` | 正整数；传给现有消息工厂，不改变 M4.1 默认上限 |
| `rocketmq.mappings` | 空 | 创建默认 Resolver 时必须至少有一个 Event Type 映射 |

固定的退避抖动比例仍为现有默认值 `0.2`。若 M4.2 暴露退避配置，必须使用同一组属性同时构造发布 Worker 与租约恢复组件的 `ExponentialBackoff`，避免二者策略分叉。

`PROJECT_DIRECTION.md` 中的 `poll-interval`、`worker-threads`、`worker-queue-capacity`、`published-retention` 属于后续运行时或清理能力，M4.2 不宣称它们已生效。`require-active-transaction` 当前也是不可关闭的发布前置条件；M4.2 不增加 `false` 分支。属性绑定对 `reliable-event` 使用严格的未知字段校验，并以测试证明上述键不会被静默忽略。

### 映射格式

`destination` 采用项目方向文档已有的 `topic[:tag]` 写法。启动时解析成 `RocketMqDestination`，再构造不可变的 `MapEventDestinationResolver`：

- 无冒号表示只有 Topic，Tag 为 `null`；
- 有一个冒号表示 Topic 和 Tag，任何一段为空都失败；
- 多个冒号、空 Event Type 或空映射均失败；同一 Event Type 在绑定后的 Map 中只能有一个目标；
- 不自动修剪、改写、推导 Topic/Tag；继续使用 M4.1 值对象的合法性校验；
- 多个 Event Type 可指向同一 Topic，Producer 预声明的 Topic 列表去重；
- 缺失运行时映射仍由 M4.1 的 Sender 分类为不可重试，不添加默认 Topic。

配置绑定对 `Map<String, ...>` 的 Event Type 键必须保留原始大小写及特殊字符（例如 `coupon-task-execute`）。测试需从实际 YAML 绑定验证，不能只直接实例化属性类。

### 凭据

M4.2 支持可选的 `rocketmq.access-key` 和 `rocketmq.secret-key`：两者同时缺省时不配置凭据；只给出一个时启动失败；同时给出时建立 SDK 的 `StaticSessionCredentialsProvider`。属性可从环境变量或外部密钥配置注入，错误消息、条件报告和测试输出均不得回显值。不要把凭据写入 Outbox Header 或目标映射。若用户提供自己的 `SessionCredentialsProvider` Bean，优先使用该 Bean，并避免再创建静态凭据提供者。

## 条件装配和覆盖规则

以 `reliable-event.enabled=true` 为总开关。默认 JDBC 路径要求单一可确定的 `DataSource`、`JdbcTemplate`、`PlatformTransactionManager` 和 `ObjectMapper`；默认 RocketMQ 路径要求 SDK 类在类路径上。缺少依赖时，自动配置退让并在 Spring 条件报告中说明原因。已经满足创建条件、但必填属性缺失或不合法时，启动失败并点名属性路径。

建议的默认 Bean 链：

```text
配置属性
  ├─ MapEventDestinationResolver
  ├─ ClientServiceProvider / ClientConfiguration / Producer
  ├─ RocketMqEventSender (EventSender)
  └─ JdbcReliableEventPublisher (ReliableEventPublisher)
       + JdbcEventPublicationWorker
       + JdbcExpiredLeaseRecovery
       + JdbcEventPublicationCycle
```

- 对每个可替换 Bean 使用对应类型的 `@ConditionalOnMissingBean`；用户 Bean 优先，默认实现退让。
- 自动创建的 Sender 需要默认 Resolver、Provider、Producer 和 `ObjectMapper`；用户提供 `EventSender` 时，不强制要求 RocketMQ Endpoint 或映射。
- 用户提供自定义 Resolver 且仍使用默认 Sender 时，还必须提供自定义 Producer：自动配置无法从任意 Resolver 枚举并预声明 Topic。缺少自定义 Producer 时启动失败。
- 用户提供 `Producer` 时直接复用，不对其调用 `close()`，也不覆盖其内部重试设置；文档明确该 Producer 必须自行符合 M4.1 的 `maxAttempts=1` 协议。
- 自动创建的 Producer 使用映射中去重后的 Topic，显式设置 `setMaxAttempts(1)`，通过 Bean 的销毁回调关闭；Sender 仍不拥有 Producer 生命周期。
- 用户提供 `ReliableEventPublisher` 时，默认 Publisher 退让。Worker 和恢复组件仍可独立装配，以支持用户只替换写入接口的场景。
- 使用类型条件与明确的单候选规则处理多数据源和多事务管理器；选择不明确时给出可操作的启动错误，不随机挑选 Bean。
- 不在自动配置类上使用普通 `@ComponentScan`，不覆盖宿主应用的 `ObjectMapper`、数据源和事务管理器。

M4.2 装配的 `JdbcEventPublicationCycle` 只提供显式 `runOnce()` 能力。应用启动或 Bean 初始化不能调用它，也不能创建定时器、后台线程或并行队列。`workerId` 在每个应用实例启动时生成，长度不超过现有 128 字符限制，同一实例的 Worker 生命周期内保持稳定；不能把固定默认 ID 复制到所有实例。

## 与现有代码的最小接缝调整

当前 `JdbcReliableEventPublisher` 只有公开的默认 `maxAttempts=8` 构造器；四参数构造器是包可见。要使属性真正生效，需提供一个公开且经过校验的可配置构造方式，保留现有便捷构造器和事务校验行为。不要通过反射改字段，也不要只绑定属性却继续调用默认构造器。

`JdbcEventPublicationWorker` 与 `JdbcExpiredLeaseRecovery` 已有可传入 `ExponentialBackoff` 的公开构造器，`JdbcEventPublicationCycle` 已有组合构造器。M4.2 应复用这些接缝，不改 SQL、Outbox 表结构、失败分类或状态机。

## 启动期校验与失败反馈

在连接 Broker 之前验证当前默认 Bean 路径需要的本地配置：正整数和时长、`max-retry-delay >= initial-retry-delay`、租约时长与请求超时的关系、Endpoint、凭据配对及映射格式。使用默认 Producer 时要求 `lease-duration > rocketmq.request-timeout`；否则单次同步发送尚未结束就可能丢失租约。实际生产网络预算仍由部署者设置。

异常信息应指明 `reliable-event.*` 的具体路径和约束，不包含凭据、Payload 或 Header 值。Producer 创建或 Broker 连接失败应使启动失败，不能把不存在的发送能力伪装成可用 Bean；相关客户端异常保留 cause 以便诊断。

`enabled=false` 时不进行需要外部服务的校验或连接。用户自行声明的 Bean 不受这个开关销毁或修改。

## 实施顺序

1. 新增 `autoconfigure` 与 `starter` 模块及自动配置注册文件，先验证独立引入和类路径退让。
2. 定义属性类型、映射解析和本地校验；补齐配置元数据与配置绑定测试。
3. 对 JDBC Publisher 做最小构造器调整，接入默认 Publisher、Worker、恢复组件和单轮编排。
4. 接入 SDK Provider、ClientConfiguration、Producer 与 Sender；固定 SDK 单次尝试和 Producer 销毁回调。
5. 验证用户 Bean 覆盖、缺失依赖、禁用开关及启动失败反馈。
6. 使用 Starter 启动最小 Spring Boot 测试应用，以真实 MySQL 和 RocketMQ 完成一次显式 `runOnce()`；最后执行全仓 `mvn clean verify`。

## 验收测试

### 不依赖容器的自动配置测试

至少验证：

1. 仅引入 Starter 且提供完整合法配置时，各默认 Bean 出现；自动配置由 imports 文件发现；
2. `enabled=false` 时没有默认 Bean，也不连接 Broker；
3. 缺少 JDBC 或 RocketMQ 类时相应配置退让，条件报告说明原因；
4. 用户自定义 Publisher、Sender、Resolver 或 Producer 时，对应默认 Bean 退让，用户 Producer 不被自动配置关闭；
5. 无映射、非法 Topic/Tag、多个冒号、空 Endpoint、单边凭据、非法时长或批量值均在启动期失败；
6. YAML 中带连字符的 Event Type 正确绑定，多个映射到同一 Topic 只预声明一次；
7. `max-attempts`、租约与退避配置真正传到对象行为中，而非仅绑定成功；
8. 默认 Producer 的 SDK `maxAttempts` 固定为 `1`，销毁时只关闭自动创建的 Producer；
9. 创建 Context 后不发生查询 Outbox 或发送消息，单轮执行必须显式触发。

### 真实服务集成测试

沿用 M4.1 固定的 MySQL 8.0.36、RocketMQ 5.5.0 和现有 Testcontainers 夹具。最小应用通过 Starter 获得 `ReliableEventPublisher`，在事务内登记事件，调用 `JdbcEventPublicationCycle.runOnce()`，从真实 Broker 消费消息并检查 Topic、Tag、Key、Body、保留属性和 Outbox `PUBLISHED`。关闭 Context 后确认自动创建的 Producer 已关闭；不要以固定睡眠推测启动或消费结果。

全部既有 M0 至 M4.1 测试仍须通过。测试结果记录在独立的 M4.2 完成文档，当前文档不预写通过数量。

## 完成边界

M4.2 完成后可以说“Starter 已能自动装配并显式执行一次发布”，不能说“引入依赖后会自动持续发送”。后台调度、有界并发和本地容量控制属于 M4.3；在途发送等待与优雅停机属于 M4.4；Micrometer、结构化日志及 M4 总验收属于 M4.5。消费者仍需按稳定事件 ID 或 `(eventType, eventKey)` 幂等处理。

## 参考

- [项目方向文档](../PROJECT_DIRECTION.md)
- [M4.1 实施文档](M4_1_ROCKETMQ_SENDER_AND_DESTINATION_MAPPING.md)
- [Spring Boot 官方自动配置与 Starter 指南](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html)
- [RocketMQ 5.x Java SDK 文档](https://rocketmq.apache.org/docs/sdk/02java/)
