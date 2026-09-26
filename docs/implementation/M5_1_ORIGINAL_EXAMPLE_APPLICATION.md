# M5.1：原创示例应用与端到端幂等演示

> 2026-09-26 范围更新：下文提及的 M5.2 私有优惠券接入已取消，不再属于 `0.1.0` 发布范围或后续待办；M5.1 的示例验收结果不受影响。

> 状态：已完成；结果见 [M5.1 完成记录](../progress/M5_1_COMPLETED.md)
>
> 基线：M4.5 已完成；Java 17、Spring Boot 3.5.16、MySQL 8.0.36、RocketMQ 5.5.0
>
> 目标：提供可独立运行的原创最小应用，让使用者从业务事务登记事件开始，亲自观察自动发布、RocketMQ 消费、重复消息去重及 Outbox 状态。示例应成为 `0.1.0` 的首个可复现接入说明。

M0 至 M4.5 已验证库内部的事务、抢占、恢复、发送和观测行为，但仓库仍缺少一个普通业务应用如何接入的完整例子。M5.1 用自己的业务模型和代码补上这条使用路径，并检验 Starter 对外是否足够易用。M5.2 再进入私有优惠券项目，M5.3 做基准测试，M5.4 汇总发布检查。

## 本阶段的交付物

1. 新增 Maven 模块 `reliable-event-example`，包含可启动的 Spring Boot 应用、原创业务代码、建表 SQL 和示例配置。该模块依赖当前 Starter，不复制 `reliable-event-jdbc` 或 `reliable-event-rocketmq` 的内部实现。
2. 提供一套固定版本的本地 MySQL、RocketMQ NameServer、Broker 和 Proxy 启动配置，以及创建普通消息 Topic 和消费者组的命令。配置、脚本与 README 放在示例模块内，启动与清理范围限于示例自身的资源。
3. 提供从空数据库启动、创建一笔业务记录、查询 Outbox、消费事件、验证幂等的逐条命令和预期结果。主路径在 Docker 镜像与 Maven 依赖已缓存的环境中应能在 15 分钟内走通；首次拉取镜像的时间单独说明。
4. 增加覆盖真实 MySQL 与 RocketMQ 的端到端测试，并保留全仓 `mvn verify` 回归。完成后单独新增 `docs/progress/M5_1_COMPLETED.md`，记录实际环境、命令、测试数和结果。

## 示例业务与成功路径

示例使用虚构的订单通知，不引用或改写私有优惠券项目代码。一次 `POST /orders` 请求产生一个 `example_order` 记录和一个 `order-created` 事件。订单 ID 作为稳定 `eventKey`；Payload 只放示例所需的订单 ID 和业务字段，Headers 只放非敏感的示例来源。`availableAt` 使用当前时间。

```text
POST /orders
  └─ 一个本地事务：INSERT example_order + publisher.publish(order-created)
         ├─ 回滚：两条记录都不存在
         └─ 提交：订单存在，Outbox 为 PENDING
                         ↓
               Starter 自动抢占并发送普通消息
                         ↓
               RocketMQ 示例消费者收到消息
                         ↓
               一个本地事务：登记去重键 + 更新示例处理结果
                         ↓
               事务提交后 ACK
```

HTTP 接口只为演示创建和查看结果：`POST /orders` 创建订单，`GET /orders/{id}` 返回订单与处理状态。Outbox 的 `PENDING`、`PUBLISHING`、`RETRY_WAIT`、`PUBLISHED`、`DEAD` 状态通过文档给出的只读 SQL 查看，不为示例引入管理后台或修改库的公共 API。

`example_order`、消费者去重表和处理结果表使用单独的 `example_` 前缀。示例初始化 SQL 同时执行当前正式 [Outbox 建表 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)，避免复制一份容易过期的 Outbox 表定义。示例不会在每次启动时删除已有数据；重置命令必须清楚指向示例数据库。

### 事务与消息身份

- 创建订单的业务方法必须由 Spring 数据库事务包围，`ReliableEventPublisher.publish(...)` 在该事务内调用。测试分别证明提交与回滚结果。
- `eventType` 固定为 `order-created`，映射到示例 Topic 和 Tag；`eventKey` 固定为订单 ID 的字符串形式。同一业务命令重复执行时，应由业务层决定是否创建新订单；同一 `eventType + eventKey` 的重复登记由 Outbox 唯一键返回既有事件 ID。
- RocketMQ Message Key 是 `eventKey`；消息属性含 `reliable_event_id`、`reliable_event_type` 和 `reliable_event_key`。示例消费者先校验必需字段，再处理 Payload。Broker Message ID 仅供诊断，不作为去重依据。
- 消费者以 `(event_type, event_key)` 作为去重表唯一键，保存 `reliable_event_id` 供核对。消费业务更新与去重登记必须处于同一个 MySQL 事务中；只有事务成功提交后才 ACK。唯一键冲突表示已处理，直接 ACK 且不重复执行业务更新。事务失败时不 ACK，让 RocketMQ 按自身消费语义重投。
- 消费者示例仅证明这一业务处理动作的幂等性。`PUBLISHED` 仍只表示生产端得到成功回执，不表示消费者处理成功。

### 可观察结果

README 至少展示以下可核对的值，而非只展示“发送成功”日志：

| 阶段 | 业务表 | Outbox | 消费者处理结果 |
| --- | --- | --- | --- |
| 创建事务提交后 | 新订单一条 | 一条 `order-created` 记录 | 尚未处理或等待消费 |
| 自动发送完成后 | 订单不变 | `PUBLISHED`，`attempt_count >= 1` | 最终出现一次处理结果 |
| 同一消息重复投递后 | 订单不变 | 原事件身份不变 | 去重记录和业务效果仍各一次 |
| 创建事务回滚后 | 无该订单 | 无对应事件 | 无处理结果 |

自动发布和消费是异步的，命令应使用有截止时间的查询或明确的重试说明；不能把一次即时查询未看到结果解释为失败。示例需要显示事件 ID、业务 Key 和处理次数，便于读者把三处事实关联起来。

## 故障演示

文档提供两个可控演示，均附恢复步骤和预期数据库状态。

1. **Broker/Proxy 暂时不可用。** 先确保应用与 Broker 正常连接，再暂停示例 Broker/Proxy，创建订单并观察 Outbox 进入 `RETRY_WAIT`、`attempt_count` 增加；恢复服务后，在退避到期并自动扫描后进入 `PUBLISHED`，消费者最终处理一次。演示不能依赖固定 `sleep` 猜测状态，也不要求故障发生时 HTTP 请求等待 Broker。
2. **重复消息。** 使用示例专属的测试入口或集成测试，在第一次真实发送成功后丢弃成功回执，使 Outbox 按 `RESULT_UNKNOWN` 重试。验证 Broker 中两条消息的 Message ID 不同、稳定事件身份相同，且消费者去重表与业务处理结果仍只有一次。若不暴露测试入口给普通运行模式，README 应给出运行该集成测试的具体命令和关键断言。

`DEAD` 只做只读观察：README 给出按状态和事件 Key 查询的 SQL，并说明达到最大尝试次数或明确不可重试错误后不会自动发送。M5.1 不新增人工重放能力，也不通过直接改表状态冒充受控重放。

## 运行与配置约束

- 示例使用项目已经固定的 Java、MySQL、RocketMQ 版本；RocketMQ 5.x Java 客户端连接 Proxy 的 gRPC 地址，不误连 NameServer 或 Broker Remoting 端口。
- 本地部署配置显式给出数据库 URL、用户名、密码的示例值及 RocketMQ Endpoint、Topic/Tag 映射。真实凭据用环境变量注入，仓库只保留测试值或占位符。
- 示例在外部服务未就绪时给出可理解的失败信息。就绪检查需确认 Broker 注册、Topic/消费者组创建及 Topic 路由可见，不能只以端口开放作为成功条件。
- 生产者仍使用 Starter 默认自动调度。示例不自行循环调用 `runOnce()`，不创建第二个扫描线程；需要控制演示速度时只调整已有的 `reliable-event.*` 属性。
- 消费者与生产者可以运行在同一个示例应用中，但代码需明确区分创建订单、可靠发布和消费处理三个职责。关闭应用时正确关闭消费者和连接资源。
- 本地脚本不得要求修改宿主机的全局网络规则、Docker 全局配置或私有项目。若端口被占用，README 给出诊断与改端口方法。

## 实施顺序

1. 建立示例模块、依赖和原创业务表；先用事务测试证明订单与 Outbox 同提交、同回滚。
2. 接入 Starter 的自动发布，完成真实 Broker 的创建订单至 `PUBLISHED` 和消费成功路径。
3. 实现消费者去重事务，证明相同业务事件的两次消息交付只产生一次业务效果。
4. 补齐本地服务启动、Topic/消费者组初始化、只读状态 SQL 与故障演示命令；由未参与编写示例的人按 README 从空环境走一次。
5. 执行示例集成测试及全仓 `mvn verify`；核对文档命令与实际输出，再记录 M5.1 完成报告并更新项目 README、文档导航和交接进度。

## 验收标准

- 示例从空数据库和固定版本服务启动，不需要私有优惠券项目或手工构造内部 Bean。
- `POST /orders` 的成功与回滚测试分别证明业务行和 Outbox 行的原子性；未开启事务调用 `publish` 仍明确失败。
- 真实 RocketMQ 消息的 Topic、Tag、Key、UTF-8 Body 和保留属性与示例配置一致；Starter 无需手动 `runOnce()` 即自动把事件推进到 `PUBLISHED`。
- 消费者在真实重复投递或确定性的双次处理测试中，仅提交一条去重记录和一次业务效果；ACK 发生在消费事务提交之后。
- Broker/Proxy 故障恢复演示能观察到 `RETRY_WAIT → PUBLISHED`；结果未知测试能观察到不同 Message ID 对应同一稳定事件身份。
- README 包含逐条命令、预期响应与 SQL 结果、故障恢复和资源清理步骤；不依赖固定休眠判断异步结果。
- `mvn verify` 全仓通过，完成记录写明实际测试数、失败/跳过数、服务版本和运行环境。只有这些证据齐备，才将 M5.1 状态改为“已完成”。

## 阶段边界

M5.1 交付公开的原创示例和消费幂等演示。私有优惠券项目真实接入属于 M5.2；吞吐、延迟和数据库成本的可复现测量属于 M5.3；完整运维手册、发布材料和 `0.1.0` 最终检查属于 M5.4。示例不承诺生产集群容错、消息顺序、Exactly Once、租约续期或死信人工重放。正式库的已发布事件清理策略与示例数据库重置是不同问题，应在发布检查中另行确认。

## 参考

- [项目方向文档](../PROJECT_DIRECTION.md)
- [当前交接](../HANDOFF.md)
- [M4.5 观测与总验收](M4_5_OBSERVABILITY_AND_M4_ACCEPTANCE.md)
- [M4.5 完成记录](../progress/M4_5_COMPLETED.md)
