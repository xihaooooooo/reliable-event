# M5.1 阶段完成记录

## 已实现

- 新增公开的 [`reliable-event-example`](../../reliable-event-example/README.md) Spring Boot 应用。`POST /orders` 在一个 MySQL 事务内插入订单并登记 `order-created` 事件；Starter 自动发送至 RocketMQ 普通消息，`GET /orders/{id}` 显示消费处理次数。
- 示例消费者校验稳定事件身份、Message Key 和订单 Payload，在一个事务内登记 `(event_type, event_key)` 去重键并写入业务效果。只有事务成功后才 ACK；同一业务键携带不同事件 ID 会被拒绝。
- 提供固定版本 MySQL 8.0.36、RocketMQ 5.5.0 的 Compose 配置、Topic/消费者组/建表初始化脚本、限时等待脚本，以及正常路径、Broker 故障和重复消息的操作说明。示例不复制库内部实现或私有优惠券项目代码。

## 验证

- JDK 21 运行，Java 17 编译目标；Windows + Docker Desktop 29.1.3、MySQL 8.0.36、RocketMQ 5.5.0。`mvn -ntp -q clean verify` 全仓 **134 个测试，0 失败、0 错误、0 跳过**，其中 M5.1 新增 7 个测试。
- MySQL 集成测试验证订单和 Outbox 同提交、同回滚，事务外登记被拒绝；重复消费只写一次业务效果，业务更新失败会回滚去重记录，错误事件 ID 不会被当作合法重复消息。
- 真实 MySQL/RocketMQ 端到端测试从 HTTP 下单走到 Starter 自动发布和消费处理；另一次测试在首次真实发送成功后丢弃回执，观察 `RETRY_WAIT → PUBLISHED`、两个不同 Broker Message ID、相同可靠事件身份及仅一次业务效果，并核对 Topic、Tag、Key、JSON Body 和保留属性。
- 按示例 README 从空 Compose 资源运行 `bootstrap.ps1`、本地 Maven 安装和应用启动。下单得到 `orderId=1、eventId=1`；Outbox 为 `status=2、attempt_count=1`，去重表和效果表各一行。暂停 Broker 后创建第二笔订单，观察到 `RETRY_WAIT`；恢复 Broker 后为 `status=2、attempt_count=2`，消费者处理次数仍为 1。Compose 与 PowerShell 脚本语法检查通过。
- 本机 8090 端口在冒烟验证时被现有连接占用，因此使用 `EXAMPLE_HTTP_PORT=18090` 覆盖；README 已写明端口冲突处理。验证后停止示例应用，并删除本次从空环境创建的 Compose 容器、网络及 MySQL 数据卷；未触碰其他资源。

## 当前边界

- 示例展示单应用、单 Broker 测试环境下的消费幂等，不改变 Starter 的至少一次发布语义；`PUBLISHED` 只表示生产端获得成功回执，不代表消费者完成。
- 示例消费者不是通用消费者框架；租约续期、`DEAD` 人工重放、生产级集群部署、私有优惠券项目接入和可复现性能基准仍不在 M5.1 范围内。

## 下一步

按 M5 规划进行私有优惠券场景接入、基准测试和 `0.1.0` 发布检查。实施范围见 [M5.1 文档](../implementation/M5_1_ORIGINAL_EXAMPLE_APPLICATION.md)。
