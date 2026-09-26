# M5.2 私有优惠券链路实施进度（已取消）

> 2026-09-25 记录接入进度；2026-09-26 决定取消 M5.2 后续验收。代码接入与聚焦测试已完成，但未满足完整业务验收标准；本阶段不标记为完成，也不作为 `0.1.0` 发布门槛。

## 已接入

- 私有项目位于 `D:\trae\onecoupon\onecoupon`。`merchant-admin` 已通过 Maven 依赖接入 Starter，建表脚本位于私有项目 `resources/database/reliable-event-outbox.sql`；未修改实际业务数据库。
- `createCouponTask` 在原有 Spring 事务内插入 `t_coupon_task` 后登记 `coupon-task-execute`。事件键为任务 ID 字符串；Body 保留旧消费协议中的 `keys`、`message.couponTaskId`、`timestamp`。立即任务不再走旧直接发送，定时任务以 `send_time` 登记 `availableAt`。
- 旧 XXL-Job 继续处理没有 Outbox 行的存量定时任务，跳过 Outbox 管理的任务。`distribution` 消费者收到到期且仍为 `PENDING` 的任务时，以条件更新转为 `IN_PROGRESS`；未到期消息会重试，已取消任务不会启动。
- 默认保留旧路径。`COUPON_TASK_OUTBOX_ENABLED=true` 开启新任务登记；`COUPON_TASK_OUTBOX_ENABLED=false`、`RELIABLE_EVENT_ENABLED=true` 可停止新登记并继续排空已提交事件。Proxy 地址与旧 NameServer 地址分开配置。私有项目的操作步骤见 `merchant-admin/RELIABLE_EVENT_M5_2.md`。
- RocketMQ 适配将遍历 JSON 字段的调用从 Jackson `JsonNode.properties()` 改为 `fields()`，兼容私有项目 Spring Boot 3.0.7 提供的 Jackson 版本。

## 已验证

| 测试 | 证据 |
| --- | --- |
| 双物理 MySQL 与 ShardingSphere 路由 | `CouponTaskOutboxIntegrationTest`：任务行和 Outbox 行落在 `one_coupon_0`，事务提交同在、回滚同无；同时验证 Starter 自动装配、定时 `availableAt` 和旧 Body 包装。2 个测试通过。 |
| 旧任务交接 | `CouponTaskJobHandoffTest`：新旧定时任务混合与排空配置，2 个测试通过。 |
| 真正的 RocketMQ 5.5.0 Broker/Proxy | `CouponTaskRocketMqCompatibilityTest`：检查 Topic、Key、Body 和 `reliable_event_*` 属性；1 个测试通过。 |
| 消费者协议及状态 | `CouponTaskOutboxConsumerTest`：新 Body 可被现有包装类型读取，到期状态转换、状态竞争失败后跳过、未到期拒绝、取消后跳过；5 个测试通过。 |

上述私有项目测试使用 JDK 17、MySQL 8.0.36 Testcontainers 与 RocketMQ 5.5.0 Testcontainers。私有项目存在其他未提交改动，本次没有清理或提交它们。

ReliableEvent 本仓库在 JDK 17 下执行 `mvn verify`，134 个测试通过，失败与错误均为 0。该回归包含公共库既有的故障和恢复测试；不能代替私有业务链路的整体验收。

## 尚待验收

- 在可控的完整业务环境中创建立即与定时任务，核对 Outbox `PUBLISHED` 后的实际用户券结果。现有聚焦测试验证了各接点，尚未运行包含 Excel、Redis、分发服务和最终用户券持久化的整链路。
- 对重复投递、消费进程退出和超过 Redis 去重 3600 秒后的再次投递做业务效果演练。消费者的 Redis 入口去重与任务 `SUCCESS` 状态不足以证明 Excel 批次、库存扣减、下游消息及用户券写入均持久幂等；现有业务实现含异步批次及 Redis 进度，需核对每处副作用。
- 在目标环境演练 Broker/Proxy 不可用、发送结果未知与租约恢复，并核对任务及用户券业务结果。公共库已有真实 Broker 故障测试，但不能代替私有业务环境的验证。
- 按私有项目操作说明迁移实际数据库、配置目标 Topic 与 Proxy 并执行灰度；这些运行环境变更尚未执行。
- 灰度时先升级所有会执行 `couponTemplateTask` XXL-Job 的实例，再开启新任务登记；旧版本 Job 不认识 Outbox 行，可能对新定时任务重复发送。

最终状态：“代码已接入，完整业务验收未完成；M5.2 已取消”。本记录保留已做工作与未验证边界，不能用于宣称私有业务链路已落地。
