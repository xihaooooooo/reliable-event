# M6.3：Starter 接入与人工操作流程

> 状态：已完成（2026-09-29）；前置 [M6.2 单条重放](M6_2_CONTROLLED_REPLAY.md)。

## 目标

让使用 Starter 的应用能够调用 M6 的查询和重放能力，并按明确步骤完成一次人工处置。

## 工作范围

- 将查询和重放能力接入 Starter，对外提供小而明确的 Java 接口；不增加管理后台或内置 HTTP 端点。
- 说明接入应用如何限制操作权限、提供真实操作者身份、保存重放原因，以及如何观察操作结果。
- 在公开示例或独立操作示例中演示“查询 → 核对外部事实 → 修复原因 → 重放 → 观察状态”。发送结果未知时必须提醒可能重复投递。

## 完成条件

仓库外或公开示例应用仅依赖 Starter 即可调用接口；未配置额外管理功能时正常发布链路不受影响。示例和文档能指导操作者完成单条处置。

## 实现记录

- JDBC 模块公开 `DeadEventOperations`，聚合只读列表、详情和带版本保护的单条重放；`JdbcDeadEventOperations` 复用 M6.1 查询和 M6.2 同事务重放实现。
- Starter 在 `reliable-event.dead-operations-enabled=true` 且有单数据源、`JdbcTemplate`、事务管理器时装配接口 Bean；默认关闭，可由应用提供自定义实现。没有新增 HTTP 端点。关闭管理功能时 Publisher 和调度装配保持原有行为。
- [公开订单示例](../../reliable-event-example/README.md#人工处理-dead-事件)只依赖 Starter，包含 Java 调用类和完整人工步骤。接入方负责鉴权、真实操作者身份、外部事实核对与结果观察；未知发送结果可能已投递。
- 自动配置测试覆盖默认关闭、显式开启和自定义 Bean 覆盖；M6.3 阶段的全仓 JDK 17 `mvn clean verify` 通过：27 份 Surefire 报告共 146 个测试，0 失败、0 错误、0 跳过；公开示例参与构建，验证 Starter 依赖路径。后续 M6.4 的真实服务结果见[完成记录](../progress/M6_COMPLETED.md)。

后续的[M6.4 端到端验收与交接](M6_4_ACCEPTANCE.md)已完成。
