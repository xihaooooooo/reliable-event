# M6.4：端到端验收与交接

> 状态：已完成（2026-09-29）；前置 [M6.3 Starter 接入](M6_3_STARTER_OPERATIONS.md)。

## 目标

用真实服务验证 M6 的人工恢复闭环，并把新能力与至少一次投递边界交接清楚。

## 验收范围

- 在真实 MySQL 与 RocketMQ 环境中制造 `DEAD`，修复原因后经公开接口重放，观察原事件最终进入 `PUBLISHED` 或再次按现有规则失败。
- 验证两个操作者竞争时只有一次有效重放；状态变化和审计记录相符。
- 覆盖发送结果未知后的重放场景，核对稳定事件身份与消费者幂等，不能把重放描述为恰好一次投递。
- 更新接入与运维指南，写明查询、权限、审计、重放前核对、结果追踪和失败时的处置方式；运行受影响测试与全仓回归。

## 完成条件

自动化测试和可复现操作记录证明上述路径，文档与实现一致；另写 M6 完成记录，列出测试结果及未覆盖的边界。只有此阶段通过后，M6 总计划才标为完成。

## 验收实现

- [公开示例端到端测试](../../reliable-event-example/src/test/java/dev/reliableevent/example/ExampleApplicationEndToEndTest.java)启动 MySQL 8.0.36、RocketMQ 5.5.0 和真实 Spring Boot 应用。应用只依赖 Starter，显式开启 `DeadEventOperations`，并预建 Outbox 与重放审计表。
- 第一条路径用缺失的事件目标映射使订单事件进入 `DEAD`；两个操作者拿同一版本并发重放，只有一个 `Replayed`，另一人得到 `NotDead`。审计只有一行；以正确映射重启服务后，Starter 自动调度将原事件送达真实 Broker，Outbox 为 `PUBLISHED`，消费者业务效果只有一份。
- 第二条路径在真实 Broker 成功接收后丢弃回执，因本轮 `max-attempts=1` 进入 `DEAD`。先核对 Broker 消息和消费者效果，再通过公开接口重放。重放后第二条 Broker 消息具有不同 Message ID、相同事件 ID/类型/业务键；消费者的持久去重使业务效果保持一次。
- [M6 完成记录](../progress/M6_COMPLETED.md)列出运行命令、结果、可复现步骤及未覆盖边界；[运维指南](../OPERATIONS.md)列出实际操作和失败处置。
