# M0 阶段完成记录

## 这一阶段做了什么

- 建立了 Maven 父工程，并先创建一个 `reliable-event-jdbc` 模块。
- 固定使用 Java 17、Spring Boot 3 和 MySQL 8.0。
- 创建了 `reliable_event_outbox` 表，保存等待发布的事件。
- 使用 Testcontainers，让测试自动启动真实的 MySQL 8.0。
- 编写了事务提交和事务回滚两个集成测试。

## 现在证明了什么

业务数据和 Outbox 事件使用同一个数据库事务：事务成功时两者一起保存，事务失败时两者一起回滚，不会留下孤立事件。

执行 `mvn verify`，两个测试均已通过。

## 这一阶段没有做什么

目前还没有实现事件发布接口、后台扫描、RocketMQ 发送、失败重试和多实例抢占。这些从 M1 开始逐步完成。

## 下一步

进入 M1，完成最小发布闭环：

```text
publish 登记事件 → 后台扫描 → Fake 发送 → 标记为 PUBLISHED
```
