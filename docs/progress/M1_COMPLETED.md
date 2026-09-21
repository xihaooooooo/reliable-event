# M1 阶段完成记录

## 这一阶段做了什么

- 定义了 `ReliableEventPublisher`、`ReliableEvent` 和 `EventId`。
- 实现了 JDBC 事件登记，Payload 和 Headers 会保存为 JSON。
- 要求 `publish()` 必须在活动事务中调用，没有事务时直接报错。
- 使用 `eventType + eventKey` 防止事件重复登记，并返回已有事件 ID。
- 实现了单实例到期扫描、Fake 发送和 `PUBLISHED` 状态更新。

## 现在能够完成什么

```text
业务事务登记事件
        ↓
Outbox 保存为 PENDING
        ↓
发布器扫描到期事件
        ↓
FakeEventSender 发送
        ↓
事件更新为 PUBLISHED
```

未来时间才可用的事件不会被提前发送，业务事务回滚也不会留下事件。

## 验证结果

使用 Java 17 和 MySQL 5.7.44 执行 `mvn verify`，5 个集成测试全部通过，覆盖：

- 事务提交和回滚；
- 无活动事务时拒绝发布；
- JSON Payload 持久化；
- 重复事件键；
- 到期事件发布和未来事件过滤。

## 当前边界

当前发送端是测试用 Fake 适配，还没有接入 RocketMQ，也没有失败重试、死信、租约和多实例安全抢占。

## 下一步

进入 M2，重点实现多实例条件抢占、指数退避、最大尝试次数和死信状态。
