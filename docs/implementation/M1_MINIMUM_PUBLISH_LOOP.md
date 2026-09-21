# M1：完成最小发布闭环

> 状态：已完成
>
> 目标：让一条事件从业务事务登记开始，经过后台发布，最终变为 `PUBLISHED`。

## 整体流程

```text
业务事务调用 publish()
        ↓
写入 PENDING 事件
        ↓
后台扫描到期事件
        ↓
FakeEventSender 发送成功
        ↓
更新为 PUBLISHED
```

## 第一个小步骤：事件登记

先实现 `ReliableEventPublisher.publish()`，负责把事件写入 Outbox。

这一部分包含：

- 定义 `ReliableEventPublisher`、`ReliableEvent` 和 `EventId`；
- 将 Payload 序列化为 JSON；
- 要求调用时必须存在活动事务，否则直接报错；
- 以 `PENDING` 状态写入 Outbox；
- 相同 `eventType + eventKey` 重复登记时，不产生第二条记录。

先做事件登记，是因为它连接业务代码和可靠事件模块。入口的事务语义不稳定，后面的扫描和发送即使成功也没有意义。

## 第二个小步骤：后台发布

事件能够稳定登记后，再增加单实例轮询器和 `FakeEventSender`：

- 扫描已经到期的 `PENDING` 事件；
- 调用 Fake 发送器记录发送结果；
- 发送成功后把状态更新为 `PUBLISHED`。

Fake 发送器只用于验证流程，不接 RocketMQ。这样可以先确认数据库状态流转正确，避免同时排查数据库和消息中间件问题。

## 完成后有什么用

M1 完成后，项目将第一次具备可以运行的端到端流程。虽然还没有重试、多实例和 RocketMQ，但已经能够证明事件可以被登记、发现、发送并完成状态更新。

## 完成标准

- 提交业务事务后，Outbox 中存在一条 `PENDING` 事件；
- 回滚业务事务后，不留下事件；
- 重复事件键不会生成重复记录；
- 未到 `availableAt` 的事件不会发送；
- 到期事件只被 Fake 发送器处理，并最终变为 `PUBLISHED`；
- 上述行为都由自动化测试验证，`mvn verify` 通过。

M1 暂不实现失败重试、死信、租约、多实例竞争和 RocketMQ，这些属于后续里程碑。
