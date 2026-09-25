# ReliableEvent 当前交接

## 当前进度

M0、M1、完整 M2、M3 和 M4.1 至 M4.4 已完成。项目已经具备经过真实 MySQL、独立 JVM 故障注入和真实 RocketMQ 5.5.0 验证的条件抢占、失败重试、死信闭环、租约恢复、并发接管与普通消息发送能力。Spring Boot Starter 现已自动装配、持续调度并执行有界优雅停机：

```text
事务内 publish → PENDING → 查询候选版本 → 条件抢占为 PUBLISHING
                                               ↓
                                 写入 leaseOwner + leaseUntil
                                               ↓
                                  事务外 RocketMQ 同步发送
                                               ↓
                         按版本 + Owner + 有效期更新为 PUBLISHED

租约过期 → 限量查询候选 → 按版本 + Owner + 截止时间快照恢复
                                      ├→ RETRY_WAIT
                                      └→ DEAD

自动轮次 → 先恢复一个有界批次 → 按本地空余槽位查询并排队候选
                                     ↓
                         执行线程开始后才抢占数据库租约

本地未完成任务数 ≤ worker-threads + worker-queue-capacity
同时执行的候选任务数 ≤ worker-threads
原有同步 runOnce() 仍可在关闭自动调度后显式调用

Context 关闭 → 停止新抢占 → 撤销未抢占队列 → 限时等待在途任务
             ├→ 正常完成：状态更新后关闭默认 Producer
             └→ 超时：中断并继续关闭；遗留租约由后续实例恢复

进程退出：
  抢占后、发送前退出 → 租约过期 → 恢复 → 新 Worker 发送一次
  发送后、状态更新前退出 → 租约过期 → 恢复 → 新 Worker 再次发送

真实 Broker：
  Event Type → Topic/Tag，Event Key → Message Key
  Payload JSON → UTF-8 Body，Headers → 用户属性
  Broker/Proxy 不可用 → RETRY_WAIT → 恢复后第二次发送成功
  成功回执被视为未知 → 重试 → 两条不同 Message ID 的预期重复消息
```

已经实现事务校验、JSON 序列化、重复事件键幂等登记、未来事件过滤、版本号条件抢占、短事务状态更新、带随机抖动的指数退避、最大尝试次数、三类发送结果、`DEAD` 终态、数据库时间租约、过期租约恢复、RocketMQ 5.x 普通消息同步发送、固定延迟调度、有界并发和有界停机。公共 API、JDBC 状态机、RocketMQ 适配、自动配置及 Starter 已拆为五个模块。真实服务测试已覆盖消息映射、Broker 不可用恢复、结果未知重复投递、Starter 自动发送、排队候选不占租约、自动租约恢复、双实例共用数据库抢占、关闭时的队列撤销与第二实例接管，以及超时后的租约恢复。

## 验证方式

启动 Docker，使用 Java 17 执行：

```bash
mvn verify
```

当前共有 120 个测试，覆盖单元、Context、MySQL、独立 JVM 和真实 RocketMQ 场景，全部通过。

## 当前边界

- Starter 默认运行时已支持有界优雅停机；超时或外部调用不响应中断时，仍可能在资源关闭后结束发送，依赖租约恢复且可能重复投递；
- 同步 `runOnce()` 仍可在 `scheduling-enabled=false` 时使用，手动调用不纳入自动执行器的容量统计；
- `shutdown-timeout` 不限制 Producer `close()` 或整个 Context 关闭时间；Spring 生命周期阶段超时应大于它；
- 尚未实现租约续期；
- `DEAD` 暂无人工重放接口；
- 尚未实现 Micrometer 指标和结构化生产日志；
- 当前只验证单 Broker 测试环境，消费者仍需按稳定事件 ID 或业务 Key 实现幂等。

## 下一步

进入 M4.5，完善 Micrometer 指标、结构化生产日志和 M4 总验收。M4.4 的实施结果见 [完成记录](progress/M4_4_COMPLETED.md)，协议见 [实施文档](implementation/M4_4_GRACEFUL_SHUTDOWN.md)。
