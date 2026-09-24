# ReliableEvent 当前交接

## 当前进度

M0、M1、完整 M2、M3、M4.1 和 M4.2 已完成。项目已经具备经过真实 MySQL、独立 JVM 故障注入和真实 RocketMQ 5.5.0 验证的条件抢占、失败重试、死信闭环、租约恢复、并发接管与普通消息发送能力，并已提供 Spring Boot Starter 自动装配：

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

单轮执行 → 先恢复一个有界批次 → 再发布一个有界批次

进程退出：
  抢占后、发送前退出 → 租约过期 → 恢复 → 新 Worker 发送一次
  发送后、状态更新前退出 → 租约过期 → 恢复 → 新 Worker 再次发送

真实 Broker：
  Event Type → Topic/Tag，Event Key → Message Key
  Payload JSON → UTF-8 Body，Headers → 用户属性
  Broker/Proxy 不可用 → RETRY_WAIT → 恢复后第二次发送成功
  成功回执被视为未知 → 重试 → 两条不同 Message ID 的预期重复消息
```

已经实现事务校验、JSON 序列化、重复事件键幂等登记、未来事件过滤、版本号条件抢占、短事务状态更新、带随机抖动的指数退避、最大尝试次数、三类发送结果、`DEAD` 终态、数据库时间租约、过期租约恢复、单轮恢复—发布编排和 RocketMQ 5.x 普通消息同步发送。公共 API、JDBC 状态机、RocketMQ 适配、自动配置及 Starter 已拆为五个模块。真实服务测试已覆盖消息映射、配置错误、消息超限、暂时不可用恢复、结果未知重复投递和 Starter 装配后的显式发布。

## 验证方式

启动 Docker，使用 Java 17 执行：

```bash
mvn verify
```

当前共有 107 个测试，覆盖单元、Context、MySQL、独立 JVM 和真实 RocketMQ 场景，全部通过。

## 当前边界

- Starter 已装配 Producer Bean 并在 Context 关闭时释放；尚未实现等待在途发送的优雅停机；
- 单个 Worker 内仍然顺序处理，没有后台调度和并行线程池；
- 单轮编排仍需显式调用，尚未接入常驻调度；
- 尚未实现租约续期；
- `DEAD` 暂无人工重放接口；
- 尚未实现 Micrometer 指标和结构化生产日志；
- 当前只验证单 Broker 测试环境，消费者仍需按稳定事件 ID 或业务 Key 实现幂等。

## 下一步

进入 M4.3，实现常驻调度与有界并发，将每轮抢占数量限制在本地可执行容量内。M4.2 的实施结果见 [完成记录](progress/M4_2_COMPLETED.md)，方案见 [实施文档](implementation/M4_2_SPRING_BOOT_AUTOCONFIGURE_AND_STARTER.md)。
