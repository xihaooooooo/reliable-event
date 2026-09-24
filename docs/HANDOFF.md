# ReliableEvent 当前交接

## 当前进度

M0、M1、完整 M2、M3 和 M4.1 已完成。项目已经具备经过真实 MySQL、独立 JVM 故障注入和真实 RocketMQ 5.5.0 验证的条件抢占、失败重试、死信闭环、租约恢复、并发接管与普通消息发送能力：

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

已经实现事务校验、JSON 序列化、重复事件键幂等登记、未来事件过滤、版本号条件抢占、短事务状态更新、带随机抖动的指数退避、最大尝试次数、三类发送结果、`DEAD` 终态、数据库时间租约、过期租约恢复、单轮恢复—发布编排和 RocketMQ 5.x 普通消息同步发送。公共 API、JDBC 状态机和 RocketMQ 适配已经拆分为 `core`、`jdbc`、`rocketmq` 三个模块。真实 Broker 测试已覆盖完整消息映射、配置错误、消息超限、暂时不可用恢复和结果未知重复投递。

## 验证方式

启动 Docker，使用 Java 17 执行：

```bash
mvn verify
```

当前共有 51 个单元测试和 40 个 MySQL、独立 JVM 或真实 RocketMQ 集成测试，共 91 个，全部通过。

## 当前边界

- 尚未实现 Spring Boot 自动配置、配置属性绑定和 Producer Bean 生命周期管理；
- 单个 Worker 内仍然顺序处理，没有后台调度和并行线程池；
- 单轮编排仍需显式调用，尚未接入常驻调度；
- 尚未实现租约续期；
- `DEAD` 暂无人工重放接口；
- 尚未实现优雅停机、Micrometer 指标和结构化生产日志；
- 当前只验证单 Broker 测试环境，消费者仍需按稳定事件 ID 或业务 Key 实现幂等。

## 下一步

进入 M4.2，实现 Spring Boot 自动配置与 Starter，包括配置属性、条件装配、Producer 生命周期、目标映射绑定和启动期校验。M4.1 的实现和验证结果见 [M4.1 完成记录](progress/M4_1_COMPLETED.md)。

M4.2 只负责装配，不改变 M4.1 已冻结的消息映射、错误分类和至少一次语义；后台调度与有界并发留到 M4.3。
