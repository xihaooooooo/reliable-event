# ReliableEvent 文档

文档按“方向—实施—对外表述”分层，避免把长期规划和当前要做的事情混在一起。

- [当前交接](HANDOFF.md)：查看已完成能力、验证方式、当前边界和下一步。

## 方向

- [项目方向](PROJECT_DIRECTION.md)：定义 `0.1.0` 的范围、语义和完成标准。
- [竞品与替代方案分析](COMPETITIVE_ANALYSIS.md)：对比 Java Outbox 库、Spring 事件发布日志、CDC 路线和 RocketMQ 事务消息，记录可借鉴项、边界与后续设计取舍。

## 实施

- [第一步：建立 MVP 的事务反馈环](implementation/M0_MVP_FOUNDATION.md)：说明为什么先做 M0、完成后有什么用，以及这一阶段的取舍。
- [M1：完成最小发布闭环](implementation/M1_MINIMUM_PUBLISH_LOOP.md)：从事件登记开始，逐步完成扫描、Fake 发送和成功状态更新。
- [M2.1：基于版本号的条件抢占](implementation/M2_1_CONDITIONAL_CLAIM.md)：拆分 M2 的第一步，定义候选快照、短事务抢占、版本校验和验收测试。
- [M2.2：双 Worker 并发竞争测试](implementation/M2_2_CONCURRENT_CLAIM_TEST.md)：让两个 Worker 使用同一候选版本并发竞争，以真实 MySQL 证明唯一执行权。
- [M2.3：失败重试与指数退避](implementation/M2_3_RETRY_BACKOFF.md)：定义 `RETRY_WAIT`、退避与抖动算法、错误摘要和可确定复现的失败重试测试。
- [M2.4：最大尝试次数与死信](implementation/M2_4_DEAD_LETTER.md)：收口 M2 状态机，定义重试耗尽、不可重试错误分类和 `DEAD` 终态。
- [M3.1：租约所有权与状态更新栅栏](implementation/M3_1_LEASE_OWNERSHIP.md)：定义 Worker 租约、数据库时间、完整抢占令牌和状态更新所有权校验。
- [M3.2：过期租约恢复](implementation/M3_2_EXPIRED_LEASE_RECOVERY.md)：定义过期候选、限量扫描、条件恢复、恢复退避和耗尽后死信语义。
- [M3.3：并发接管与恢复编排](implementation/M3_3_CONCURRENT_TAKEOVER_ORCHESTRATION.md)：冻结单轮恢复—发布顺序，并定义同一过期候选的多恢复者竞争和新旧 Worker 三方竞态测试。
- [M3.4：发布进程退出故障注入](implementation/M3_4_PROCESS_EXIT_FAULT_INJECTION.md)：定义独立 JVM 的两个强制退出窗口、持久化发送探针和至少一次重复投递验收。
- [M4.1：RocketMQ 发送适配与目标映射](implementation/M4_1_ROCKETMQ_SENDER_AND_DESTINATION_MAPPING.md)：冻结 RocketMQ 5.x 客户端基线、消息映射、错误分类和真实 Broker 验收方案。
- [M4.2：Spring Boot 自动配置与 Starter](implementation/M4_2_SPRING_BOOT_AUTOCONFIGURE_AND_STARTER.md)：定义配置属性、条件装配、Producer 生命周期、目标映射绑定和启动期校验。
- [M4.3：常驻调度与有界并发](implementation/M4_3_SCHEDULING_AND_BOUNDED_CONCURRENCY.md)：定义自动扫描、排队前不抢占、本地容量约束、并发执行和验收测试。
- [M4.4：停止抢占、等待在途发送与优雅停机](implementation/M4_4_GRACEFUL_SHUTDOWN.md)：定义有界停机、排队候选撤销、在途发送等待、Producer 关闭顺序和超时语义。
- [M4.5：Micrometer 指标、结构化日志与 M4 总验收](implementation/M4_5_OBSERVABILITY_AND_M4_ACCEPTANCE.md)：冻结指标口径、日志字段、首次可用时间迁移和真实服务验收。
- [M5.1：原创示例应用与端到端幂等演示](implementation/M5_1_ORIGINAL_EXAMPLE_APPLICATION.md)：定义公开示例的业务事务、自动发布、消费去重、本地运行与验收证据。
- [M5.2：私有优惠券项目首条业务链路接入](implementation/M5_2_PRIVATE_COUPON_INTEGRATION.md)：定义创建发券任务链路的接入前核对、事务与消息身份、切换方式及验收证据。
- [M5.3：可复现基准测试](implementation/M5_3_REPRODUCIBLE_BENCHMARK.md)：定义独立于私有业务验收的负载、指标口径、实验矩阵、原始证据和完成标准。

## 进度记录

- [MySQL 8.0 基线更正记录](progress/MYSQL_8_BASELINE.md)：记录服务端版本调整、并发策略取舍和完整回归结果。
- [M0 阶段完成记录](progress/M0_COMPLETED.md)：简要说明已经完成和验证的内容，以及下一步工作。
- [M1 阶段完成记录](progress/M1_COMPLETED.md)：记录最小发布闭环、验证结果和当前能力边界。
- [M2.1 阶段完成记录](progress/M2_1_COMPLETED.md)：记录版本号条件抢占、事务边界调整和新增验证结果。
- [M2.2 阶段完成记录](progress/M2_2_COMPLETED.md)：记录双 Worker 并发竞争的测试方式、验证结果和当前边界。
- [M2.3 阶段完成记录](progress/M2_3_COMPLETED.md)：记录失败重试、指数退避、错误摘要和新增验证结果。
- [M2.4 阶段完成记录](progress/M2_4_COMPLETED.md)：记录最大尝试次数、错误分类、死信终态和 M2 的最终验证结果。
- [M3.1 阶段完成记录](progress/M3_1_COMPLETED.md)：记录数据库时间租约、完整抢占令牌、所有权栅栏和新增验证结果。
- [M3.2 阶段完成记录](progress/M3_2_COMPLETED.md)：记录过期候选、限量恢复、恢复退避、耗尽后死信和新增验证结果。
- [M3.3 阶段完成记录](progress/M3_3_COMPLETED.md)：记录单轮恢复—发布编排、同候选多恢复者竞争和新旧 Worker 三方竞态验证结果。
- [M3.4 阶段完成记录](progress/M3_4_COMPLETED.md)：记录独立 JVM 强制退出、持久化发送证据和至少一次重复投递故障验证结果。
- [M4.1 阶段完成记录](progress/M4_1_COMPLETED.md)：记录模块拆分、RocketMQ 消息映射、错误分类和真实 Broker 故障验收结果。
- [M4.2 阶段完成记录](progress/M4_2_COMPLETED.md)：记录 Starter 自动配置、属性校验、Producer 生命周期和真实服务验收结果。
- [M4.3 阶段完成记录](progress/M4_3_COMPLETED.md)：记录常驻调度、有界并发、队列不持有租约和真实服务验收结果。
- [M4.4 阶段完成记录](progress/M4_4_COMPLETED.md)：记录有界停机、Producer 关闭顺序、超时恢复与全仓回归结果。
- [M4.5 阶段完成记录](progress/M4_5_COMPLETED.md)：记录指标、结构化日志、数据库迁移与 M4 总验收结果。
- [M5.1 阶段完成记录](progress/M5_1_COMPLETED.md)：记录原创示例、真实订单链路、重复消息去重和完整回归结果。
- [M5.2 实施进度](progress/M5_2_IMPLEMENTATION_PROGRESS.md)：记录私有项目接入代码、已运行测试与尚未通过的业务验收项。
- [M5.3 完成记录](progress/M5_3_COMPLETED.md)：记录真实基准环境、26 轮矩阵、故障证据、执行计划、原始 ZIP 与回归结果。

## 对外表述

- [简历项目文案](RESUME_PROJECT.md)：根据实际完成进度选择可使用的项目描述。
