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

## 对外表述

- [简历项目文案](RESUME_PROJECT.md)：根据实际完成进度选择可使用的项目描述。
