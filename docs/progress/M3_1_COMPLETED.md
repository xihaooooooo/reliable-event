# M3.1 阶段完成记录

## 这一阶段做了什么

- 为每个 `JdbcEventPublicationWorker` 增加稳定的 `workerId` 和正数 `leaseDuration`；
- 抢占事件时写入 `lease_owner`，并使用 MySQL `UTC_TIMESTAMP(3)` 计算 `lease_until`；
- 将 Owner 和截止时间加入不可变 `ClaimedEvent`，形成包含版本、所有者和有效期的完整租约令牌；
- 成功、重试和死信更新同时校验事件状态、抢占版本、租约所有者和租约有效期；
- 将租约边界统一为 `lease_until > UTC_TIMESTAMP(3)`，边界时刻视为已经过期；
- 事件正常离开 `PUBLISHING` 时清空 `lease_owner` 和 `lease_until`；
- 状态更新失去所有权时抛出包含事件 ID 和 Worker ID 的明确异常；
- 保持 Sender 在数据库事务外执行，没有引入租约续期、恢复扫描或后台调度。

## 现在证明了什么

版本号不再是完成状态的唯一条件。Worker 必须同时持有当前抢占版本、正确 Owner 和尚未过期的租约，才能将事件更新为 `PUBLISHED`、`RETRY_WAIT` 或 `DEAD`。

即使旧 Worker 使用相同事件 ID，以下任一情况发生后也不能覆盖数据库状态：

- 抢占版本已经前进；
- 租约 Owner 不匹配；
- 租约已经到期。

租约创建和有效性判断使用数据库时间，因此不会由不同 Worker 所在 JVM 的本地时钟偏差决定所有权。

## 验证结果

使用 MySQL 8.0.36 Testcontainers 执行 `mvn verify`，共 34 个测试全部通过：

- 14 个单元测试覆盖原有退避、错误摘要、失败分类，以及新增的租约令牌不变量；
- 20 个集成测试覆盖原有事务、幂等、双 Worker 竞争、重试和死信，以及新增的数据库时间租约、错误 Owner、过期租约、租约清理和 Worker 配置校验。

重点验证：

- 抢占返回的 Owner 和截止时间与数据库记录一致；
- `lease_until` 落在抢占前后数据库时间加租约时长的区间内；
- 错误 Owner 无法完成成功、重试或死信更新；
- 过期租约无法完成任何状态更新；
- 正常发布、重试和死信都会清空租约字段；
- 两个不同 Owner 的 Worker 竞争同一候选时仍然只有一个 Sender 调用；
- 相同 Owner 的旧抢占版本仍然不能覆盖新状态。

## 当前边界

- 过期的 `PUBLISHING` 事件还不会自动恢复；
- 抢占后进程退出仍会留下等待恢复的记录；
- 尚未实现多个恢复 Worker 的并发竞争和恢复批量限制；
- 尚未执行发送前退出、发送后状态更新前退出的独立进程故障测试；
- 没有租约续期；
- 尚未接入 RocketMQ、后台调度、并行线程池、自动配置和指标。

这些是 M3.1 刻意保留的边界。当前实现已经提供安全恢复所需的所有权栅栏，但还不能表述为已完成宕机恢复。

## 下一步

进入 M3.2，基于现有 `idx_lease_recovery (status, lease_until, id)` 限量查询过期的 `PUBLISHING` 记录，并使用事件 ID、版本、Owner 和过期条件将其安全恢复为 `RETRY_WAIT` 或 `DEAD`。
