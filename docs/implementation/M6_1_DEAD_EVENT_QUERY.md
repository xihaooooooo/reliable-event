# M6.1：死信查询与排障信息

> 状态：已完成（2026-09-29）；归属 [M6 总计划](M6_DEAD_LETTER_OPERATIONS.md)。

## 目标

让操作者先找到并理解 `DEAD` 事件，不依赖手写 SQL 修改数据库。本阶段只读，不触发发送或状态转换。

## 工作范围

- 提供按事件 ID 查询详情、分页查询 `DEAD` 列表的能力；列表按稳定顺序返回，并限定单页大小。
- 返回事件身份、失败摘要、尝试次数、时间和版本等排障所需信息。默认不返回 Payload、Headers；明确 `event_key` 和 `last_error` 的访问边界。
- 区分事件不存在、事件存在但不是 `DEAD`，让后续重放操作能使用查询得到的版本。

## 完成条件

真实 MySQL 测试证明查询结果、分页和状态过滤正确；查询不修改事件。接入方能依据返回信息定位一条死信，但此阶段还不能重放。

## 实施结果

- `reliable-event-jdbc` 提供 `JdbcDeadEventQuery`：`lookup(id)` 区分不存在、非 `DEAD` 和死信详情；`firstPage(pageSize)`、`nextPage(cursor, pageSize)` 只列出 `DEAD`，按事件 ID 倒序，单页最多 100 条。游标不构成数据库快照；分页期间新进入 `DEAD` 的旧事件可能需要重新查询。
- 列表不读取 `event_key`、`last_error`、Payload 或 Headers。详情仅读取前两项排障字段，不读取 Payload 和 Headers；详情的 `toString()` 省略敏感字段。调用方仍须限制详情访问，避免自行记录这些字段。
- 新建表 SQL 包含 `idx_dead_list(status, id)`；已有表执行一次 [M6.1 增量 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m6-1.sql)。查询类不会自动迁移数据库，也未接入 Starter 自动装配。
- 真实 MySQL 8.0.36 的 `ReliableEventIntegrationTest` 共 38 个测试通过，覆盖查询结果、状态区分、分页边界、只读行为和已有表索引迁移。M6.1 没有重放能力。

下一步：[M6.2 单条重放与原子审计](M6_2_CONTROLLED_REPLAY.md)。
