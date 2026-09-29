# M6.2：单条重放与原子审计

> 状态：已完成（2026-09-29）；前置 [M6.1 死信查询](M6_1_DEAD_EVENT_QUERY.md)。

## 目标

将经人工确认的一条 `DEAD` 事件安全地重新交给现有发布器，同时留下可追溯的操作记录。

## 工作范围

- 重放请求包含事件 ID、预期版本、操作者和原因；仅允许当前仍为 `DEAD` 且版本匹配的事件成功。
- 原子地开启新一轮尝试并重新入队，保留原事件身份；明确处理现有 `attempt_count < max_attempts` 抢占条件，不能只把 `status` 改回待发布。
- 成功的状态转换与审计记录在同一事务中提交；审计保留重放前的失败与尝试信息。重放接口不直接调用 RocketMQ。
- 并发重放同一版本时最多一次成功；过期版本、非死信和无效操作信息给出可识别的结果。

## 完成条件

真实 MySQL 测试覆盖单条成功、并发竞争、重复请求、事务回滚及不符合资格的事件；成功重放后事件能被原有到期扫描发现。

## 实施结果

- JDBC 模块提供 `JdbcDeadEventReplay.replay(DeadEventReplayRequest)`。请求须带事件 ID、M6.1 详情中的预期版本、非空操作者和原因；字段分别限制为 128 和 1024 个字符。请求的 `toString()` 不输出操作者或原因。调用方负责鉴权与真实身份绑定，库不会自行识别用户。
- 同一数据库事务内先用 `SELECT ... FOR UPDATE` 锁定事件，按状态和版本分别返回 `NotFound`、`NotDead`、`VersionMismatch`，或执行一次条件更新并返回含审计 ID 的 `Replayed`。拒绝结果不修改事件，也不写审计；并发请求同一旧版本最多一个成功。数据库异常直接抛出并回滚，不伪装成业务拒绝。
- 成功时保留原事件 ID、事件类型、业务键、Payload、Headers 和 `max_attempts`，将状态置为 `PENDING`、`attempt_count` 清零、版本加一，使用数据库 UTC 时间设置新的 `next_attempt_at`；清除旧租约、旧失败摘要和 `published_at`。这是新一轮最多 `max_attempts` 次的尝试，原有查询和抢占条件 `attempt_count < max_attempts` 可直接接手。`first_available_at` 保留原值，因此重放后的发布延迟仍从最初可用时间计算，不能解释为本轮重放耗时。
- 成功状态转换与 `reliable_event_replay_audit` 插入共用同一事务。审计保存旧版本、新版本、重放前尝试次数及上限、旧失败摘要、操作者、原因、数据库时间和 `REQUEUED` 结果；审计 ID 随成功结果返回。旧失败摘要与原因可能包含敏感文本，应限制审计表访问。重放方法只入队，不直接调用 RocketMQ；遇到发送结果未知时仍可能重复投递。
- 使用前执行一次[审计表 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-replay-audit-m6-2.sql)。Starter 不自动建表；M6.3 已提供需显式开启的 Java 接口。
- 真实 MySQL 8.0.36 的 `ReliableEventIntegrationTest` 45 个测试通过，覆盖耗尽后的成功重放及原 Worker 发布、非死信/缺失/旧版本、重复与并发请求、调用方回滚、审计写入失败回滚、UTC 返回时间和操作信息校验。JDK 17.0.12 下全仓 `mvn -o -Dmaven.repo.local=<本机缓存> clean verify` 成功；UTC 修正后又对最终源码执行全仓 `verify` 成功：27 份 Surefire 报告共 144 个测试，0 失败、0 错误、0 跳过。

后续：[M6.3 Starter 接入与人工操作流程](M6_3_STARTER_OPERATIONS.md)。
