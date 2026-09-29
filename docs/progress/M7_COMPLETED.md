# M7 已发布事件保留与清理完成记录

> 2026-09-29，M7.1 至 M7.3 的代码、迁移脚本、运维说明和真实服务验收已完成。清理默认关闭；本记录不表示已在生产环境迁移或开启。

## 交付范围

- 新建永久身份表 `reliable_event_identity`，以 `(event_type, event_key)` 唯一登记事件并保存原 `EventId`。业务事务内先插入身份，再以同一 ID 插入 Outbox；重复登记直接返回原 ID，不更新 Payload、Headers 或计划时间，也不因已发布行被清理而重新入队。身份与 Outbox 在同一事务提交或回滚。
- 提供[旧数据全量回填 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-identity-m7-1.sql)、[一致性核对 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-identity-m7-1-check.sql)及[清理索引增量 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox-m7-2.sql)。[正式建表 SQL](../../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)包含两表与索引；Starter 不自动迁移。
- JDBC 单轮清理入口 `JdbcPublishedEventRetention.runOnce(retention, batchSize)` 在短事务内使用数据库 UTC 时间，按 `published_at, id` 有界扫描，只删除身份匹配、仍处于 `PUBLISHED` 且仍已到期的 Outbox 行。结果包含扫描数、删除数和剩余最老到期行年龄。
- Starter 增加默认关闭的 `published-retention-enabled`。启用时必须显式指定正数 `published-retention`；`cleanup-batch-size` 限制为 1–1000，`cleanup-interval` 默认为 1 小时。独立固定延迟任务记录数量、耗时、失败和最老待清理年龄；提供对应 Micrometer 指标。清理日志不打印业务键、Payload 或 Headers。
- 基准数据重置同时清理身份表，历史数据种子同步建立身份；示例与运维文档说明升级、开关、查询和回退约束。

## 验证结果

在 JDK 17.0.12、Docker 可用的环境，从仓库根目录执行全仓离线缓存构建：

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-17' # 按本机路径调整
mvn -o '-Dmaven.repo.local=C:\Users\20659\.m2\repository' -q clean verify
python -m unittest discover -s reliable-event-benchmark/scripts/tests -v
```

- `clean verify` 成功；28 份 Surefire 报告共 160 个测试，0 失败、0 错误、0 跳过。Python 基准报告契约测试 3/3 通过。
- [JDBC 集成测试](../../reliable-event-jdbc/src/test/java/dev/reliableevent/jdbc/PublishedRetentionIntegrationTest.java)在真实 MySQL 上覆盖旧数据全状态回填、清理后重复登记、批次与不可清理状态、截止时间与缺失身份保护、业务事务回滚、并发登记与并发清理、删除后故障回滚及身份表不可用时失败。
- [公开订单示例端到端测试](../../reliable-event-example/src/test/java/dev/reliableevent/example/ExampleApplicationEndToEndTest.java)在真实 MySQL 8.0.36 与 RocketMQ 5.5.0 上开启短保留期：订单事件送达 Broker 并被消费者处理，随后 Outbox 行清理而身份保留；同键再次发布返回原 ID，Broker 不出现新消息；新业务键仍可正常发布和消费。该测试类 5/5 通过。
- Starter Context 和 Micrometer 测试覆盖默认关闭、显式配置校验和清理指标。原有发布、重试、重放及故障注入测试一并通过。

## 生产升级顺序

1. 关闭清理并备份；暂停全部事件登记入口，等待在途业务事务结束。按既有表版本补齐 M4.5、M6.1 迁移。
2. 执行身份表创建与**全部状态**的旧行回填；运行核对 SQL。首次迁移的三个异常计数须为零，身份表下一个自增 ID 须大于已有最大事件 ID。抽查历史 `published_at` 是否确实按 UTC 表示。
3. 执行 M7 清理扫描索引 SQL；在登记保持暂停的情况下将全部写入实例切换到新协议。恢复入口后再次核对新增行与身份的一致性，并验证重复登记和事务回滚。
4. 确定消息完整内容的备份和排障窗口，显式设置保留期后才开启清理。详细命令、配置与核对入口见[接入与运维指南](../OPERATIONS.md)。

## 边界与后续

- `PUBLISHED` 表示生产端成功记录回执，不代表消费者完成。`PENDING`、`PUBLISHING`、`RETRY_WAIT` 和 `DEAD` 不清理；没有在线完整消息归档。身份表永久增长，重放审计表另有保留策略。
- 清理后 Outbox 中的 Payload、Headers 和状态历史不可在线查询；关闭开关可停止后续删除，但不能直接回滚到只依赖 Outbox 唯一键的旧版发布器。备份恢复须使 Outbox、身份和重放审计处于相容时间点。
- 真实服务验收使用单 Broker 测试环境。生产迁移、生产清理和多 Broker 行为未执行。M5.3 的 26/26 轮基准矩阵是 M7 写入协议之前的历史结果，本次未重跑；不据此声称新协议性能。
- M7 代码尚未纳入此前 M5.4 发布检查的候选工件。正式发布须固定最终源码、重新做发布检查和外部工件下载验证。
