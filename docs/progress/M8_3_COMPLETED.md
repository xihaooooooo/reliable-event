# M8.3：运行指标、快照采样与有界停止

状态：M8.3 已实施。M8.4–M8.6 尚未实施；本记录不表示 M8 整体完成。

## 实现

`JdbcOutboxRepository.readMetricsSnapshot` 用一条只读聚合 SQL 取得 backlog、dead、ready、ready age、连续未完成超期量与年龄、缺失首次可用时间数量。SQL 在数据库内用 UTC 语句时间计算年龄，并按 PENDING/RETRY_WAIT 的领取口径排除退避事件；连续未完成口径同时包含 PENDING、PUBLISHING、RETRY_WAIT，按首次 `first_available_at` 到期计算，不被退避时间重置。空集合年龄为 0；有记录但年龄时间全缺失时为 NaN。Payload 和 Headers 不进入聚合读取。

Micrometer 实现将七个聚合值与 `snapshot.last_success_timestamp` 放在同一不可变快照中原子替换。查询失败保留旧快照及旧成功时间。自动刷新、周期刷新和手动刷新经由同一个不排队准入门；忙或停止时立即跳过并输出有限 reason 的 debug 诊断。Sampler 仅在全局启用、MeterRegistry、默认指标 Observer、默认 Worker/Cycle/Scheduler 以及调度和快照开关全部满足时装配。自定义观察器或运行时不自动得到新后台线程；关闭周期采样后保留自动轮次限频刷新和不受限频影响的同步手动入口。

默认周期采样使用一条守护定时线程和一个单线程、无队列的查询执行槽。自动等待预算包含连接获取、SQL 和结果读取；Statement 单独设置查询超时，JDBC 的秒级 `setQueryTimeout` 参数对小于一秒的正值向上取整。等待超时后请求取消，但真实 JDBC 调用只有在底层返回后才释放槽和准入；因此忽略中断的查询不会造成线程、任务或借用连接增长。采样器关闭先关闭准入、废弃在途代次，再在独立 daemon 停止线程内按自己的预算结束并回调。它与发布 Scheduler 位于同一生命周期阶段，不延长发布方截止时间。手动刷新保持调用线程同步语义，仅受自己的 Statement 超时和应用连接池策略约束。

Worker inflight 在成功抢占后的发送状态处理范围内用 finally 归零，Scheduler queued 反映等待执行候选并以增减量维护。空闲和容量满但成功结束的自动轮次仍刷新 heartbeat；失败使用有限 mode/stage 标签。`publication.persisted` 和 `dead.entered` 在实际事务回调中登记 afterCommit；回滚、未登记 afterCommit 的事务路径或抢占所有权拒绝均不增加。发送后的状态更新失败单独计数。事件 ID、业务键、消息 ID、错误内容不作为标签。

## 验收证据

- Java 17、MySQL 8.0.36 Testcontainers：真实数据库逐次验证 PENDING → PUBLISHING → 未来 RETRY_WAIT；待测事件的首次时间保持不变，连续超期年龄维持约 300 秒，转为 PUBLISHED 后最老年龄降至其他记录约 240 秒。未来首次可用事件不计入，NULL 时间另计数；全部 ready 时间缺失时年龄 NaN、没有 ready 行时年龄为 0。实际读取还在 Asia/Tokyo JVM 默认时区下执行。
- 真实外层事务覆盖发送转为 PUBLISHED、发送终态 DEAD、租约恢复进入 DEAD 的提交与回滚。提交后对应进度回调增加；回滚后不增加且 Outbox 状态保留 PUBLISHING。该测试在真实 MySQL 上通过。
- Prometheus `PrometheusMeterRegistry.scrape()` 检查了 Gauge/Counter 导出名称、after-commit 计数及仅有有限 mode/stage/result 标签；文本中没有事件或消息标识。
- AutoConfiguration 覆盖默认 Registry 正向采样器启动，以及全局关闭、无 Registry、调度关闭、采样关闭、自定义 Observer、Worker、Cycle、Scheduler 等条件下不装配周期采样器。默认实例启动后 `scheduler.running=1`、`snapshot.periodic_enabled=1`。
- 真实 JDBC API fixture 经过 `JdbcTemplate → DataSource → Connection → PreparedStatement → ResultSet`，核验 Statement timeout、阻塞连接获取和忽略中断查询均保持单槽，自动等待超时和独立停止回调有界；迟到结果不能发布。释放旧查询后确认 JDBC 资源关闭，重建 Observer/采样器能成功更新新 Meter。
- `GenericApplicationContext` 同时管理真实 Sampler 与发布 Scheduler 的 SmartLifecycle 包装；在快照查询与租约恢复都阻塞时关闭 Context，记录同阶段停止起点和回调，确认两个有界 stop 并行返回。关闭后重建 Context，再释放旧查询，旧 Registry 的 Meter 已移除且新 Context Gauge 未被旧结果覆盖。Sampler/Context fixture 在 `finally` 释放阻塞并停止资源。
- MySQL 8.0.36 持有一条连接的 `LOCK TABLES ... WRITE`，另一连接运行快照聚合并在 `readMetricsSnapshot(1)` 的 Statement 超时内失败；测试耗时落在 0.75–5 秒范围，`UNLOCK TABLES` 在 `finally` 执行。自动采样的 5 秒单调等待预算另由连接获取和无视中断的查询夹具验证。
- Worker 的既有 Trace/DB 失败与所有权拒绝 fixture 同时检查 Micrometer：Broker 发出成功回执仍记录 `publish.success`，但 DB 写失败或 ownership 拒绝不增加 `publication.persisted`；两者分别增加 `publication.state_update_failure`，inflight 归零。
- MySQL `EXPLAIN` 验证当前 Outbox 索引：`idx_publish_scan`、`idx_lease_recovery`、`idx_dead_list`、`idx_published_retention`。此只读聚合在测试表上的计划为 `type=ALL`、`key=NULL`、估计 1 行。它需要覆盖状态与时间字段，现有扫描索引不能消除聚合整表读取；暂未增加索引。实际大表查询耗时受行数和存储负载影响，应在部署数据量下监控。

最终 Java 17 `mvn clean verify` 全仓通过，耗时 5 分 47 秒；33 份 Surefire XML 共 196 个测试，0 失败、0 错误、0 跳过，8 个模块均为 SUCCESS。完整日志为 `target/evidence/m8.3-full-verify.log`，退出码为 `target/evidence/m8.3-full-verify.exit-code.txt`（0）。Maven clean 前后均保留原 M8.1/M8.2 证据；首次验证曾因测试 DataSource fixture 编译缺少重载失败，诊断记录独立保存在 `target/evidence/m8.3-failed-clean-verify-1.log` 和对应退出码文件（1），修复后的最终全仓构建已通过。

## 时间口径与跨实例聚合

年龄和到期筛选来自单条数据库聚合语句的 UTC 时钟；`snapshot.last_success_timestamp` 与 `scheduler.last_success_timestamp` 来自应用 UTC Unix 时钟。采样快照描述的是同一个 Outbox 存储的当前总量，部署多个发布实例时不能把重复 Gauge 求和。`persisted`、`dead.entered`、failure 等实例计数先对每实例计算 `rate` / `increase` 再汇总；inflight 和 queued 按实例求和。快照成功时间与数据库年龄时钟存在应用/数据库时钟偏差，监控上应保留允许偏差并在数据库时钟同步后解释。
