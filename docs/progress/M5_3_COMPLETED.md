# M5.3 可复现基准测试完成记录

> 2026-09-26 在本地隔离环境完成。M5.2 私有业务验收已取消；这里的数据只描述本次机器、容器和负载。

## 交付与复现

新增 [基准模块](../../reliable-event-benchmark/README.md)：通过公开 `ReliableEventPublisher` 在真实 MySQL 事务中登记事件，由一个或两个默认 Starter 实例发送至真实 RocketMQ 5.5.0；独立 SimpleConsumer 按稳定事件 ID、业务 Key 与 Broker Message ID 保存接收记录。模块包含固定版本的 Compose、初始化、单轮与矩阵脚本、资源采样、三种 `EXPLAIN ANALYZE`、报告生成和证据打包脚本。故障注入分别暂停真实 Broker/Proxy，以及在一次真实发送成功后丢弃回执。

在 Windows、Docker Engine 可用且 3308/8081 端口空闲的环境，从仓库根目录运行：

```powershell
mvn -pl reliable-event-benchmark -am -DskipTests package
./reliable-event-benchmark/scripts/bootstrap.ps1
./reliable-event-benchmark/scripts/run-matrix.ps1 -Profile full -Prefix m53-full-d
python ./reliable-event-benchmark/scripts/package-results.py ./reliable-event-benchmark/results m53-full-d ./reliable-event-benchmark/evidence/m53-full-d.zip
python -m unittest discover -s reliable-event-benchmark/scripts/tests -v
Set-Location reliable-event-benchmark
docker compose down
Set-Location ..
mvn verify
```

单轮会清空**仅供基准使用**的 `reliable_event_benchmark` Outbox 表；Java 侧校验数据库名。脚本将历史数据预置、应用就绪、事务登记、发布采样、排空、执行计划与报告生成分开。每轮独立保存配置、环境、事件、Broker 消息、指标、资源时序和进程日志。完整原始证据为 [m53-full-d.zip](../../reliable-event-benchmark/evidence/m53-full-d.zip)，其 [SHA-256](../../reliable-event-benchmark/evidence/m53-full-d.zip.sha256) 为 `94046fb6273bf9afc11d71681d1fbff92caa1f8418d83e3248fec6c88e44c057`；ZIP 的 555 个文件通过 CRC 检查。[逐轮矩阵 CSV](../../reliable-event-benchmark/evidence/m53-full-d-matrix.csv)可直接浏览。另保留了 100 条事件的[先导矩阵](../../reliable-event-benchmark/evidence/m53-pilot-a.zip)，用于脚本和回执丢失路径核对，不计入下表。

## 实测环境与口径

- 宿主机：AMD Ryzen 9 7945HX、32 个逻辑处理器、约 16.8 GB 内存、SKHynix SSD、Windows NT 10.0.26200；Docker Engine 29.1.3，容器可见内存约 7.595 GiB。
- 运行 JDK 为 21.0.10、Maven 为 3.9.9，项目编译目标为 Java 17；Spring Boot 3.5.16、MySQL 8.0.36、MySQL Connector/J 8.0.33、RocketMQ 5.5.0、RocketMQ Java 客户端 5.2.1。镜像 SHA-256 ID、Git 基线 `dc3f25d9bff81721c610524727bd35a4e733ed93` 与工作区改动记录在每轮 `metadata.json`。本次使用的是有未提交改动的工作区。
- 每轮新增 10,000 条立即可用事件；历史规模组另外预置 90,000 或 990,000 条 `PUBLISHED` 行，历史行不发送给 Broker。Payload 为固定字段结构和 256 个 ASCII 填充字符；轮次 ID 与序号位数使实际字节数略有变化，本次没有逐条记录消息体字节数。
- “整批条/秒”使用第一批登记事务返回至最后一个 `published_at` 的时间差；三轮组报告该值的中位数。“P99”是每轮 `published_at - first_available_at` 的分位数范围，**没有平均各轮 P99**。`published_at` 在数据库状态更新前取值，且 `first_available_at` 可能早于登记事务提交，不能解释成严格的提交到数据库完成延迟。
- MySQL CPU 为 `docker stats` 在整轮采样的平均值区间，100% 约等于一个逻辑 CPU；采样间隔受命令耗时影响，并非严格 1 秒。锁等待取 MySQL `Innodb_row_lock_waits` 的轮内增量。发送耗时与成功回执延迟取 Micrometer 快照，最大值是采样期间观察到的峰值。

## 矩阵结果

26/26 轮均满足本轮行数守恒、最终 10,000 条 `PUBLISHED` 和 Broker 身份覆盖。合计登记与发布 260,000 个唯一事件；Broker 收到 260,001 条消息，其中额外 1 条来自预定的结果未知重投。260,000 条事件级延迟均非负。

| 配置（其余保持默认） | 有效轮次 | 整批条/秒中位数 | 逐轮 P99 范围 | MySQL 平均 CPU 范围 |
| --- | ---: | ---: | ---: | ---: |
| 1 万行基线：1 实例、1 秒轮询、批次 50、8 线程、队列 200 | 3/3 | 49.40 | 197.97–198.45 秒 | 9.9–10.2% |
| 总表约 10 万行 | 3/3 | 49.26 | 198.66–198.72 秒 | 10.2–10.4% |
| 总表约 100 万行 | 3/3 | 49.30 | 198.38–198.69 秒 | 11.3–11.8% |
| 轮询 200 毫秒 | 3/3 | 227.87 | 39.51–41.09 秒 | 36.2–40.1% |
| 批次 100 | 3/3 | 98.91 | 97.06–97.30 秒 | 18.5–19.0% |
| 16 线程 | 3/3 | 49.44 | 197.69–198.68 秒 | 9.6–10.7% |
| 队列 50 | 3/3 | 49.37 | 197.64–198.23 秒 | 10.0–10.9% |
| 2 个发布实例 | 3/3 | 97.90 | 98.57–99.57 秒 | 17.6–20.7% |

基线登记吞吐中位数为约 3,668 条/秒，整批发布吞吐中位数约 49.40 条/秒，20%–80% 稳态区间中位数约 48.91 条/秒。基线事件级 P50 为 100.18–100.65 秒，P95 为 189.99–190.47 秒。基线 MySQL 最大连接线程数为 33、运行线程数为 8–9，行锁等待增量均为 0。双实例组的行锁等待增量分别为 0、8、80，但三轮均无额外 Broker 消息。当前默认配置的速率受轮询与每轮领取数影响明显；增加线程或将队列从 200 缩至 50 没有改变这一组参数下的速率。队列 50 仍足以容纳单轮 50 条候选，不能据此推断更小队列的表现。

## 故障与执行计划

- Broker/Proxy 暂停组：配置暂停 15 秒，`faults.csv` 记录实际暂停约 18.6 秒；发送日志有 16 次 `RESULT_UNKNOWN` 失败。恢复后 10,000 条全部 `PUBLISHED`，Broker 记录 10,000 条、无额外消息、无死信；整批约 46.76 条/秒。暂停恢复、首个失败、重试与最终状态可由时间线、日志、`samples.csv`、`events.csv` 复核。
- 回执丢失组：真实 Broker 成功接收后，装饰器将一次回执改为 `RESULT_UNKNOWN`。最终 10,000 条 `PUBLISHED`、Broker 10,001 条；有且仅有 1 个稳定事件 ID 对应两条不同的 Broker Message ID，额外消息率为 `1/10001 ≈ 0.010%`，无死信。
- 1 万、10 万、100 万行三个表规模各三轮的到期扫描、过期租约扫描与状态聚合计划均保存原文，共 27 份；9/9 轮到期扫描使用 `idx_publish_scan`，租约恢复使用 `idx_lease_recovery`，状态聚合使用覆盖索引。执行计划在发布排空后采集，实际匹配行数为 0，说明索引选择，但不能证明大量活跃积压下的扫描成本。

健康组发送失败率和额外消息率为 0；两个故障组的单次 Sender 失败率分别约 0.160% 与 0.010%。`RESULT_UNKNOWN` 不能当作 Broker 一定未接收，也不能从这些单次故障样本推断一般故障概率。Broker 接收探针在记录后 ACK；它证明消息到达该探针，不等于业务消费者已完成处理。资源数据来自一台共享开发机，不应当作生产容量承诺。

## 验证与阶段边界

- `python -m unittest discover -s reliable-event-benchmark/scripts/tests -v`：3 个报告契约测试通过。
- `mvn verify`：8 个模块构建成功，134 个测试通过，0 失败、0 错误、0 跳过。
- 基准 Compose 已执行 `docker compose down`；未删除专用 MySQL 数据卷，`results/` 原始目录仍保留在本地。

M5.3 的脚本、原始证据、结果报告和全仓回归已齐备。2026-09-26 范围更新：M5.2 私有业务验收已取消，不再作为 `0.1.0` 发布条件；发布检查仍属于 M5.4。
