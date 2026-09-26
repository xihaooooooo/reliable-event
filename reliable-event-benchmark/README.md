# ReliableEvent 基准测试

本模块为 M5.3 提供独立的真实 MySQL/RocketMQ 基准环境。负载进程使用公开 `ReliableEventPublisher` 在 Spring 事务中登记事件；一个或两个 Starter 进程自动发布；独立的 RocketMQ SimpleConsumer 保存 Broker 消息身份。造历史行、负载登记、发布、接收和报告分别运行，不依赖私有优惠券项目。

2026-09-26 的完整实测见 [M5.3 完成记录](../docs/progress/M5_3_COMPLETED.md)、[逐轮矩阵](evidence/m53-full-d-matrix.csv)与[原始证据包](evidence/m53-full-d.zip)；证据包附有同名 `.sha256` 校验文件。结果只适用于记录的环境与配置。

## 环境与启动

当前脚本在 Windows 上验证，需要 JDK 17 或更高版本、Maven 3.9、Python 3 标准库、PowerShell 5.1 或 7，以及可用的 Docker Engine。环境采集使用 Windows CIM，进程启动使用 `Start-Process -WindowStyle Hidden`。本地服务固定为 MySQL 8.0.36、RocketMQ 5.5.0；默认使用宿主机 3308 和 8081 端口。RocketMQ Proxy 的路由发布地址为 `127.0.0.1:8081`，不能只修改 Compose 的宿主机映射端口而不核对客户端可达性。运行前先停止占用 8081 的示例 Broker。

在仓库根目录执行：

```powershell
mvn -pl reliable-event-benchmark -am -DskipTests package
./reliable-event-benchmark/scripts/bootstrap.ps1
./reliable-event-benchmark/scripts/run-once.ps1 -RunId local-1 -Count 10000
```

`bootstrap.ps1` 启动容器，等待 Broker 注册及 Topic 路由就绪，创建 `reliable-event-benchmark` Topic、消费者组和正式 Outbox 表。它可重复执行。`run-once.ps1` 每轮**清空专用 `reliable_event_benchmark` 数据库的 Outbox 表**；Java 侧在清表和造数前再次校验当前数据库名。不要把 JDBC URL 指向业务库。运行结束后查看 `reliable-event-benchmark/results/local-1/report.md`、`summary.json` 和旁边的原始 CSV、进程日志、环境元数据、三种 SQL 执行计划。运行 ID 不能重复使用已有结果目录。

快速验证实验矩阵：

```powershell
./reliable-event-benchmark/scripts/run-matrix.ps1 -Profile pilot -Prefix pilot-20260926
```

完整矩阵：

```powershell
./reliable-event-benchmark/scripts/run-matrix.ps1 -Profile full -Prefix full-20260926
```

`pilot` 跑 100 条事件的单实例、双实例、丢失一次成功回执三组，仅用于核对脚本与故障路径。`full` 跑每组 1 万条本轮事件：单实例基线三轮；10 万、100 万总表规模各三轮；轮询间隔、抢占批次、线程数、队列容量四种单变量配置各三轮；双实例三轮；Broker 暂停和结果未知各一轮。完整矩阵可能持续较长时间，运行前确认磁盘空间与容器资源。各轮都保留原始文件；矩阵最后生成 `<Prefix>-matrix.csv` 与 `.md`。P99 只按每轮单独列示，不把各轮 P99 相加或平均。

矩阵完成后可打包原始证据，脚本同时生成 ZIP 的 SHA-256 校验文件：

```powershell
python ./reliable-event-benchmark/scripts/package-results.py ./reliable-event-benchmark/results full-20260926 ./reliable-event-benchmark/evidence/full-20260926.zip
```

单轮常用参数：

```powershell
./reliable-event-benchmark/scripts/run-once.ps1 -RunId table-100k-1 -Group history-100k -Count 10000 -HistoricalRows 90000
./reliable-event-benchmark/scripts/run-once.ps1 -RunId two-workers-1 -Group multi -Count 10000 -PublisherInstances 2
./reliable-event-benchmark/scripts/run-once.ps1 -RunId outage-1 -Group outage -Count 10000 -PauseBrokerSeconds 15
./reliable-event-benchmark/scripts/run-once.ps1 -RunId unknown-1 -Group unknown -Count 10000 -DropFirstReceipt
```

`-DropFirstReceipt` 的装饰器先通过真实 Broker 成功发送，再将**该进程**的第一份成功回执改为 `RESULT_UNKNOWN`，以确定性方式验证预期重复消息；它不模拟 Broker 完全未接收。`-PauseBrokerSeconds` 在负载开始前暂停 Broker，按指定秒数恢复，验证 `RETRY_WAIT` 与排空。两个故障场景均由独立接收探针核对稳定 `reliable_event_id` 与 Broker Message ID。`run-once.ps1` 在 `finally` 中恢复暂停的 Broker 并停止自己启动的 Java 进程；超时或失败时保留现场文件。

## 数据和口径

| 文件 | 内容 |
| --- | --- |
| `metadata.json` | Git 状态、硬件、运行 JDK、镜像 ID、全部生效参数 |
| `registration.csv` | 每个业务事务批次的提交返回时刻与耗时 |
| `events.csv` | 本轮每条 Outbox 事件的身份、状态、尝试次数、首次可用与发布字段 |
| `messages.csv` | Broker 接收的稳定事件 ID、业务 Key、Broker Message ID；接收进程在每条记录落盘后 ACK |
| `samples.csv` | 带实际时间戳的状态、MySQL/Broker CPU、内存、块读写、发布进程 CPU/内存、数据库连接与行锁等待累计值 |
| `publisher-*-meters.csv` | 各发布实例每秒采样的 Micrometer 发送耗时、回执延迟、积压与死信快照 |
| `faults.csv` | Broker 暂停、恢复或异常清理的实际时间戳；无故障轮仅含表头 |
| `explain-*.txt` | 到期候选、过期租约和状态聚合查询的 `EXPLAIN ANALYZE` 原文 |
| `summary.json` / `report.md` | 由同目录原始文件生成的汇总 |

`registration_events_per_second` 用登记进程的完整批次执行时间计算。发布整体吞吐从首批登记事务返回到最后一条 `PUBLISHED` 行所记录的时间；稳态吞吐使用本轮事件 `published_at` 时间戳排序后的 20% 至 80% 区间。`first_to_published_time_difference` 是 `published_at - first_available_at`，其中 `published_at` 由应用在 SQL 状态更新前取时钟值，不能解释成精确的数据库提交时刻。多实例测量前必须检查宿主机时钟一致性。`sender_failure_rate` 从发布进程日志的单次 Sender 调用结果计算；Outbox `attempt_count` 是抢占次数，不能直接当发送次数。重复率从接收探针按稳定事件 ID 分组计算；若 Broker 消息覆盖不完整，报告置空重复率而非写零。

`send_duration_by_outcome` 从各实例 `publish.duration` Timer 的最后一次快照合并累计次数与总耗时；最大耗时取整轮快照中观察到的峰值，可能漏掉采样间隔内过期的 Micrometer 窗口峰值。`first_available_to_successful_receipt_timer` 对应既有 `publish.lag`；一次成功回执生成一个样本，同一事件异常重发时可能生成多个样本。`publisher-*-meters.csv` 的文件采样每秒执行一次，统计值由 Micrometer 在 Sender 调用边界累计。

历史表规模通过预置 `PUBLISHED` 行构造，不把 100 万行再次发送给 Broker。`samples.csv` 的实际采样间隔受 Docker/SQL 调用耗时影响，应按时间戳计算变化，不能假设严格一秒。运行脚本中的 SQL 计划在发布完成后的独立窗口执行；活跃积压的执行计划如需研究，应另建专门实验。CPU 百分比取自 `docker stats`，100% 表示占用一个逻辑 CPU 的量级，不是整台机器的百分比。

## 回归与清理

```powershell
python -m unittest discover -s reliable-event-benchmark/scripts/tests -v
mvn verify
Set-Location reliable-event-benchmark
docker compose down
```

`docker compose down` 停止本模块容器并保留专用 MySQL 数据卷。确认不再需要测试数据库时，才在本模块目录执行 `docker compose down -v`；这会删除本模块的 MySQL 卷。`results/` 中的原始证据不会被上述命令删除，但不会逐个纳入版本管理；使用 `package-results.py` 将正式矩阵完整打包到 `evidence/`，包括通常被 Git 忽略的进程日志。
