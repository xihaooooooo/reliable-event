# M3.4 阶段完成记录

## 这一阶段做了什么

- 新增测试专用 `PublicationCrashProcess`，由父测试进程通过 `ProcessBuilder` 启动为独立 JVM；
- 子进程支持“抢占后、发送前”和“投递已提交、状态更新前”两个固定故障模式；
- 新增严格的 `READY CLAIMED` / `READY DELIVERED` 单行协议，只有对应数据库事务提交后才通知父进程；
- 父进程在收到并校验协议后查询数据库确认故障点，再调用 `destroyForcibly()` 主动终止子 JVM；
- Java 可执行文件、测试 classpath 和命令参数全部以跨平台参数列表传递，不依赖 shell；
- JDBC URL、测试凭据、Worker ID、租约时长和候选时间通过测试专用环境变量传递，不出现在命令行和协议中；
- 新增测试专用 `JdbcDeliveryProbeSender`，用独立提交记录外部投递证据；
- 新增 `test_message_delivery` 测试表，允许同一事件保存多次投递，同时不复制 Payload 和 Headers；
- 所有进程读取、故障点等待、强制退出和清理都有超时，没有使用 `Thread.sleep(...)` 猜测时序；
- 生产 Outbox 表、状态机、Repository SQL 和 Worker 流程没有因故障测试改变。

## 现在证明了什么

### 抢占后、发送前退出

独立 JVM 成功抢占事件并提交租约后，被父进程强制终止。终止前没有投递记录，终止后 Outbox 仍保持旧进程留下的 `PUBLISHING`、Owner、版本和尝试次数。

父进程将租约推进到数据库过去后，现有恢复组件把事件恢复为 `RETRY_WAIT`。新 Worker 到达 `next_attempt_at` 后取得第二次租约并成功发布。最终只有一条投递记录，来自新 Worker。

### 投递已提交、状态更新前退出

独立 JVM 抢占事件后，测试 Sender 使用独立提交写入第一条投递记录并返回成功。父进程从另一数据库连接确认该记录已经可见，同时确认 Outbox 仍为 `PUBLISHING` 且 `published_at` 为空，然后强制终止子进程。

租约恢复后，新 Worker 再次发送同一事件并完成 `PUBLISHED` 更新。最终投递表存在两条相同事件 ID 和事件键、不同消息 ID 的记录，稳定证明了至少一次投递的重复消息窗口。

两个场景最终都满足：

- 恢复动作不增加 `attempt_count`；
- 新 Worker 是第二次成功抢占，因此最终 `attempt_count = 2`；
- 旧抢占、恢复、新抢占和新完成各推进一次版本，最终 `version = 4`；
- 进程退出后的恢复不依赖旧 JVM 执行异常处理、`finally` 或 shutdown hook。

## 验证结果

执行 `mvn clean verify`，以 Java 17 字节码目标和 MySQL 8.0.36 Testcontainers 完成 67 个测试：

- 34 个单元测试，包括新增的 7 个故障模式、环境配置、协议解析和进程命令测试；
- 33 个 MySQL 集成测试，包括新增的 2 个独立 JVM 强制退出场景；
- 0 个失败、0 个错误、0 个跳过。

两个进程故障场景还分别单独执行通过，证明子进程测试 classpath、宿主机到 Testcontainers MySQL 的连接、Windows Java 进程强制终止和跨进程数据库可见性均可工作。

## 当前边界

- 持久化发送探针只模拟独立外部副作用，不是 RocketMQ 实现；
- 尚未验证真实 Broker 的消息 ID、刷盘、复制、超时和不可用恢复；
- 没有后台定时调度和有界并行线程池；
- 没有 Spring Boot 自动配置、外部属性校验和优雅停机；
- 没有租约续期、Micrometer 指标和结构化日志；
- `DEAD` 暂无人工重放接口；
- 没有实现消费者幂等，只明确要求下游使用稳定事件键去重。

因此当前可以表述为完成 JDBC 层的进程级宕机恢复验证和至少一次重复窗口故障注入，但不能提前表述为完成真实 RocketMQ 故障测试或 Exactly Once。

## 下一步

进入 M4.1，实现 RocketMQ 发送适配与事件类型到 Topic/Tag 的目标映射，并用真实 Broker 集成测试验证正常发送、配置缺失、消息过大、Broker 不可用和结果不确定等边界。
