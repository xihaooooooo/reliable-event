# M3.4：发布进程退出故障注入

> 状态：已完成
>
> 目标：使用由测试父进程主动终止的独立 JVM，稳定复现“抢占后、发送前退出”和“外部发送已持久化、Outbox 状态更新前退出”两个故障窗口，验证其他 Worker 能够在租约过期后恢复事件，并用可重复测试证明至少一次投递产生的重复消息边界。

## 为什么 M3.4 必须使用独立进程

M3.1 至 M3.3 已经依次证明：

- 抢占会创建包含版本、Owner 和数据库截止时间的租约；
- 旧版本、错误 Owner 和过期租约不能完成状态更新；
- 过期的 `PUBLISHING` 可以按原租约快照恢复；
- 多个恢复者竞争同一候选时只有一个成功；
- 恢复或重新抢占后，仍在运行的旧 Worker 不能覆盖新状态。

这些并发测试都在同一个测试 JVM 中使用门闩控制线程交错。它们能够证明数据库栅栏，但还没有证明发布进程被操作系统终止后：

- 已提交的租约仍然留在数据库；
- 进程内存和 Java 栈完全消失不会破坏恢复协议；
- 外部发送已经产生持久结果、但 Outbox 没有记录成功时，系统确实会再次发送；
- 后续 Worker 不依赖旧进程执行 `finally`、抛出异常或主动释放资源。

如果只让 Worker 线程抛出异常，旧 JVM、连接池、测试对象和清理逻辑仍然存在，不能代表真实进程退出。因此 M3.4 必须启动一个独立 JVM，并由父测试进程在确认故障点后强制终止它。

## 本阶段冻结的结论

M3.4 冻结以下决定：

- 故障执行者必须运行在独立 JVM 中，不能用同 JVM 线程异常代替；
- 子进程在故障点阻塞，父进程收到明确协议消息后主动调用强制终止；
- 不依赖固定时长睡眠猜测子进程是否已经到达故障点；
- 租约到期继续通过 MySQL 数据库时间判断；测试可以在子进程退出后把截止时间推进到数据库过去，不等待真实 30 秒；
- 第二个故障场景使用独立提交的测试发送探针记录“外部系统已经接受一次发送”；
- 发送探针不与 Outbox 状态更新共享事务，不对事件 ID 设置唯一约束，必须允许同一事件留下两次投递记录；
- 本阶段证明至少一次投递的重复窗口，不承诺 Exactly Once；
- 本阶段仍不接入真实 RocketMQ。真实 Broker 成功、超时和停机恢复属于 M4；
- 不为了测试向生产 Worker 增加暂停钩子、测试回调或进程控制接口。

## 两个必须复现的故障窗口

### 场景一：抢占提交后、发送前退出

```text
子进程查询候选 v0
        ↓
短事务抢占成功
PUBLISHING, version = v1
attempt_count = 1
lease_owner = crash-worker
        ↓
抢占事务提交
        ↓
子进程报告 READY CLAIMED
        ↓
父进程强制终止子进程
        ↓
没有调用 Sender，没有投递记录
        ↓
租约过期
        ↓
父进程执行恢复
RETRY_WAIT, version = v2
        ↓
新 Worker 到达 next_attempt_at 后抢占
PUBLISHING, version = v3, attempt_count = 2
        ↓
发送一次并完成
PUBLISHED, version = v4
```

最终必须证明：

- 子进程退出后记录仍为 `PUBLISHING`，旧 Owner 和版本仍在数据库中；
- 子进程退出前没有产生投递记录；
- 恢复不增加 `attempt_count`；
- 新 Worker 能够获得第二次租约并完成；
- 最终只有一条投递记录；
- 最终 `attempt_count = 2`、`version = 4`；
- 事件不是因为子进程执行异常处理而恢复，而是由另一个执行者处理过期租约。

### 场景二：发送结果已持久化、状态更新前退出

```text
子进程抢占事件
PUBLISHING, version = v1, attempt_count = 1
        ↓
测试 Sender 向独立投递表提交一条记录
        ↓
Sender 返回成功
        ↓
子进程报告 READY DELIVERED
        ↓
父进程在 markPublished 前强制终止子进程
        ↓
Outbox 仍是 PUBLISHING v1
投递表已有第 1 条记录
        ↓
租约过期并恢复为 RETRY_WAIT v2
        ↓
新 Worker 抢占 v3，再次调用 Sender
        ↓
投递表出现第 2 条记录
        ↓
新 Worker 标记 PUBLISHED v4
```

最终必须证明：

- 第一次投递在子进程被终止前已经独立提交；
- 第一次投递后 `published_at` 仍为空，Outbox 仍为 `PUBLISHING`；
- 恢复线程不能推断第一次发送是否成功，必须按现有规则恢复；
- 新 Worker 会再次发送同一个事件；
- 两条投递记录具有相同事件 ID 和事件键，但拥有不同投递记录 ID 或消息 ID；
- 最终 Outbox 为 `PUBLISHED`，`attempt_count = 2`、`version = 4`；
- 重复投递是测试预期，不是测试失败。

该场景证明的是生产端至少一次语义。它不验证消费者幂等实现，也不能用 Outbox 最终为 `PUBLISHED` 推断下游只处理了一次。

## 为什么使用持久化发送探针

普通内存 Fake Sender 的状态属于子进程。子进程被强制终止后，内存计数和返回值都会消失，父进程无法证明发送动作已经发生。

M3.4 增加仅用于测试的持久化发送探针，例如：

```text
JdbcDeliveryProbeSender
```

每次 `send` 使用独立连接和独立提交向测试表插入一行，然后返回 `SendReceipt`。它模拟“外部消息系统已经持久接受消息”这一事实：

```text
Outbox 抢占事务        已提交
投递探针事务           单独提交
Outbox PUBLISHED 更新  尚未执行
```

探针表和 Outbox 位于同一个 Testcontainers MySQL，只是为了让测试证据可从父、子进程共同观察。二者不能共享事务，也不能把探针表描述为 RocketMQ 等价实现。

该设计验证的是事务边界和重复窗口，不验证 Broker 协议、网络超时、消息刷盘或 RocketMQ 消息 ID 语义。

## 测试投递表

新增测试资源，例如：

```sql
CREATE TABLE IF NOT EXISTS test_message_delivery (
    id            BIGINT NOT NULL AUTO_INCREMENT,
    event_id      BIGINT NOT NULL,
    event_key     VARCHAR(192) NOT NULL,
    worker_id     VARCHAR(128) NOT NULL,
    message_id    VARCHAR(192) NOT NULL,
    delivered_at  DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_test_message_id (message_id),
    KEY idx_test_delivery_event (event_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

约束如下：

- 不能对 `event_id` 或 `event_key` 设置唯一约束，否则无法记录预期重复；
- `message_id` 每次发送生成不同值，只用于区分两次投递；
- `delivered_at` 使用数据库时间；
- 测试清理必须先清空投递表，再清空 Outbox 和业务表；
- 该表只能存在于 `src/test/resources`，不能加入生产建表脚本；
- Payload 和 Headers 不写入投递表，避免测试日志和证据表复制敏感内容。

## 独立 JVM 测试夹具

新增测试专用主类，例如：

```text
dev.reliableevent.jdbc.fault.PublicationCrashProcess
```

它不启动完整 Spring Boot 应用，只创建完成故障步骤所需的最小 JDBC 对象：

```text
DriverManagerDataSource
JdbcTemplate
DataSourceTransactionManager
TransactionTemplate
JdbcOutboxRepository
JdbcDeliveryProbeSender
```

不启动完整应用的原因：

- M3.4 只验证 JDBC 租约和进程退出边界；
- 当前还没有 Spring Boot 自动配置和后台调度器；
- 最小夹具启动更快，输出更少，故障点更容易精确控制；
- M4 再增加真实 Starter 生命周期和优雅停机测试。

### 子进程模式

子进程只接受两个固定模式：

```text
AFTER_CLAIM_BEFORE_SEND
AFTER_DELIVERY_BEFORE_STATE_UPDATE
```

未知模式、缺少参数或非法事件 ID 必须快速失败，不能回退到默认行为。

### 场景一的子进程流程

```text
1. 连接父测试启动的 MySQL
2. 查询指定事件的候选版本
3. 使用明确 workerId 和 leaseDuration 短事务抢占
4. 确认抢占事务已经提交
5. 向 stdout 写入 READY CLAIMED ... 并 flush
6. 阻塞等待父进程输入
7. 不调用 Sender
```

### 场景二的子进程流程

```text
1. 连接父测试启动的 MySQL
2. 短事务抢占指定事件并提交
3. 调用 JdbcDeliveryProbeSender
4. Sender 独立提交投递记录并返回
5. 向 stdout 写入 READY DELIVERED ... 并 flush
6. 阻塞在 markPublished 之前
7. 不主动修改 Outbox 状态
```

父进程必须在第 6 步强制终止子进程。测试正常路径不会允许子进程继续执行状态完成。

## 父子进程通信协议

父进程不能通过轮询睡眠猜测子进程进度。子进程到达故障点后写出单行协议并立即刷新：

```text
READY CLAIMED <eventId> <claimVersion> <workerId>
READY DELIVERED <eventId> <claimVersion> <workerId> <messageId>
```

协议要求：

- 一行只表达一个就绪事件；
- 字段数量固定；
- 不包含 JDBC 密码、Payload、Headers 或完整连接串；
- 只有抢占事务提交后才能发送 `CLAIMED`；
- 只有投递记录提交且 Sender 返回后才能发送 `DELIVERED`；
- 父进程必须校验事件 ID、版本和 Worker ID 与预期一致；
- 父进程使用带超时的阻塞读取等待协议行；
- 意外 EOF、格式错误、超时或子进程提前退出都使测试失败；
- 子进程的诊断输出写入 stderr，不能伪装为 `READY` 协议。

## 启动子进程

父测试使用 `ProcessBuilder(List<String>)` 直接启动 Java，不拼接 shell 命令：

```text
<java executable>
-cp
<test classpath>
dev.reliableevent.jdbc.fault.PublicationCrashProcess
<mode>
<eventId>
```

跨平台约束：

- Java 路径基于 `java.home/bin` 解析；Windows 使用 `java.exe`，其他平台使用 `java`；
- 测试 classpath 优先读取 Surefire 提供的测试 classpath，缺失时才回退到 `java.class.path`；
- classpath 作为 `ProcessBuilder` 的独立参数传递，不手动添加引号；
- 不调用 `cmd.exe`、PowerShell、`sh` 或 `bash`；
- 数据库 URL、用户名、密码、Worker ID、租约时长和候选时间通过环境变量传递；
- 环境变量必须使用测试专用前缀，不能覆盖 `HOME`、`JAVA_HOME` 或其他系统变量；
- 父进程保留 `Process` 引用，并在 `finally` 中清理仍存活的子进程。

Testcontainers 暴露给宿主机的 JDBC URL 可以直接由子 JVM 使用。M3.4 不在 Docker 容器内再启动子 JVM。

## 强制终止与资源清理

父进程收到并校验 `READY` 后执行：

```text
process.destroyForcibly()
process.waitFor(timeout)
assert process.isAlive() == false
```

约束如下：

- 必须先观察到目标故障点，再终止进程；
- 不要求不同操作系统返回相同退出码，但必须确认进程已经退出；
- 进程退出后才能修改租约截止时间和执行恢复；
- 每个等待都必须有超时；
- `finally` 必须再次终止仍存活的进程；
- 父进程必须持续读取或合并有限输出，避免子进程因输出缓冲区写满而阻塞；
- 测试结束时不能遗留 Java 子进程；
- 不能依赖 JVM shutdown hook、`finally` 或 Spring 生命周期回调清理租约。

本阶段使用强制终止模拟崩溃，不测试 `SIGTERM`、Ctrl+C 或优雅关闭。优雅停机属于 M4。

## 租约过期与恢复

为了保持测试快速且可确定，父进程确认子进程已经退出后执行数据库更新：

```sql
UPDATE reliable_event_outbox
SET lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(3))
WHERE id = ?
  AND status = ?
  AND lease_owner = ?
  AND version = ?;
```

测试必须断言该更新只影响一行。它只推进租约截止时间，不修改状态、Owner、版本或尝试次数。

随后调用现有 `JdbcExpiredLeaseRecovery`：

- 恢复结果必须为 `1`；
- 状态变为 `RETRY_WAIT`；
- 版本从 `v1` 变为 `v2`；
- `attempt_count` 仍为 `1`；
- 租约字段被清空；
- `last_error` 等于固定租约过期原因；
- `next_attempt_at` 由数据库恢复时间加退避生成。

父进程读取 `next_attempt_at`，使用固定到该时刻的 Worker `Clock` 完成第二次抢占，不使用真实等待。

## 测试 Sender 的事务边界

`JdbcDeliveryProbeSender` 每次发送必须：

1. 获取与当前 Outbox 事务无关的新连接；
2. 使用自动提交或显式独立事务插入投递记录；
3. 确认插入成功后生成并返回 `SendReceipt`；
4. 不更新 `reliable_event_outbox`；
5. 不捕获并伪装数据库错误；
6. 不在内存中维护唯一证据。

父进程在终止子进程之前必须能从另一连接查询到投递记录。这证明发送证据已经提交，不依赖子进程退出时刷新内存。

新 Worker 复用同一种测试 Sender。第二次发送产生新 `message_id`，投递表行数从 `1` 变为 `2`。

## 预计代码改动

### 新增进程故障集成测试

新增独立测试类，例如：

```text
PublicationProcessFailureIntegrationTest
```

它单独负责：

- 启动 MySQL 8.0.36 Testcontainers；
- 准备 Outbox、业务表和测试投递表；
- 创建事件并启动子 JVM；
- 等待并校验故障点协议；
- 强制终止和回收子进程；
- 推进租约截止时间；
- 调用恢复和新 Worker；
- 断言 Outbox 状态和投递次数。

不要继续扩大已经较大的 `ReliableEventIntegrationTest`。

### 新增子进程主类

`PublicationCrashProcess` 位于 `src/test/java`，只用于故障测试：

- 严格解析模式和环境变量；
- 创建最小 JDBC 依赖；
- 只处理父进程指定的事件；
- 抢占失败时以非零状态退出；
- 到达故障点后输出协议并阻塞；
- 不包含重试循环和后台线程。

### 新增进程夹具

建议新增父进程辅助类，例如：

```text
PublicationProcessFixture
```

负责 Java 路径、测试 classpath、环境变量、协议读取、超时、强制终止和诊断输出。进程控制细节不能散落在每个测试方法中。

### 新增持久化测试 Sender

`JdbcDeliveryProbeSender` 位于测试源码：

- 实现内部 `EventSender`；
- 接收 DataSource 和 Worker ID；
- 每次发送独立插入一条投递记录；
- 返回唯一测试消息 ID；
- 可同时被父、子 JVM 使用。

### 测试资源

- 新增测试投递表建表脚本；
- 更新清理脚本，先删除投递记录；
- 不修改生产 `reliable-event-outbox.sql`；
- 不增加 Maven 生产依赖。

### 生产代码

正常情况下 M3.4 不修改生产状态机、Repository SQL 或 Worker 流程。现有 `jdbc.internal` 类型已经能够被测试夹具组合。

如果实现时发现必须给生产 Worker 增加“发送后暂停”钩子，应停止并重新检查夹具设计，不能为测试污染生产控制流。

## 子进程夹具单元测试清单

至少覆盖：

1. 两个合法故障模式能够解析；
2. 未知模式被拒绝；
3. 缺少数据库环境变量时快速失败；
4. 非法事件 ID、租约时长或候选时间被拒绝；
5. `READY CLAIMED` 协议能够解析；
6. `READY DELIVERED` 协议能够解析并保留消息 ID；
7. 错误前缀、字段缺失和非法数字被拒绝；
8. Java 可执行文件和 classpath 以独立参数传给 `ProcessBuilder`；
9. 超时和提前 EOF 被报告为明确失败；
10. 清理操作对已经退出的进程保持幂等。

单元测试不得真正启动长期阻塞子进程；进程生命周期由 MySQL 集成测试覆盖。

## MySQL 进程故障集成测试清单

继续固定 MySQL 8.0.36，并为每个场景设置明确超时。

### 抢占后、发送前终止

- 创建 `max_attempts = 2` 的到期事件；
- 启动 `AFTER_CLAIM_BEFORE_SEND` 子进程；
- 收到并校验 `READY CLAIMED`；
- 从父连接确认状态为 `PUBLISHING`、版本为 `1`、尝试次数为 `1`、Owner 为子进程 Worker；
- 确认投递表为空；
- 强制终止子进程并确认它已退出；
- 将当前租约推进到数据库过去；
- 恢复为 `RETRY_WAIT`，确认版本为 `2`、尝试次数仍为 `1`；
- 使用新 Worker 在 `next_attempt_at` 抢占和发送；
- 最终状态为 `PUBLISHED`、版本为 `4`、尝试次数为 `2`；
- 投递表恰好一行，来自新 Worker。

### 发送已提交、状态更新前终止

- 创建 `max_attempts = 2` 的到期事件；
- 启动 `AFTER_DELIVERY_BEFORE_STATE_UPDATE` 子进程；
- 收到并校验 `READY DELIVERED`；
- 从父连接确认投递表已经存在第一行；
- 确认 Outbox 仍为 `PUBLISHING`、版本为 `1`、`published_at` 为空；
- 强制终止子进程并确认它已退出；
- 推进租约截止时间并恢复为 `RETRY_WAIT`；
- 新 Worker 在退避到期后重新抢占和发送；
- 最终 Outbox 为 `PUBLISHED`、版本为 `4`、尝试次数为 `2`；
- 投递表恰好两行；
- 两行事件 ID 和事件键相同；
- 两行消息 ID 不同，Worker ID 分别属于旧、新 Worker。

### 子进程提前失败不会被误判为故障点

- 使用错误配置或不存在的事件启动子进程；
- 父进程不能收到合法 `READY`；
- 测试报告子进程退出码和受限诊断输出；
- 不推进租约、不执行恢复，也不把该情况计为故障注入成功。

该场景可以作为夹具单元测试或一个轻量进程测试实现，不要求为每一种非法输入重复启动 MySQL。

## 防止测试偶发失败

- 不使用 `Thread.sleep(...)`；
- 协议等待、进程退出、数据库操作和 Future 获取都设置超时；
- 子进程只有在事务提交后才发送就绪信号；
- 父进程收到信号后再次查询数据库验证真实状态；
- 强制终止后确认进程不再存活，再开始恢复；
- 租约使用数据库更新立即过期，不等待墙上时间；
- 新 Worker 使用读取到的 `next_attempt_at` 作为固定时钟；
- 每个测试使用不同事件 ID 和 Worker ID；
- `finally` 中无条件清理子进程和输出读取线程；
- 子进程日志设置长度上限，失败信息不得无限累积；
- 测试失败时保留模式、事件 ID、最后协议状态、退出码和有限输出，不能打印数据库密码或 Payload。

## 完成标准

- 存在可由父测试进程启动的独立 JVM 故障夹具；
- 父进程能够通过明确协议识别两个故障点；
- 子进程由父进程主动强制终止，不依赖线程异常、自杀退出或正常清理；
- 抢占后、发送前退出时，没有产生投递记录，事件可恢复并由新 Worker 发送一次；
- 发送证据独立提交后、Outbox 状态更新前退出时，事件可恢复并产生第二次投递；
- 两次投递使用同一事件标识，明确证明至少一次重复窗口；
- 两个场景最终都能由新 Worker 将 Outbox 更新为 `PUBLISHED`；
- 恢复不增加尝试次数，最终尝试次数和版本变化符合状态机；
- 所有进程等待和清理都有超时，不遗留子进程；
- 测试不使用固定睡眠猜测状态；
- 生产 Outbox 表结构和状态机不因测试夹具改变；
- M0 至 M3.3 的 58 个已有测试继续通过；
- 新增夹具单元测试和 MySQL 进程故障测试全部通过；
- `mvn clean verify` 在 Java 17 和 MySQL 8.0 下通过。

## 本阶段明确不做

- 接入真实 RocketMQ；
- 终止或重启真实 Broker；
- 验证 RocketMQ 消息 ID、刷盘和复制语义；
- 网络分区、半开连接或发送超时故障注入；
- 在 SQL 提交协议的任意字节位置杀进程；
- Docker 容器级或宿主机级崩溃测试；
- `SIGTERM`、Ctrl+C 和 Spring 优雅停机；
- 后台定时调度、并行 Worker 线程池和队列容量；
- 租约续期或心跳；
- `SKIP LOCKED` 替代协议；
- 消费者幂等实现；
- Spring Boot 自动配置、Micrometer 指标和死信人工重放。

## 阶段性边界

M3.4 完成后，可以表述：

- 已通过独立 JVM 故障注入验证抢占后进程退出可以由其他 Worker 恢复；
- 已稳定复现外部发送成功但 Outbox 未更新时的重复投递窗口；
- 项目的租约恢复和至少一次语义不仅有线程级竞态测试，也有进程级故障证据。

但仍不能表述：

- 已完成真实 RocketMQ 故障注入；
- 消息绝不重复；
- 已实现 Exactly Once；
- 已验证生产环境的操作系统、容器编排和网络故障；
- 已完成 Starter、后台调度、优雅停机和指标。

M3.4 收口 JDBC 层的 M3 宕机恢复阶段。真实消息系统和应用生命周期从 M4 开始验证。

## 后续顺序

```text
M3.4 独立 JVM 发布进程故障注入
        ↓
M4.1 RocketMQ 发送适配与目标映射
        ↓
M4.2 Spring Boot 自动配置与配置校验
        ↓
M4.3 后台调度、优雅停机与 Micrometer 指标
```
