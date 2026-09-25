# M4.4：停止抢占、等待在途发送与优雅停机

> 状态：已完成
> 基线：M4.3 已完成；Java 17、Spring Boot 3.5.16、MySQL 8.0、RocketMQ 5.x
> 目标：Context 关闭时停止新的自动抢占，撤销未抢占的排队候选，在有上限的时间内等待已进入执行阶段的任务完成，然后关闭默认 Producer 和本项目线程资源。

M4.3 的基本停止钩子曾在 `ReliableEventScheduler.stop()` 中立即中断发送线程，并在 `stop(Runnable)` 中立即回调，无法保证在途发送及其数据库状态更新完成后才销毁默认 Producer。M4.4 完善了生命周期，没有改变 Outbox 表、条件抢占、发送映射、重试状态机或至少一次投递语义。

实施结果与回归数据见 [M4.4 阶段完成记录](../progress/M4_4_COMPLETED.md)。

阶段顺序保持为：

```text
M4.3 常驻调度与有界并发
  → M4.4 有界优雅停机
  → M4.5 Micrometer 指标、结构化日志与 M4 总验收
```

## 本阶段冻结的语义

1. 停机只管理默认自动调度运行时。`scheduling-enabled=false` 时不存在该运行时；显式调用的 `JdbcEventPublicationCycle.runOnce()` 不纳入自动执行器的停机等待，调用方须自行协调。
2. 停止入口先关闭新的扫描、候选提交和抢占准入。已进入抢占准入的任务属于在途任务，即使尚未取得数据库租约，也在等待范围内。停止后的排队候选不得再抢占数据库。
3. 尚未抢占的排队候选从本地队列撤销，释放事件 ID 和容量槽位；数据库中的事件保持原状态、尝试次数和版本，供其他实例或下次启动扫描。
4. 正常排空期间不主动中断在途任务。等待范围覆盖条件抢占、RocketMQ 同步发送以及最终的 `PUBLISHED`、`RETRY_WAIT` 或 `DEAD` 数据库更新，不能以 Sender 返回作为任务完成点。
5. 停机有一个覆盖全部步骤的单调时钟截止时间。到期后发出中断、完成生命周期回调并允许 Context 继续关闭；不无限等待不响应中断的 SDK 或数据库调用。
6. 超时不伪造发送失败，也不直接修改已抢占事件的 Outbox 状态。无法完成状态更新的 `PUBLISHING` 事件仍由既有租约过期恢复接管；Broker 已接收而状态未更新时仍可能重复投递。
7. 默认 Producer 的关闭发生在正常排空完成或超时处理结束之后。用户提供的 Producer 仍由用户管理，本项目不调用其 `close()`。超时后如果外部调用无视中断仍在运行，不能承诺 Producer 关闭与该调用绝对不重叠；必须防止超时后再开始新的发送调用。

`PUBLISHED` 仍只表示生产端获得成功回执，不表示消费者已处理。M4.4 不增加 Exactly Once、租约续期、人工重放或消费者幂等实现。

## 停机流程与所有权

```text
Context 请求停止
      ↓
原子进入 STOPPING；关闭扫描、候选提交和抢占准入
      ↓
撤销未抢占的排队候选并释放本地预留
      ↓
发送执行器不再接收任务；等待已准入任务完成
      ├─ 截止前完成：全部任务释放本地预留 → 生命周期回调
      └─ 截止时仍有任务：中断并记录安全摘要 → 生命周期回调
      ↓
Spring 销毁自动创建的 Producer Bean 和其他资源
```

停止请求与候选派发可能并发。调度器应把“允许进入抢占”做成短暂的原子准入操作：在同一同步边界内检查 `RUNNING` 并登记在途任务，随后在边界外执行可能阻塞的数据库抢占。停止操作在该边界内切换到 `STOPPING`。这样停止标记生效后没有新的任务进入抢占流程，而此前已准入的任务会被计入等待。超时分支再切换到 `TIMED_OUT`，最终进入不可重启的 `STOPPED`。不要在持有停机锁时执行 JDBC 或 RocketMQ 调用，否则单个卡住的外部调用会使停机截止时间失效。

已经运行但尚未准入的任务看到 `STOPPING` 后直接结束。已准入任务在 `STOPPING` 期间可以完成原有流程；若切换到 `TIMED_OUT` 时还没有调用 Sender，须在发送入口再次检查状态并跳过发送，避免默认 Producer 进入销毁阶段后才开始新发送。已经进入 Sender 的调用可能忽略中断，需依赖其正常返回或 JVM 退出；旧租约令牌继续保护后续数据库状态。

排队任务释放使用现有幂等 `CandidateTask.release()` 语义。任务可能与队列撤销同时被执行线程取走，所有路径都只能释放一次槽位和事件 ID，不能把仍在执行的任务误判为已经排空。等待条件以执行器终止及已准入任务完成为准，不能只看队列为空或 `outstandingIds.size()`。

### 正常完成

最后一个已准入任务完成状态更新并释放本地资源后，停止线程结束等待并执行 `SmartLifecycle.stop(Runnable)` 的回调。Spring 随后销毁本项目自动创建的 Producer Bean。不能在 Scheduler 内直接关闭 Producer，也不能把 `Producer.close()` 放在 Sender 的 `finally` 中，以免多个任务共享 Producer 时提前关闭。

### 超时和异常

新增 `reliable-event.shutdown-timeout`，默认 `20s`，必须至少 `1ms`。它是从收到停止请求起到生命周期回调的总预算，包括关闭扫描、撤销排队、等待执行任务和超时中断；实现应使用单调时钟和剩余时间，不在每一步重新获得完整超时。该值不限制随后执行的 Producer `close()` 或整个 Context 关闭耗时。配置值需要在启动时校验并进入配置元数据。部署时 Spring 的 `spring.lifecycle.timeout-per-shutdown-phase` 应大于该值，并留出 Producer 销毁时间；文档与 Context 测试要覆盖这个关系，避免框架先于本项目截止时间推进销毁。

截止时间到达时，停止等待并中断剩余工作线程。若调用响应中断且 Worker 能安全完成既有失败或状态更新流程，以数据库实际结果为准；若无法完成，不猜测 Broker 是否接收，也不绕过版本和租约栅栏强行改成 `RETRY_WAIT` 或 `DEAD`。停机日志只记录超时、在途数量和可安全取得的事件 ID，不记录 Payload、Header 值或凭据。M4.5 再统一结构化字段和指标。

`stop()`、`stop(Runnable)` 和重复关闭必须共享同一次停止过程。每个传入的回调都应恰好执行一次，包含无任务、正常完成、超时、中断和内部异常路径。`stop(Runnable)` 不在 Spring 生命周期调用线程中阻塞到超时；等待可由本项目的专用停机任务执行。该任务也必须在结束后退出，不能留下新的常驻线程。无论哪个入口先触发，`isRunning()` 在进入 `STOPPING` 后都返回 `false`，调度器不允许重新启动。

## 配置与 Bean 生命周期

现有默认 Producer 由 `@Bean(destroyMethod = "close")` 管理，Scheduler 实现 `SmartLifecycle`。M4.4 应利用 Context 先停止生命周期组件、再销毁单例 Bean 的顺序：生命周期回调只在排空或超时处理结束时发出。默认 Producer 的销毁回调保留；用户自定义 Producer 不被本项目关闭。

如果需要增加专门的停机协调组件，它必须只在默认 Scheduler 路径装配，并且不能创建第二个自动扫描循环。`reliable-event.enabled=false` 或 `scheduling-enabled=false` 不应创建停机执行资源。用户自定义 `ReliableEventScheduler` Bean 时默认实现继续退让，用户负责其自定义运行时的关闭语义。

`shutdown-timeout` 只约束本项目等待时间，不改变 RocketMQ `request-timeout`、数据库查询超时或租约长度。停机开始时租约仍按数据库时间自然流逝；发送或状态更新超过租约时，现有旧 Owner 栅栏仍会拒绝完成。正常部署应综合单次发送、数据库状态更新、租约和 Spring 生命周期预算设置这些时长，不能用更长的停机等待冒充租约续期。

## 与现有代码的接缝

1. 修改 `ReliableEventScheduler` 的停止路径，保留 M4.3 的候选查询、本地容量限制、去重和“执行时才抢占”语义；增加明确的准入与在途跟踪，代替跨数据库调用持有停机读锁。
2. 将当前立即 `shutdownNow()` 调整为先拒绝新任务、撤销未抢占队列并等待已准入任务；仅在超时分支中断执行线程。扫描线程也须停止并计入同一截止时间。
3. 在 `ReliableEventProperties` 增加并校验 `shutdown-timeout`，由 `ReliableEventPublicationAutoConfiguration` 传给默认 Scheduler；同步更新属性元数据和绑定测试。
4. 保持 `JdbcEventPublicationWorker` 的发送分类、事务边界和数据库状态更新协议。若需要让停机后的“尚未发送”任务跳过 Sender，增加最小检查接缝，不复制或改写 JDBC 状态机。
5. 保留 M4.2 默认 Producer 的 Bean 销毁路径，验证 `stop(Runnable)` 回调、Producer `close()` 与在途发送的实际顺序。自定义 Producer 仍不由本项目销毁。

## 验收测试

### 单元与 Context 测试

1. 阻塞一个已进入 Sender 的任务并关闭 Context：确认停机先停止新候选和新抢占，排队的第二个候选未取得租约，Context 在释放 Sender 前尚未完成关闭，默认 Producer 也尚未关闭。
2. 释放 Sender 后确认状态更新先完成，随后生命周期回调和默认 Producer `close()` 执行；本地槽位、事件 ID 和线程均清理。成功、可重试失败、不可重试失败各沿用 Worker 原有状态机。
3. 将 Sender 阻塞至超过很短的测试用 `shutdown-timeout`：生命周期回调在上限内发生，Context 可以继续关闭；在回调前发出中断，默认 Producer 在超时处理后关闭。已抢占事件不被停机代码直接改状态，后续仍能通过租约恢复。对忽略中断的 Sender，验证关闭后不再开始新的发送，并明确允许已进入的调用晚于 Context 结束。
4. 构造“抢占中”“发送后状态更新中”和“执行线程刚从队列取出”的竞态，验证准入边界、等待范围和恰好一次释放；停机开始后不产生新的自动抢占。
5. 扫描、队列撤销、等待或回调路径抛异常时，仍完成有界退出、资源清理和回调；重复 `stop()` / `stop(callback)` 不重复关闭 Producer，不丢回调，也不允许重启。
6. 验证 `shutdown-timeout` 的默认值、YAML 绑定、非法值拒绝和配置元数据；`enabled=false`、`scheduling-enabled=false`、自定义 Scheduler 与自定义 Producer 均遵守原有条件装配和所有权规则。

测试使用同步器和有截止时间的等待断言，不依赖固定 `Thread.sleep(...)` 猜测调度顺序。超时测试使用短配置，但不得只断言“方法返回”；还要查询数据库状态、记录 Sender 与 Producer 调用顺序，并检查本地资源释放。

### 真实服务测试

沿用 MySQL 8.0.36 和 RocketMQ 5.5.0 Testcontainers 夹具，至少验证：

- 默认 Starter 正常关闭时，已经开始的真实 RocketMQ 发送和 Outbox `PUBLISHED` 更新在 Producer 关闭前完成，消息可由消费者收到。
- 关闭时仍在本地队列中的候选保持 `PENDING`、`attempt_count=0`、无租约；新应用实例能够继续发布它。
- 超时或进程退出后仍可能遗留 `PUBLISHING`，过期租约恢复与稳定事件 ID 的至少一次语义保持有效；不把“关闭成功”误写为“消息一定只发送一次”。

最后执行全仓 `mvn clean verify`，在独立的 M4.4 完成记录中填写实际环境、测试数量和结果。验收前不将 README、交接文档或简历改成“已完成优雅停机”。

## 完成边界

M4.4 完成后可以说“默认 Starter 关闭时停止新自动抢占，撤销未抢占候选，并在有界时间内等待在途发送和状态更新，之后关闭默认 Producer”。超时、强制杀进程或客户端不响应中断时仍依赖租约恢复，仍可能重复投递。Micrometer 指标、统一结构化日志和 M4 总验收属于 M4.5。

## 参考

- [项目方向文档](../PROJECT_DIRECTION.md)
- [当前交接](../HANDOFF.md)
- [M4.2 Spring Boot 自动配置与 Starter](M4_2_SPRING_BOOT_AUTOCONFIGURE_AND_STARTER.md)
- [M4.3 常驻调度与有界并发](M4_3_SCHEDULING_AND_BOUNDED_CONCURRENCY.md)
