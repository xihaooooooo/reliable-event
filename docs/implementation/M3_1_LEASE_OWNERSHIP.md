# M3.1：租约所有权与状态更新栅栏

> 状态：已完成
>
> 目标：在暂不实现过期租约恢复的前提下，让每次成功抢占都获得有明确所有者和有效期的租约，并确保只有仍持有有效租约的 Worker 才能完成事件状态更新。

## 为什么先做这一小步

M2 已经实现基于候选版本的条件抢占，并通过真实 MySQL 8.0 双 Worker 测试证明同一个候选版本只有一个 Worker 能获得执行权。当前抢占令牌包含事件版本，但不包含 Worker 身份和租约有效期：

```text
查询 EventCandidate(id, version)
        ↓
条件抢占为 PUBLISHING
        ↓
得到 ClaimedEvent(event, claimVersion, attemptCount, maxAttempts)
        ↓
事务外发送
        ↓
按 id + PUBLISHING + version 完成状态
```

如果获胜 Worker 在抢占后退出，事件会一直停留在 `PUBLISHING`。如果未来直接增加一个“扫描所有 `PUBLISHING` 并重置”的恢复任务，又没有租约所有权和有效期校验，恢复线程可能覆盖仍在正常发送的 Worker，旧 Worker 也可能在事件被接管后写回结果。

因此 M3.1 先建立恢复机制所依赖的安全边界：

- 抢占成功时写入租约所有者和截止时间；
- 抢占结果携带完整、不可变的租约令牌；
- 成功、重试和死信更新同时校验版本、所有者和租约有效期；
- 租约一旦过期，旧 Worker 不再拥有完成状态的权限；
- 事件离开 `PUBLISHING` 时清空租约字段。

M3.1 不负责把过期租约恢复为可重试状态。恢复扫描和接管流程属于 M3.2。

## 本阶段完成后的流程

```text
查询到期候选 id + version
        ↓
短事务：按 id + status + version 条件抢占
        ↓
PUBLISHING
attempt_count + 1
version + 1
lease_owner = 当前 Worker
lease_until = 数据库当前时间 + leaseDuration
        ↓
读取并返回完整租约令牌
        ↓
提交抢占事务
        ↓
事务外调用 EventSender
        ↓
短事务：按 id + PUBLISHING + claimVersion
             + leaseOwner + 租约仍有效
             完成 PUBLISHED / RETRY_WAIT / DEAD
        ↓
version + 1
lease_owner = NULL
lease_until = NULL
```

候选查询仍然不代表获得执行权。只有条件更新影响一行，并取得完整租约令牌的 Worker 才能调用 Sender。

## 租约语义

### 1. Worker 标识

每个 `JdbcEventPublicationWorker` 实例持有一个非空 `workerId`，并在抢占时写入 `lease_owner`。

约束如下：

- `workerId` 在 Worker 生命周期内保持不变；
- 不同 Worker 实例必须使用不同标识；
- 长度不得超过表字段 `lease_owner VARCHAR(128)`；
- 不能为空或只包含空白字符；
- M3.1 通过构造参数显式注入，测试不得依赖随机值；
- M4 自动配置时再决定默认生成策略，例如实例名、进程信息和随机后缀的组合。

`workerId` 只用于租约所有权和诊断，不作为业务幂等键，也不向普通业务调用方公开。

### 2. 租约时长

Worker 同时持有正数 `leaseDuration`。抢占成功时，`lease_until` 等于数据库当前时间加该时长。

约束如下：

- `leaseDuration` 必须至少为一毫秒；
- 精度按 MySQL `DATETIME(3)` 收敛到毫秒；
- M3.1 不实现租约续期；
- 后续接入 RocketMQ 时，默认租约时长必须大于发送超时和正常本地排队时间；
- M3.1 不把租约时长加入公开配置，M4 再由 Spring Boot 配置属性对外暴露。

### 3. 时间来源

租约创建、有效性判断和过期判断统一使用 MySQL 数据库时间，例如 `UTC_TIMESTAMP(3)`，不使用 Worker 所在 JVM 的本地时钟。

原因是多个应用实例的系统时钟可能存在偏差。如果一个时钟较快的实例使用自己的时间判断其他实例的租约，可能在租约实际仍有效时提前接管事件。

M3.1 只冻结租约时间来源：

- `lease_until` 由数据库时间计算；
- 状态完成时使用数据库时间判断租约是否仍有效；
- M3.2 的过期租约扫描也必须沿用相同数据库时间；
- 现有 `availableAt`、失败时间和退避时间继续使用当前可注入 `Clock`，本阶段不扩大为全局时间重构。

### 4. 租约有效边界

统一采用以下定义：

```text
lease_until > 数据库当前时间  → 租约有效
lease_until <= 数据库当前时间 → 租约过期
```

边界时刻视为已经过期，避免完成更新和恢复更新在等号处都认为自己有效。

租约过期后，即使 M3.2 的恢复线程尚未执行，旧 Worker 也不能再把事件更新为 `PUBLISHED`、`RETRY_WAIT` 或 `DEAD`。旧 Worker 的状态更新必须影响零行并明确失败。

## 抢占令牌

当前 `ClaimedEvent` 扩展为：

```text
ClaimedEvent(
    storedEvent,
    claimVersion,
    attemptCount,
    maxAttempts,
    leaseOwner,
    leaseUntil
)
```

其中：

- `storedEvent`：待发送的事件内容；
- `claimVersion`：抢占更新后的数据库版本；
- `attemptCount`：本次抢占完成后的累计发送尝试次数；
- `maxAttempts`：该事件允许的最大总发送次数；
- `leaseOwner`：获得本次租约的 Worker 标识；
- `leaseUntil`：数据库生成的租约截止时间。

该对象是内部不可变令牌。后续状态更新必须直接使用令牌中的 Owner 和版本，不能从 Worker 当前配置重新拼装，也不能先查询数据库中的最新 Owner 或版本再尝试更新。

基本不变量：

- `claimVersion` 必须为正数；
- `attemptCount` 和 `maxAttempts` 必须为正数；
- `attemptCount` 不得大于 `maxAttempts`；
- `leaseOwner` 不能为空或空白；
- `leaseUntil` 不能为空。

MySQL 8.0 不支持 `UPDATE ... RETURNING`。抢占条件更新成功后，继续在同一个短事务中按事件 ID 读取事件内容、版本、尝试次数、最大次数和租约字段，再构造 `ClaimedEvent`。

## 条件抢占

抢占 SQL 在 M2 条件基础上增加租约字段：

```sql
UPDATE reliable_event_outbox
SET status = 1,
    attempt_count = attempt_count + 1,
    lease_owner = ?,
    lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)),
    version = version + 1,
    updated_at = ?
WHERE id = ?
  AND status IN (0, 3)
  AND next_attempt_at <= ?
  AND attempt_count < max_attempts
  AND version = ?;
```

其中时间间隔参数由 `leaseDuration` 转换为微秒，但最终存储精度仍为毫秒。实现可以采用语义等价的 MySQL 8.0 表达式，不能先用 JVM 时间计算绝对截止时间再写入。

抢占规则保持不变：

- 更新一行表示获得租约；
- 更新零行表示候选已经失效，直接跳过；
- 只有成功抢占才增加 `attempt_count` 和 `version`；
- 不允许读取最新版本后重新尝试旧候选；
- `PUBLISHING` 事件不参与普通抢占，过期后的处理由 M3.2 完成。

`updated_at` 在 M3.1 中继续使用现有 Worker 时间，避免把阶段范围扩大为所有时间字段的迁移。租约正确性只依赖 `lease_until` 和数据库当前时间。

## 租约保护的状态更新

### 发布成功

发送成功后的更新调整为：

```sql
UPDATE reliable_event_outbox
SET status = 2,
    published_at = ?,
    lease_owner = NULL,
    lease_until = NULL,
    updated_at = ?,
    version = version + 1
WHERE id = ?
  AND status = 1
  AND version = ?
  AND lease_owner = ?
  AND lease_until > UTC_TIMESTAMP(3);
```

### 进入重试等待

可重试失败且尚未耗尽次数时：

```sql
UPDATE reliable_event_outbox
SET status = 3,
    next_attempt_at = ?,
    last_error = ?,
    lease_owner = NULL,
    lease_until = NULL,
    updated_at = ?,
    version = version + 1
WHERE id = ?
  AND status = 1
  AND version = ?
  AND lease_owner = ?
  AND lease_until > UTC_TIMESTAMP(3);
```

### 进入死信

发送次数耗尽或发生明确不可重试错误时：

```sql
UPDATE reliable_event_outbox
SET status = 4,
    last_error = ?,
    lease_owner = NULL,
    lease_until = NULL,
    updated_at = ?,
    version = version + 1
WHERE id = ?
  AND status = 1
  AND version = ?
  AND lease_owner = ?
  AND lease_until > UTC_TIMESTAMP(3);
```

三种更新都必须影响一行。更新零行统一表示当前 Worker 的租约令牌已经失效，必须抛出明确异常，不能静默认为状态已保存。

错误信息应至少包含事件 ID 和 Worker 标识，但不得打印 Payload、Headers 或其他敏感内容。

## 为什么版本和 Owner 都要校验

`lease_owner` 不能替代 `version`，`version` 也不能替代 `lease_owner`。

- 版本用于区分同一事件的不同抢占轮次；
- Owner 用于证明哪一个 Worker 获得了当前租约；
- 有效期用于证明该所有权此刻仍然有效。

即使一个 Worker 在未来再次抢占到同一事件，它的 `workerId` 可能相同，但 `claimVersion` 已经不同。保留版本校验可以阻止该 Worker 的旧请求覆盖自己的新抢占结果。

完整栅栏条件是：

```text
事件相同
状态仍是 PUBLISHING
抢占轮次相同
租约所有者相同
租约仍在有效期内
```

缺少任何一个条件，都不能证明调用者仍拥有完成状态的权限。

## Worker 行为

`JdbcEventPublicationWorker` 增加：

```text
workerId
leaseDuration
```

处理流程调整为：

```text
for candidate in candidates
    短事务：使用 workerId 和 leaseDuration 抢占
    如果未获得 ClaimedEvent
        continue

    事务外调用 Sender

    成功
        使用 ClaimedEvent 中的版本和 Owner 标记 PUBLISHED

    发送失败
        按现有规则选择 RETRY_WAIT 或 DEAD
        使用同一 ClaimedEvent 中的版本和 Owner 完成更新
```

Sender 调用仍然不能位于数据库事务中。M3.1 不增加租约续期线程，也不在发送期间主动刷新 `lease_until`。

如果 Sender 返回时租约已经过期，完成更新会失败，事件暂时保持 `PUBLISHING`，等待 M3.2 恢复。这可能导致后续重复发送，是至少一次投递语义的一部分。

## 预计代码改动

### `ClaimedEvent`

- 增加 `leaseOwner` 和 `leaseUntil`；
- 校验 Owner、截止时间和已有次数不变量；
- 继续保持包内可见和不可变。

### `JdbcOutboxRepository`

- `claim` 接收 `workerId` 和 `leaseDuration`；
- 抢占时使用数据库时间生成 `lease_until`；
- 抢占成功后读取并返回租约字段；
- `markPublished`、`markRetryWait` 和 `markDead` 增加 Owner 与有效期条件；
- 事件离开 `PUBLISHING` 时清空租约字段；
- 条件更新失败时抛出包含事件 ID 和 Worker 标识的明确异常。

### `JdbcEventPublicationWorker`

- 构造时接收并校验 `workerId` 和 `leaseDuration`；
- 抢占时把两者传给 Repository；
- 状态完成继续只使用返回的 `ClaimedEvent`，不在发送后重新获取租约；
- 保持现有批次顺序处理和返回成功发布数量的语义。

### 数据库表

现有建表脚本已经包含：

```sql
lease_owner VARCHAR(128) NULL,
lease_until DATETIME(3) NULL,
KEY idx_lease_recovery (status, lease_until, id)
```

M3.1 不需要修改表结构。`idx_lease_recovery` 在 M3.2 扫描过期租约时开始发挥作用。

## 单元测试清单

至少增加以下验证：

1. `ClaimedEvent` 接受合法 Owner 和截止时间；
2. 空白 Owner 被拒绝；
3. 空截止时间被拒绝；
4. Worker 拒绝空白 `workerId`；
5. Worker 拒绝零或负数 `leaseDuration`；
6. Worker 拒绝长度超过 128 的 `workerId`；
7. 已有尝试次数和最大次数不变量继续成立。

如果构造器校验只通过集成测试即可稳定覆盖，不要求为了测试新增公开类型或公开方法。

## MySQL 集成测试清单

继续使用 MySQL 8.0.36 Testcontainers，至少覆盖以下场景。

### 抢占写入租约

- 登记一条已经到期的事件；
- 使用明确的 Worker ID 和租约时长抢占；
- 状态变为 `PUBLISHING`；
- 数据库 `lease_owner` 等于当前 Worker；
- `lease_until` 不为空；
- `ClaimedEvent` 中的 Owner 和截止时间与数据库一致；
- `attempt_count` 和 `version` 各增加一次。

测试租约截止时间时，应读取抢占前后的数据库时间并断言截止时间落在合理区间，不能拿固定 JVM 时钟直接等值比较数据库时间。

### 完成状态需要正确 Owner

- 抢占后构造或取得 Owner 不匹配的旧令牌；
- `PUBLISHED`、`RETRY_WAIT` 和 `DEAD` 更新均影响零行并明确失败；
- 数据库状态、版本和租约字段保持不变。

### 过期租约不能完成状态

- 抢占一条事件；
- 将数据库中的 `lease_until` 设置为数据库当前时间之前；
- 原 Worker 尝试标记成功时失败；
- 原 Worker 尝试写入重试或死信时同样失败；
- 事件继续保持 `PUBLISHING`，等待 M3.2 恢复。

### 正常完成清空租约

- 有效租约下发送成功；
- 最终状态为 `PUBLISHED`；
- `lease_owner` 和 `lease_until` 均为空；
- 最终版本按抢占和完成各增加一次；
- Sender 执行期间仍然没有活动数据库事务。

失败进入 `RETRY_WAIT` 和 `DEAD` 的现有测试也应补充断言租约字段已被清空。

### 双 Worker 竞争继续成立

- 两个 Worker 使用不同 `workerId`；
- 两个 Worker 处理同一份候选版本；
- 仍然只有一个 Worker 获得租约并调用 Sender；
- 数据库中的 Owner 是获胜 Worker；
- 最终完成后租约字段被清空；
- `attempt_count = 1`，最终 `version = 2`。

### 同一 Worker 的旧轮次不能覆盖新轮次

- 同一个 `workerId` 获得一份旧抢占令牌；
- 使数据库版本前进，模拟后续抢占轮次；
- 使用旧令牌完成状态时失败；
- 证明 Owner 相同的情况下，版本栅栏仍然有效。

## 完成标准

- 每次成功抢占都会写入 `lease_owner` 和由数据库时间计算的 `lease_until`；
- 抢占结果携带 Owner、截止时间和现有版本令牌；
- `PUBLISHED`、`RETRY_WAIT` 和 `DEAD` 更新全部校验事件 ID、状态、版本、Owner 和租约有效期；
- 租约边界统一使用 `lease_until > UTC_TIMESTAMP(3)`；
- 租约过期后旧 Worker 不能完成任何状态；
- 正常离开 `PUBLISHING` 时租约字段被清空；
- Sender 调用继续位于数据库事务外；
- 双 Worker 条件抢占测试继续证明只有一个发送者；
- M0 至 M2 的 27 个已有测试继续通过；
- 新增租约测试全部通过；
- `mvn clean verify` 在 Java 17 和 MySQL 8.0 下通过。

## 本阶段明确不做

- 扫描和恢复过期租约；
- 将过期 `PUBLISHING` 转为 `RETRY_WAIT` 或 `DEAD`；
- 多个恢复 Worker 的并发竞争测试；
- 抢占后退出、发送后退出的独立进程故障注入；
- 租约续期或心跳；
- 后台定时调度；
- Worker 内并行线程池；
- RocketMQ 发送适配；
- Spring Boot 自动配置和外部配置属性；
- Micrometer 租约指标；
- 死信人工重放。

## 阶段性边界

M3.1 完成后，正常运行的 Worker 受到租约所有权保护，旧版本、错误 Owner 和过期租约都不能覆盖当前状态。但进程在抢占后退出时，事件仍然会保持：

```text
status = PUBLISHING
lease_until <= 数据库当前时间
```

它不会被普通候选扫描再次处理，因为普通抢占只接受 `PENDING` 和 `RETRY_WAIT`。这是刻意保留的阶段边界，不应在 M3.1 中通过无条件重置状态解决。

M3.2 将基于 `idx_lease_recovery (status, lease_until, id)` 限量扫描这些记录，并使用版本、Owner 和过期条件安全地恢复为 `RETRY_WAIT` 或 `DEAD`。

## 后续顺序

```text
M3.1 租约所有权与状态更新栅栏
        ↓
M3.2 过期租约恢复
        ↓
M3.3 并发接管与限量恢复
        ↓
M3.4 发布进程退出故障注入
```
