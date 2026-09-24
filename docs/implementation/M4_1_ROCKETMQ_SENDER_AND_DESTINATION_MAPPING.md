# M4.1：RocketMQ 发送适配与目标映射

> 状态：已完成
>
> 日期：2026-09-24
>
> 目标：在不改变现有 JDBC 抢占、租约和重试协议的前提下，引入真实 RocketMQ 5.x 普通消息发送适配，冻结事件到 Topic、Tag、Key、Body 和用户属性的映射，并通过真实 Broker 集成测试验证明确成功、明确可重试、明确不可重试和结果未知四类发送结果。
>
> 实施结果与回归数据见 [M4.1 阶段完成记录](../progress/M4_1_COMPLETED.md)。

## 为什么 M4.1 单独成阶段

M0 至 M3 已经证明数据库侧的可靠性协议：

- 业务数据与 Outbox 事件在同一本地事务中提交；
- 多个 Worker 使用候选版本进行条件抢占；
- Sender 在抢占事务提交后、数据库事务外执行；
- 失败事件按照指数退避进入 `RETRY_WAIT`；
- 明确不可重试错误和重试耗尽进入 `DEAD`；
- 版本、租约 Owner 和数据库截止时间共同保护状态更新；
- 进程退出后，过期租约可以被其他 Worker 恢复；
- 外部发送完成但 Outbox 未更新时，恢复后会再次发送。

实施前 `EventSender` 只有测试实现，因此 M4.1 需要把这些结论扩展到真实 Broker、RocketMQ 消息构造、客户端异常、消息 ID、Topic/Tag 路由和发送超时。

M4.1 只替换消息系统接缝，不同时增加后台调度、线程池、自动配置或指标。这样出现失败时可以明确判断问题来自：

```text
数据库状态协议
        或
RocketMQ 消息映射与发送适配
```

如果在同一阶段同时引入调度线程、并行执行和 Spring 生命周期，Broker 错误、线程竞态和 Bean 装配错误会混在一起，无法形成清晰的验证反馈环。

## 本阶段冻结的决定

M4.1 冻结以下决定：

- 使用 Apache RocketMQ 5.x gRPC Java SDK，不使用旧版 Remoting 客户端；
- 实现基线固定为 `rocketmq-client-java 5.2.1`；
- 集成测试服务端固定为 `apache/rocketmq:5.5.0`，同时启动 Broker 和 Proxy；
- 只发送普通消息，不发送 RocketMQ 事务消息、顺序消息、延迟消息或单向消息；
- 使用同步 `Producer.send(Message)`，只有获得非空且合法的 Broker Message ID 才视为明确成功；
- RocketMQ 客户端内部最大发送尝试次数固定为 `1`，持久化重试由 Outbox 状态机负责；
- 客户端内部尝试次数即使固定为 `1`，仍不对外承诺消息不会重复；
- `availableAt` 继续由 Outbox 扫描控制，不映射为 RocketMQ 延迟消息；
- `eventType` 通过显式映射解析为 Topic 和可选 Tag，不从事件类型字符串隐式推导目标；
- Payload JSON 的 UTF-8 字节作为消息 Body，不额外套一层新的 JSON 信封；
- `eventKey` 作为 RocketMQ Message Key；消费者幂等键仍为 `eventType + eventKey`；
- 事件 ID、事件类型和业务 Key 同时写入保留的消息属性，便于消费端关联和诊断；
- 调用方 Headers 作为 RocketMQ 用户属性传递，但必须经过名称、值和保留前缀校验；
- 缺失映射、非法目标、非法 Header 和确定的消息超限属于不可重试错误；
- 限流、服务暂时不可用和确定未成功的连接失败属于可重试错误；
- 超时、响应丢失和无法判断 Broker 是否已经接受消息的异常属于“结果未知”；
- 可重试与结果未知都进入现有重试路径，但二者不能在类型和后续指标中合并；
- 未识别的 Producer 发送异常默认归为结果未知，不能误判为不可重试；
- 不解析异常消息文本决定业务分类，分类依据固定客户端版本的异常类型、响应码和调用阶段；
- 不打印 Payload、完整 Headers、Access Key、Secret Key 或连接凭据；
- M4.1 不改变 Outbox 表结构，不增加 Broker Message ID 数据库列；
- 不使用 RocketMQ 事务消息叠加 Outbox，两套事务恢复协议不同时启用。

## 版本与协议基线

### 1. Java 客户端

新增生产依赖：

```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java</artifactId>
    <version>5.2.1</version>
</dependency>
```

选择该客户端的原因：

- 它是 RocketMQ 5.x 的 gRPC Java 客户端；
- 官方 `Producer` 接口提供同步发送并返回 `SendReceipt`；
- 官方 `MessageBuilder` 支持 Topic、单个 Tag、一个或多个 Key 和字符串用户属性；
- `ProducerBuilder` 可以预声明 Topic 并设置内部最大发送尝试次数；
- 客户端和 Broker 的至少一次边界与本项目已经证明的语义一致。

使用 shaded 版本 `rocketmq-client-java`，不使用 `rocketmq-client-java-noshade`。只有实际出现无法解决的依赖或日志冲突并留下复现证据时，才重新评估 no-shade 版本。

### 2. 服务端

真实 Broker 测试使用：

```text
apache/rocketmq:5.5.0
```

测试环境必须包含：

```text
NameServer
Broker
Proxy（提供 gRPC 接入）
```

测试不能只启动 Broker 后让 gRPC 客户端连接 Remoting 端口。所有容器镜像必须使用固定版本，不能使用 `latest`。

RocketMQ 5.5.1 已经发布源码和二进制包，但截至本文编写时，Apache 官方 Docker Hub 中可直接固定使用的最新镜像标签是 `5.5.0`，因此测试不引用尚不存在的 `apache/rocketmq:5.5.1` 标签。

如果实现时发现客户端 `5.2.1` 与服务端 `5.5.0` 存在阻塞性兼容问题，必须先提交最小复现并在文档中记录新的固定版本组合，不能在 CI 中静默漂移版本。

### 3. 客户端重试边界

RocketMQ SDK 自身支持发送重试，而且官方明确说明重试可能产生重复消息。ReliableEvent 已经有数据库持久化重试，因此生产者构建时固定：

```java
producerBuilder.setMaxAttempts(1);
```

这样做的目的不是获得 Exactly Once，而是避免出现两层独立重试策略：

```text
一次 Outbox attempt
        ×
多次 SDK 内部 attempt
```

Outbox 的 `attempt_count` 只表示获得租约并调用一次 Sender，不承诺等于 Broker 实际收到的网络请求数。即使 `maxAttempts = 1`，连接中断和响应丢失仍可能让 Broker 已接收消息而调用方没有获得成功结果。

## 模块边界调整

M4.1 是第二个生产适配出现的阶段，应建立最小但真实的模块边界。目标结构为：

```text
reliable-event-parent
├── reliable-event-core
├── reliable-event-jdbc
└── reliable-event-rocketmq
```

### `reliable-event-core`

包含：

- `EventId`；
- `ReliableEvent`；
- `ReliableEventPublisher`；
- 与公开发布 API 直接相关的异常；
- 跨 JDBC 和 RocketMQ 模块使用的内部发送模型与发送结果类型。

需要从当前 JDBC 模块抽出的内部类型至少包括：

```text
StoredEvent
EventSender
SendReceipt
EventSendException
EventSendFailureType
```

这些类型虽然为了跨 Maven 模块访问需要是 `public`，仍然放在明确的 `internal` 包中，并通过 `package-info.java` 声明不属于 `0.1.0` 用户 API。不能因为抽出模块就承诺通用 Broker 插件体系。

`EventSendFailureType` 扩展为：

```java
public enum EventSendFailureType {
    RETRYABLE,
    NON_RETRYABLE,
    RESULT_UNKNOWN
}
```

`EventSendException` 增加结果未知工厂方法：

```java
EventSendException.resultUnknown(message, cause)
```

### `reliable-event-jdbc`

依赖 `reliable-event-core`，继续拥有：

- Outbox Repository；
- 候选查询和条件抢占；
- `ClaimedEvent`、`ExpiredLeaseCandidate` 等 JDBC 状态模型；
- 发布 Worker；
- 重试、死信和租约恢复；
- 单轮恢复—发布编排。

JDBC Worker 对失败类型的处理冻结为：

| 发送结果 | 尚有尝试次数 | 已耗尽尝试次数 |
| --- | --- | --- |
| `RETRYABLE` | `RETRY_WAIT` | `DEAD` |
| `RESULT_UNKNOWN` | `RETRY_WAIT` | `DEAD` |
| `NON_RETRYABLE` | `DEAD` | `DEAD` |

`RESULT_UNKNOWN` 与 `RETRYABLE` 当前使用相同状态转换，但类型必须保留，供 M4.5 指标和日志区分。

### `reliable-event-rocketmq`

依赖 `reliable-event-core` 和 `rocketmq-client-java`，包含：

```text
RocketMqEventSender
EventDestinationResolver
MapEventDestinationResolver
RocketMqDestination
RocketMqMessageFactory
RocketMqSendFailureClassifier
```

生产依赖方向固定为：

```text
reliable-event-jdbc ───────→ reliable-event-core
reliable-event-rocketmq ───→ reliable-event-core
```

`reliable-event-jdbc` 和 `reliable-event-rocketmq` 之间不能形成生产依赖。RocketMQ 模块的端到端测试可以在 test scope 依赖 JDBC 模块。

### 拆分顺序

模块拆分必须先作为纯重构完成：

1. 创建 `reliable-event-core`；
2. 移动类型并更新包引用；
3. 不改变 SQL、状态机或发送行为；
4. 现有 67 个测试全部通过；
5. 再创建 `reliable-event-rocketmq` 并实现新行为。

不能一边移动类型一边修改状态机，使回归失败时无法判断原因。

## 目标映射

### 1. 目标模型

目标使用显式值对象表示：

```java
public record RocketMqDestination(String topic, String tag) {
}
```

约束：

- `topic` 必填，不能为空或只含空白；
- `tag` 可选，`null` 表示不设置 Tag；
- 空字符串或纯空白 Tag 一律拒绝，不能偷偷转换为无 Tag；
- Topic 和 Tag 的字符、长度限制按照固定 RocketMQ 客户端版本进行本地校验；
- 值对象创建后不可变；
- `toString()` 不包含凭据，因为目标本身也不保存凭据。

### 2. 解析接口

```java
interface EventDestinationResolver {

    RocketMqDestination resolve(String eventType);
}
```

第一版实现：

```java
MapEventDestinationResolver
```

它接收 `Map<String, RocketMqDestination>`，构造时复制并验证映射，运行时只读取不可变快照。

约束：

- Event Type 必须精确匹配，区分大小写；
- 不支持通配符、正则、默认 Topic 或层级回退；
- 缺少映射时抛出明确的不可重试发送异常；
- 映射缺失不能回退到把 `eventType` 当 Topic；
- 多个 Event Type 可以映射到同一 Topic，并通过不同 Tag 区分；
- 一个 Event Type 在一个运行实例中只能对应一个目标；
- M4.1 通过 Java 对象直接构造映射，YAML 绑定和启动期配置报告属于 M4.2。

示例：

```text
coupon-task-execute → topic=coupon-task-topic, tag=execute
coupon-remind       → topic=coupon-reminder-topic, tag=created
```

## RocketMQ 消息映射

每个 `StoredEvent` 映射为一条普通 RocketMQ Message：

| Outbox 字段 | RocketMQ 字段 | 规则 |
| --- | --- | --- |
| `eventType` | Topic/Tag | 通过 `EventDestinationResolver` 解析 |
| `eventKey` | Keys | 设置为唯一业务事件 Key |
| `payloadJson` | Body | UTF-8 编码，不增加外层 JSON |
| `headersJson` | User Properties | 解析为字符串键值并逐项校验 |
| `id` | `reliable_event_id` | 十进制字符串 |
| `eventType` | `reliable_event_type` | 保留属性 |
| `eventKey` | `reliable_event_key` | 保留属性，与 Keys 一致 |

### 1. Body

Body 直接使用：

```java
event.payloadJson().getBytes(StandardCharsets.UTF_8)
```

原因：

- Payload 已经在业务事务内完成 JSON 序列化；
- 发送阶段不能再次把 Payload 当 Java 对象序列化；
- 不改变现有消费者预期的业务消息结构；
- 重试发送必须产生相同业务 Body。

M4.1 不把以下内部字段放入 Body：

- Outbox 状态；
- `attempt_count`；
- `lease_owner`；
- `lease_until`；
- 数据库版本号；
- `last_error`。

这些字段属于生产端执行状态，不是业务事件内容。

### 2. Key

RocketMQ Keys 设置为 `eventKey`。消费者的完整幂等身份仍是：

```text
(eventType, eventKey)
```

不能使用 Broker Message ID 作为业务幂等键，因为同一 Outbox 事件重复发送时会得到不同 Message ID。

### 3. Tag

一个消息最多设置一个 Tag：

- 映射配置存在 Tag 时调用 `setTag(tag)`；
- Tag 为 `null` 时不调用 `setTag`；
- 不把多个 Tag 用 `||` 拼接到生产消息；
- 不根据 Header 动态改变 Tag。

### 4. 用户属性与保留属性

保留属性：

```text
reliable_event_id
reliable_event_type
reliable_event_key
```

调用方 Header 约束：

- Key 和 Value 都不能为空；
- Key 必须使用约定的安全字符集；
- Key 不能使用 `reliable_event_` 前缀；
- Key 不能与 RocketMQ 系统属性冲突；
- 属性数量和编码后总长度必须设置上限；
- 非字符串 JSON 值、嵌套对象或数组视为损坏数据；
- 任一 Header 非法时整条事件进入不可重试失败，不能静默丢弃单个 Header；
- 日志和 `last_error` 只记录非法 Header 名称的安全摘要，不记录值。

调用方 Headers 逐项使用 `MessageBuilder.addProperty(key, value)` 写入。保留属性最后写入，且由于前缀冲突已经提前拒绝，不存在覆盖顺序依赖。

### 5. 消息大小

RocketMQ 5.x 官方默认消息大小上限为 4 MiB，但 Broker 可以配置不同值。M4.1 引入明确的 `maxBodyBytes`：

```text
默认值：4 * 1024 * 1024
```

发送前按照 UTF-8 Body 实际字节数校验。超过本地限制时：

- 不调用 Producer；
- 抛出不可重试发送异常；
- Worker 将事件转为 `DEAD`；
- `last_error` 可以记录实际字节数和上限，但不能包含 Body 内容。

Broker 仍可能因为消息属性、服务端更小限制或其他协议约束拒绝消息。明确的消息非法响应同样分类为不可重试。

M4.1 不自动压缩、不截断 Payload，也不把大消息上传到其他存储。

## `RocketMqEventSender`

建议构造依赖：

```java
RocketMqEventSender(
        ClientServiceProvider provider,
        Producer producer,
        EventDestinationResolver destinationResolver,
        ObjectMapper objectMapper,
        int maxBodyBytes
)
```

职责顺序：

```text
1. 校验 StoredEvent
2. 解析 Event Type 对应的目标
3. 将 Payload JSON 编码为 UTF-8
4. 校验 Body 大小
5. 解析并校验 Headers JSON
6. 构造普通 RocketMQ Message
7. 同步调用 Producer.send
8. 校验 Broker SendReceipt 和 Message ID
9. 返回内部 SendReceipt
```

伪代码：

```java
public SendReceipt send(StoredEvent event) {
    RocketMqDestination destination = destinationResolver.resolve(event.eventType());
    Message message = messageFactory.create(event, destination);

    try {
        org.apache.rocketmq.client.apis.producer.SendReceipt receipt =
                producer.send(message);
        return requireValidReceipt(receipt);
    } catch (ClientException exception) {
        throw failureClassifier.classifyAfterSend(exception);
    } catch (RuntimeException exception) {
        throw EventSendException.resultUnknown(
                "RocketMQ send result is unknown",
                exception
        );
    }
}
```

本地映射、Body 和 Header 校验在调用 `producer.send` 之前完成。进入 `producer.send` 后再出现的未识别异常，默认结果未知。

Sender 不负责：

- 创建或关闭 Producer；
- 后台调度；
- 开启线程池；
- 自己执行循环重试；
- 更新 Outbox 状态；
- 记录 Micrometer 指标；
- 修改 Payload；
- 创建 Topic；
- 消费消息。

Producer 生命周期和 Spring Bean 装配在 M4.2 实现。

## 发送结果与错误分类

M4.1 将结果分成四类。

### 1. 明确成功

同时满足：

- `Producer.send` 正常返回；
- RocketMQ `SendReceipt` 非空；
- Message ID 非空且可转换为稳定字符串。

适配器返回内部：

```java
new SendReceipt(messageId)
```

JDBC Worker 随后按现有租约令牌更新为 `PUBLISHED`。

M4.1 不将 Message ID 保存到 Outbox 表。M4.5 结构化日志必须能够记录它；是否增加持久化列需要独立需求和迁移方案，不能顺手修改表结构。

### 2. 明确不可重试

包括：

- Event Type 没有目标映射；
- Topic 或 Tag 本地校验失败；
- Header JSON 损坏或不符合字符串键值约束；
- Header 使用保留名称；
- Body 超过本地限制；
- 客户端或 Broker 明确返回消息格式非法、资源不存在、认证失败或权限拒绝；
- 其他能够确定“相同事件和相同配置再次发送仍不会成功”的错误。

适配器抛出：

```java
EventSendException.nonRetryable(...)
```

Worker 直接转为 `DEAD`。

### 3. 明确可重试

包括：

- Broker 或 Proxy 明确返回限流；
- 服务暂时不可用；
- 路由暂时不可用；
- 在请求尚未被 Broker 接受前即可确定的连接建立失败；
- 其他明确的临时服务错误。

适配器抛出：

```java
EventSendException.retryable(...)
```

Worker 按现有指数退避进入 `RETRY_WAIT`。

### 4. 结果未知

包括：

- 发送请求超时；
- 请求可能已经到达 Broker，但响应未到达客户端；
- 连接在发送过程中中断；
- SDK 报告取消，但不能证明 Broker 没有接受消息；
- Producer 返回空 Receipt 或无 Message ID；
- 调用 `producer.send` 后出现未分类的客户端异常或运行时异常。

适配器抛出：

```java
EventSendException.resultUnknown(...)
```

Worker 的当前行为仍然是：

```text
尚有次数 → RETRY_WAIT
次数耗尽 → DEAD
```

下一次发送可能产生重复消息，这是正确的至少一次行为。不能因为最终 Outbox 为 `PUBLISHED` 就推断 Broker 中只有一条消息。

### 分类实现规则

`RocketMqSendFailureClassifier` 必须：

- 针对固定的 `rocketmq-client-java 5.2.1` 编写；
- 优先使用具体异常类型和结构化响应码；
- 不通过 `contains("timeout")` 等消息字符串判断；
- 保留原异常为 cause；
- 输出稳定、安全的外层错误摘要；
- 对未识别异常采取结果未知的保守策略；
- 不捕获 `Error`；
- 单元测试覆盖每个明确映射和默认分支。

如果客户端升级导致异常层次或响应码变化，必须先更新分类测试，不能只修改依赖版本。

## 凭据与敏感信息

M4.1 允许测试夹具直接构造 RocketMQ 客户端配置，但生产代码遵循：

- Access Key、Secret Key 和 Token 不进入 `StoredEvent`；
- 凭据不写入 Outbox Headers；
- 凭据不进入 Topic 映射；
- `EventSendException` 外层消息不拼接客户端配置对象；
- `last_error` 不保存连接串、完整 Endpoint、Payload 或 Header 值；
- 测试失败输出可以包含容器名称和固定测试 Topic，但不能输出凭据；
- M4.2 再负责从外部配置建立 Credentials Provider。

## 单元测试清单

### 目标映射

至少覆盖：

1. 已配置 Event Type 返回正确 Topic 和 Tag；
2. 无 Tag 目标保持无 Tag；
3. 缺失 Event Type 映射产生不可重试异常；
4. Event Type 区分大小写；
5. 空 Topic、空白 Topic、空白 Tag 被拒绝；
6. 构造后修改原始 Map 不影响解析结果；
7. 不支持默认 Topic 或隐式回退。

### 消息构造

至少覆盖：

1. Payload JSON 按 UTF-8 原样成为 Body；
2. `eventKey` 成为 Message Key；
3. Topic 和可选 Tag 正确设置；
4. 三个保留属性正确设置；
5. 合法 Headers 成为用户属性；
6. Header 保留前缀冲突被拒绝；
7. Header 非字符串值被拒绝；
8. Body 按字节而不是 Java 字符数量校验；
9. 超限 Body 不调用 Producer；
10. 不设置延迟时间、Message Group 或事务字段。

### 发送结果

至少覆盖：

1. 正常 Receipt 转换为内部 Message ID；
2. 空 Receipt 分类为结果未知；
3. 空 Message ID 分类为结果未知；
4. 明确客户端参数错误分类为不可重试；
5. 明确限流和临时不可用分类为可重试；
6. 超时和发送中断分类为结果未知；
7. 未识别 `ClientException` 分类为结果未知；
8. Producer 抛出的未识别 `RuntimeException` 分类为结果未知；
9. 原因异常得到保留；
10. 错误摘要不包含 Payload、Header 值或凭据。

### JDBC 状态机回归

扩展现有测试，证明：

- `RESULT_UNKNOWN` 在尚有次数时进入 `RETRY_WAIT`；
- `RESULT_UNKNOWN` 在最后一次尝试后进入 `DEAD`；
- `NON_RETRYABLE` 仍然立即进入 `DEAD`；
- 现有未知普通 Fake 异常的兼容行为有明确测试；
- M0 至 M3 的版本、Owner、租约和恢复断言不变。

## 真实 RocketMQ 集成测试

新增独立测试类，不继续扩大 `ReliableEventIntegrationTest`。建议放在 `reliable-event-rocketmq` 模块：

```text
RocketMqEventSenderIntegrationTest
RocketMqPublicationIntegrationTest
```

测试环境：

- MySQL 8.0.36；
- RocketMQ 5.5.0；
- RocketMQ Proxy；
- 固定测试 Topic；
- 测试侧 SimpleConsumer，仅用于读取和断言消息；
- 所有等待都有上限；
- 不使用固定 `Thread.sleep` 猜测 Broker 或消息状态。

### 场景一：正常发送

```text
事务内登记事件
        ↓
Worker 抢占
        ↓
RocketMqEventSender 同步发送
        ↓
获得 Broker Message ID
        ↓
Outbox 更新为 PUBLISHED
        ↓
SimpleConsumer 收到一条消息
```

必须断言：

- Outbox 最终为 `PUBLISHED`；
- `attempt_count = 1`；
- Topic、Tag、Key、Body 和属性全部符合映射；
- Broker Message ID 非空；
- 消费者收到的业务 Payload 与登记时 JSON 一致；
- Payload 和 Headers 不出现在测试日志中。

### 场景二：映射缺失

- 创建一个没有目标映射的 Event Type；
- Worker 完成抢占但不调用 Producer；
- 事件直接进入 `DEAD`；
- `attempt_count = 1`；
- `last_error` 包含安全、稳定的映射缺失摘要；
- Broker 中没有该事件消息。

该场景可以使用真实 JDBC 状态机，但不要求启动 Broker；如果与正常发送测试共享容器，仍必须断言 Producer 没有被调用。

### 场景三：消息超限

- 测试使用较小的显式 `maxBodyBytes`，不构造数 MiB 的测试常量；
- Body UTF-8 字节数超过限制；
- Producer 未被调用；
- 事件进入 `DEAD`；
- 错误摘要记录大小，不包含 Body。

另外增加一个真实 Broker 拒绝非法消息的适配测试，确认固定客户端版本的异常能够被分类为不可重试。该测试不能依赖错误消息文本。

### 场景四：Broker 或 Proxy 不可用

测试必须先让 Producer 与 Broker 建立成功连接，再通过受控网络代理或容器操作切断 gRPC 路径：

```text
Producer 已就绪
        ↓
切断客户端到 Proxy 的路径
        ↓
Worker 调用 Sender
        ↓
在有界超时内失败
        ↓
RETRY_WAIT
        ↓
恢复网络路径
        ↓
推进到 next_attempt_at
        ↓
再次发送并 PUBLISHED
```

必须断言：

- 首次失败不会标记 `PUBLISHED`；
- 首次失败进入 `RETRY_WAIT`；
- `next_attempt_at` 符合现有退避策略；
- 恢复连接后第二次尝试能够成功；
- 最终尝试次数为 `2`；
- 故障注入和等待都有明确超时；
- 测试结束后恢复代理并清理容器。

推荐使用 Testcontainers Toxiproxy 控制连接，不通过 Windows 防火墙或宿主机全局网络规则制造故障。

### 场景五：发送结果未知并产生预期重复

真实网络中精确切断“Broker 已接受、客户端尚未收到响应”的时点容易偶发。M4.1 使用真实 Broker 加确定性发送装饰器：

```text
第一次调用真实 Producer.send
        ↓
Broker 返回成功，测试记录第一个 Message ID
        ↓
测试装饰器丢弃 Receipt 并抛出 RESULT_UNKNOWN
        ↓
Outbox 进入 RETRY_WAIT
        ↓
第二次使用正常 Producer.send
        ↓
Outbox 更新为 PUBLISHED
```

测试侧消费者最终必须观察到两条消息：

- Topic、Tag、业务 Key 和 Body 相同；
- `reliable_event_id` 相同；
- Broker Message ID 不同；
- Outbox 最终为 `PUBLISHED`；
- `attempt_count = 2`。

该测试证明：RocketMQ 已真实接收第一次消息，而应用将结果视为未知时，Outbox 重试会产生重复消息。它不是 TCP 字节级故障测试，文档和测试名称不能声称已经精确模拟真实响应包丢失。

如果后续增加可确定控制 gRPC 响应的故障代理，可以再补充线级测试，但不能替代当前确定性验收。

## Producer 测试夹具

M4.1 的测试夹具负责：

- 建立客户端 Endpoint；
- 禁用测试环境 TLS 或按容器能力配置；
- 设置有界 Request Timeout；
- 预声明全部测试 Topic；
- 设置 `maxAttempts = 1`；
- 创建和关闭 Producer；
- 创建测试侧 SimpleConsumer；
- 在测试结束时确认所有客户端资源关闭。

生产 `RocketMqEventSender` 不拥有 Producer 生命周期，因此单元测试可以传入受控 Producer，M4.2 再由 Spring Bean 生命周期管理真实 Producer。

## 防止集成测试偶发失败

- 使用容器日志和客户端探测共同确认 Broker、Proxy 和 Topic 已就绪；
- 不用端口已打开代替 RocketMQ 路由已经可用；
- 每个测试使用唯一事件 Key；
- 每个消费等待和发送 Future 都有超时；
- 不用固定睡眠等待消息；
- 测试 Topic 在发送前显式创建；
- 测试 Consumer 明确确认或清理已读取消息；
- 网络故障通过测试专用代理控制，不修改宿主机全局规则；
- 失败信息记录事件 ID、测试 Topic 和阶段，不打印 Payload、Headers 或凭据；
- `finally` 或测试资源生命周期必须关闭 Producer、Consumer、代理和容器；
- 结果未知重复测试必须先确认第一次真实发送成功，再抛出测试异常；
- 不能用内存计数代替 Broker 中可消费消息的证据。

## 预计代码改动

### 父工程

- 增加 `reliable-event-core` 模块；
- 增加 `reliable-event-rocketmq` 模块；
- 固定 RocketMQ Java 客户端版本；
- 保持 Java 17 编译目标。

### Core

- 移动公开事件 API；
- 移动内部发送接缝；
- 增加 `RESULT_UNKNOWN`；
- 加强 `SendReceipt` 的非空 Message ID 约束；
- 增加内部包边界说明。

### JDBC

- 改为依赖 Core；
- 更新包引用；
- 让 Worker 对 `RESULT_UNKNOWN` 执行可重试状态转换；
- 不修改 Outbox SQL；
- 保留已有 Fake Sender 测试。

### RocketMQ

- 增加目标模型和解析器；
- 增加消息工厂；
- 增加 Sender；
- 增加固定版本异常分类；
- 增加单元测试和真实 Broker 集成测试。

### 文档

- M4.1 完成后新增对应进度记录；
- 更新 README 当前能力；
- 更新 HANDOFF 下一步为 M4.2；
- 简历文案只有在真实 Broker 测试通过后才能写“已接入 RocketMQ”。

## 实现顺序

```text
1. 纯重构抽出 reliable-event-core
        ↓
2. 运行现有 67 个测试
        ↓
3. 实现目标模型、映射解析和消息工厂
        ↓
4. 扩展发送失败类型，接入 RocketMQ Sender
        ↓
5. 完成映射、消息构造和异常分类单元测试
        ↓
6. 启动真实 RocketMQ 完成正常发送测试
        ↓
7. 完成不可用恢复和结果未知重复测试
        ↓
8. 运行完整 mvn clean verify
```

每一步完成后都应保持已有 JDBC 测试通过。不能等所有模块同时完成后再首次回归。

## 完成标准

- Maven 工程包含职责明确的 Core、JDBC 和 RocketMQ 模块；
- 现有 67 个测试在模块拆分后继续通过；
- `RocketMqEventSender` 使用固定版本 RocketMQ 5.x gRPC Java SDK；
- Producer 只发送普通同步消息，客户端内部最大尝试次数为 `1`；
- Event Type 通过显式不可变映射解析为 Topic 和可选 Tag；
- Payload、Key、保留属性和用户 Headers 的消息映射已经冻结并有测试；
- 缺失映射、非法 Header 和消息超限不会调用 Producer，并进入 `DEAD`；
- 明确临时故障进入 `RETRY_WAIT`；
- 结果未知拥有独立失败类型，并按照至少一次语义重试；
- 真实 RocketMQ 正常发送能够返回非空 Message ID，并将 Outbox 更新为 `PUBLISHED`；
- Broker 或 Proxy 不可用后，事件能够退避并在恢复后发送成功；
- 真实 Broker 加确定性结果未知注入能够产生两条相同业务事件、不同 Message ID 的消息；
- 集成测试不使用固定睡眠猜测状态；
- 所有客户端、代理和容器资源都能在测试后关闭；
- Outbox 表结构、抢占 SQL、租约所有权和恢复协议没有被破坏；
- `mvn clean verify` 在 Java 17、MySQL 8.0.36 和固定 RocketMQ 5.x 基线下通过。

## 本阶段明确不做

- Spring Boot 自动配置；
- YAML 配置绑定和条件装配；
- 后台定时调度；
- 有界并行发送线程池；
- 优雅停机和 Spring 生命周期管理；
- Micrometer 指标；
- 结构化生产日志；
- Broker Message ID 持久化；
- 租约续期；
- 死信人工重放；
- RocketMQ 事务消息；
- RocketMQ 延迟消息；
- 顺序消息和 Message Group；
- 单向发送和异步发送；
- 自动创建生产 Topic；
- 消费者 SDK 或消费者幂等实现；
- Kafka、RabbitMQ 等第二种 Broker；
- Payload 压缩、拆分或外部大对象存储；
- 生产环境网络分区和多 Broker 集群故障演练。

## 阶段性边界

M4.1 完成后，可以表述：

- 已实现 RocketMQ 5.x 普通消息发送适配；
- 已实现 Event Type 到 Topic/Tag 的显式映射；
- 已通过真实 Broker 验证正常发送、临时不可用恢复和结果未知重复投递；
- 已区分明确可重试、明确不可重试和结果未知发送错误；
- 已保留至少一次投递语义，没有把 Broker 接入描述为 Exactly Once。

但仍不能表述：

- 已完成 Spring Boot Starter；
- 已实现后台自动调度或并行发布；
- 已完成优雅停机；
- 已提供生产可观测性；
- 已避免所有重复消息；
- 已完成消费者幂等；
- 已通过生产集群验证。

## 后续顺序

M3.4 文档曾将后续运行时工作合并描述为 M4.2 和 M4.3。为缩小单阶段风险，当前计划细化为：

```text
M4.1 RocketMQ 发送适配与目标映射
        ↓
M4.2 Spring Boot 自动配置与 Starter
        ↓
M4.3 常驻调度与有界并发
        ↓
M4.4 生命周期与优雅停机
        ↓
M4.5 Micrometer、日志与 M4 总验收
```

该细化不扩大 `0.1.0` 范围，只把原 M4 的既定能力拆成更小的验证单元。

## 官方资料依据

- [RocketMQ Java Client SDK](https://rocketmq.apache.org/docs/sdk/02java/)
- [RocketMQ Java Client 5.2.1 release](https://github.com/apache/rocketmq-clients/releases/tag/java-5.2.1)
- [RocketMQ Server releases](https://github.com/apache/rocketmq/releases)
- [Apache RocketMQ 官方 Docker 镜像标签](https://hub.docker.com/r/apache/rocketmq/tags)
- [RocketMQ 5.x Java Producer API](https://github.com/apache/rocketmq-clients/blob/java-5.2.1/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/producer/Producer.java)
- [RocketMQ 5.x ProducerBuilder API](https://github.com/apache/rocketmq-clients/blob/java-5.2.1/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/producer/ProducerBuilder.java)
- [RocketMQ 5.x MessageBuilder API](https://github.com/apache/rocketmq-clients/blob/java-5.2.1/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/message/MessageBuilder.java)
- [发送重试和限流策略](https://rocketmq.apache.org/docs/featureBehavior/05sendretrypolicy/)
- [消息模型与大小限制](https://rocketmq.apache.org/docs/domainModel/05message/)
- [使用 Docker 启动 RocketMQ](https://rocketmq.apache.org/docs/quickStart/02quickstartWithDocker/)
