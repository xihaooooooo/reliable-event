# ReliableEvent

ReliableEvent 是一个面向 Spring Boot 的事务 Outbox 发布库。业务服务在自己的数据库事务中写业务数据并调用 `ReliableEventPublisher.publish` 登记事件；后台发布器之后从 Outbox 领取到期事件，向 RocketMQ 或 Kafka 同步发送，并根据 Broker 确认结果更新状态。当前仓库实现的是生产端登记、发布、重试、运维查询与清理；它不提供通用消费容器、跨数据库事务、Exactly-once Broker 投递或管理 HTTP API。

## 目标与边界

- 业务行和待发事件写在同一个 Spring JDBC 事务里：事务提交后两者一起存在，回滚时两者一起撤销。
- 发布与业务事务异步解耦。Broker 不可用时，请求线程仍只执行业务数据库事务；后台发布按策略重试。
- 事件身份是 `(event_type, event_key)`。Outbox 身份表对该组合加唯一约束，用于重复登记返回原 `EventId`。相同身份的后续调用不会替换首次保存的 payload、headers 或可用时间。
- 发送结果可能未知。消息有机会已到 Broker，但确认在网络中丢失；重试会造成重复消息。因此业务消费者必须按稳定事件身份幂等。
- `PUBLISHED` 表示发送器收到了传输实现所定义的 Broker 成功确认，不能证明消费者已经处理。
- `ReliableEventPublisher` 当前由 JDBC 实现；库内的 Outbox SQL、时钟查询和状态更新采用 MySQL/InnoDB 方言。其他数据库未实现或验证。
- RocketMQ 与 Kafka 都是可选发送适配器。Kafka 示例另带一个示例消费者；该消费者不是库的公共 API。

## 模块

- `reliable-event-core`：`ReliableEvent`、`EventId`、`ReliableEventPublisher`、传输 SPI、确认对象、失败分类和事件头限制。
- `reliable-event-jdbc`：事务登记、Outbox 仓储、抢占与发送 Worker、过期租约恢复、指数退避、死信查询/重放、发布事件清理和 SQL 文件。
- `reliable-event-spring-boot-autoconfigure`：公共 `reliable-event.*` 配置、发布调度、传输选择、Micrometer 指标和可选 Micrometer Tracing 接入。
- `reliable-event-spring-boot-starter-base`：JDBC、JSON 与公共自动配置依赖。
- `reliable-event-rocketmq` 与 `reliable-event-rocketmq-spring-boot-autoconfigure`：RocketMQ 5 Java 客户端发送器、Topic/Tag 映射、凭证和连接配置。
- `reliable-event-rocketmq-spring-boot-starter`：RocketMQ 版本的便捷依赖。
- `reliable-event-kafka` 与 `reliable-event-kafka-spring-boot-autoconfigure`：Kafka Producer 发送器、Topic 映射、发送预算和 Producer 生命周期。
- `reliable-event-kafka-spring-boot-starter`：Kafka 版本的便捷依赖。
- `reliable-event-example`：RocketMQ 订单发布与消费示例，含 HTTP 应用、MySQL/RocketMQ Compose、手动死信操作调用示例。
- `reliable-event-kafka-example`：Kafka 订单发布和示例消费程序，含独立 MySQL/Kafka Compose。
- `reliable-event-benchmark`：真实 MySQL/RocketMQ 基准运行程序、采集/汇总脚本和历史原始证据。
- `observability`：可选的 Prometheus、Alertmanager、Grafana、Tempo 本地演示环境及规则、验收脚本。

## 发布与传输语义

`ReliableEvent<T>` 含 `eventType`、`eventKey`、非空 payload、`availableAt` 和字符串 headers。event type/key 不能为空白；构造时 headers 会复制为不可变 Map。登记时 payload 和 headers 序列化为 JSON；序列化失败抛出 `ReliableEventSerializationException`。公开的 `publish` 返回正数 `EventId`。

发布必须发生在活动、可写的 Spring 数据库事务中，且 Outbox 使用的 `DataSource` 必须正是该事务绑定的连接（不能是另一个 DataSource 或 auto-commit 连接）。发布器在调用数据库前校验这些约束。Outbox 插入和业务更新因此参与相同提交或回滚。identity 表的唯一 `(event_type,event_key)` 键保护并发重复登记；检测到重复时会返回现有 identity ID。

后台扫描 `PENDING` 与到期的 `RETRY_WAIT`，按 `next_attempt_at, id` 取一批候选。候选带 `version`；真正发送前在短事务中用条件更新抢占为 `PUBLISHING`、递增 `attempt_count` 和 `version`、写入 worker owner 与过期时间，再读取事件。队列中的候选尚未持有数据库租约。发送调用在数据库事务之外进行；成功后短事务更新为 `PUBLISHED` 并记录时间。状态写入同时核对状态、版本、owner 和尚未过期的租约，旧 Worker 不能覆盖新的领取者。

传输必须同步等待实际 Broker 确认。`RETRYABLE` 表示确认失败且可重试；`NON_RETRYABLE` 表示确定的永久拒绝并停止普通发送；`RESULT_UNKNOWN` 表示消息是否被接受不确定，会重试。未分类的 RuntimeException 按 `RESULT_UNKNOWN` 处理。永久拒绝、尝试耗尽及耗尽时租约恢复都会进入 `FALLBACK_PROCESSING`，在无数据库事务和行锁时调用应用可选的 `EventFallbackHandler`。发布和恢复流程必须在外部事务之外运行兜底；检测到外部事务时会拒绝处理，之后由租约恢复接手。正常返回写为 `FALLBACK_COMPLETED`；缺少 handler 或 handler 抛异常则写为 `FAILED`。handler 必须幂等：进程崩溃、租约过期或最终状态写入失败都可能导致再次调用。兜底完成不代表 Broker 发布成功，也不会更新 identity 的成功时间。发送成功但状态更新失败时，消息可能已被 Broker 接收；至少一次投递要求消费端去重。

状态码在 SQL 和 Java 中一致：

- `0 PENDING`：刚登记，等待发送。
- `1 PUBLISHING`：已被某个 Worker 抢占并持有租约。
- `2 PUBLISHED`：收到 Broker 确认且成功保存发布状态。
- `3 RETRY_WAIT`：可重试失败或租约超时，等待 `next_attempt_at`。
- `4 FAILED`：普通发送停止且兜底未成功完成；旧版 `DEAD` 行保留数值并对应此状态，无需重写状态码。
- `5 FALLBACK_COMPLETED`：应用兜底正常返回，不代表 Broker 已确认。
- `6 FALLBACK_PROCESSING`：应用兜底处理中；租约过期后恢复器抢占并只再次调用兜底。

`attempt_count` 在普通发送领取时递增，表示普通发送领取次数；兜底租约重抢不会增加它。默认最多 8 次。退避初始时间、基础时间上限和抖动比例必须显式配置。基础时间每次指数翻倍，达到配置上限后停止增长，再按配置比例增加随机抖动；最终等待时间可以超过基础时间上限。达到最大次数后，发送失败或租约恢复会调用兜底。最后错误最多保存 1024 个 Unicode 字符；兜底失败时会合并兜底异常摘要和原发送错误摘要。应用应避免异常消息携带凭证、payload 或个人数据。

## 消费端与 Broker 行为

本库只发布，不管理消费订阅、offset 或 ACK。RocketMQ 示例以事务写入 `(event_type,event_key)` 去重行和业务效果，然后在事务代理返回后 ACK；处理失败不 ACK，ACK 失败不撤销已提交的业务事务。Kafka 示例关闭自动提交，事务处理后同步提交 offset；处理失败会 seek 回失败记录并短暂停顿重试。两个示例都校验事件身份，并拒绝同一业务键对应不同 `EventId`。

发送到 Broker 的 payload 是数据库里保存的原始 JSON 文本，以 UTF-8 编码；传输层不会反序列化再序列化业务 payload。headers 数量上限 64，key 最长 128 字符且仅允许 ASCII 字母、数字、下划线、点和连字符；value 非空、UTF-8 最长 4 KiB；总 header 字节最多 16 KiB。`reliable_event_` 前缀保留给传输元数据。RocketMQ 和 Kafka 都附带 `reliable_event_id`、`reliable_event_type`、`reliable_event_key` 元信息。payload 默认最多 4 MiB，可分别配置。

## 安装与 Spring Boot 配置

当前 Maven 坐标版本由根 POM 管理，为 `dev.reliableevent:reliable-event-*:0.1.0-SNAPSHOT`。从源码集成可在仓库根运行：

```powershell
mvn -DskipTests install
```

应用选择一个 starter：RocketMQ 使用 `reliable-event-rocketmq-spring-boot-starter`，Kafka 使用 `reliable-event-kafka-spring-boot-starter`。若自己提供 `EventTransport`，可用 base starter。项目目标 Java 17，根 POM 使用 Spring Boot 3.5.16 和 Maven reactor。

在应用自己的服务方法里登记事件。业务表写入与 `publish` 必须使用相同的事务管理器和 `DataSource`：

```java
import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;

@Service
public class OrderService {
    private final JdbcTemplate jdbc;
    private final ReliableEventPublisher events;

    public OrderService(JdbcTemplate jdbc, ReliableEventPublisher events) {
        this.jdbc = jdbc;
        this.events = events;
    }

    @Transactional
    public EventId create(long orderId, String itemCode, int quantity) {
        jdbc.update("INSERT INTO example_order (id, item_code, quantity, created_at) "
                        + "VALUES (?, ?, ?, UTC_TIMESTAMP(3))",
                orderId, itemCode, quantity);
        return events.publish(new ReliableEvent<>(
                "order-created",
                Long.toString(orderId),
                new OrderCreatedPayload(orderId, itemCode, quantity),
                Instant.now(),
                Map.of("source", "orders")
        ));
    }
}

record OrderCreatedPayload(long orderId, String itemCode, int quantity) { }
```

这里的 `OrderCreatedPayload` 是应用定义的普通 JSON 可序列化类型。该方法应由 Spring 事务代理调用；不要在事务外手动调用，也不要让 Outbox 使用另一个数据库连接。

RocketMQ 的最小配置如下。连接地址是 RocketMQ 5 Proxy endpoint；Topic 和 Tag 应预先在 Broker 侧准备好，数据库需先执行本仓库的 Outbox DDL：

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/orders?connectionTimeZone=UTC
    username: ${DB_USER}
    password: ${DB_PASSWORD}

reliable-event:
  initial-retry-delay: 1s
  max-retry-delay: 5m
  retry-jitter-ratio: 0.2
  transport: rocketmq
  rocketmq:
    endpoints: localhost:8081
    mappings:
      order-created:
        destination: orders:created
```

Kafka 的最小配置如下。该 Producer 会等待 Broker 的 topic/partition/offset 确认，Topic 需预先创建：

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/orders?connectionTimeZone=UTC
    username: ${DB_USER}
    password: ${DB_PASSWORD}

reliable-event:
  initial-retry-delay: 1s
  max-retry-delay: 5m
  retry-jitter-ratio: 0.2
  transport: kafka
  kafka:
    bootstrap-servers: localhost:9092
    mappings:
      order-created:
        topic: orders
```

`reliable-event.transport` 未设置时：只有一种内置适配器则自动选择；同时安装 RocketMQ 与 Kafka 时必须显式设为 `rocketmq` 或 `kafka`。自定义 `EventTransport` 可用 `custom`；多个自定义或与内置实现冲突会在启动时失败。未选择的其他内置配置仅记录为未使用。

公共属性（均位于 `reliable-event`）：

- `enabled=true`：关闭时不启用 ReliableEvent 自动配置。
- `transport`：可选的 `rocketmq`、`kafka` 或 `custom`，默认未设置并按上述规则选择。
- `published-retention-enabled=false`、`published-retention`：默认不清理；开启时必须提供正的保留时长。
- `identity-retention-enabled=false`、`identity-retention`：默认永久保留已发布身份；开启要求同时启用发布行清理并提供正的 identity 保留期。
- `cleanup-batch-size=100`、`cleanup-interval=1h`：每次最多清理 1–1000 行；周期必须为正。
- `tracing-enabled=true`：仅当应用另有 Micrometer `Tracer` 与 `Propagator` 时创建追踪接入；不附带 exporter/SDK。
- `scheduling-enabled=true`：自动发布扫描和过期租约恢复。未配置任何 `EventTransport` 时默认启动失败；仅登记的 ingress 实例可设为 `false`。
- `claim-batch-size=50`、`recovery-batch-size=50`。
- `poll-interval=1s`；`adaptive-polling-enabled=false`；`active-poll-interval=100ms`。自适应模式在发现工作时按活跃间隔扫描，空闲或失败仍使用普通轮询间隔。
- `worker-threads=8`、`worker-queue-capacity=200`：有界工作池，容量总计为二者之和；队列中候选尚未占有租约。
- `shutdown-timeout=20s`：关闭阶段等待时长。
- `lease-duration=30s`：必须长于发送器受控请求预算；Kafka 还要求严格大于发送预算加 1 秒数据库状态更新预留。
- `max-attempts=8`。`initial-retry-delay`、`max-retry-delay`、`retry-jitter-ratio` 无默认值，启用 ReliableEvent 时必须显式配置；缺失会导致启动失败。两个等待时间至少为 1ms，最大基础等待时间不得短于初始时间；抖动比例为 0–0.2 的有限数，0 表示关闭抖动。`max-retry-delay` 限制的是叠加抖动之前的基础等待时间。
- `metrics-snapshot-enabled=true`、`metrics-snapshot-interval=15s`、`metrics-snapshot-query-timeout=2s`、`metrics-snapshot-timeout=5s`、`metrics-snapshot-shutdown-timeout=2s`。指标需应用提供 `MeterRegistry`；快照查询超时后以 NaN/缺失呈现，而非阻塞发布器。

RocketMQ 属性（`reliable-event.rocketmq`，未知属性会报错）：

- `endpoints`：必填，无空白字符的 RocketMQ Proxy endpoint 列表。
- `request-timeout=5s`、`max-body-bytes=4194304`、`ssl-enabled=false`。
- `access-key` 与 `secret-key`：可选，必须同时提供；也可提供自定义 `SessionCredentialsProvider`。
- `mappings.<event-type>.destination`：必填映射，写成 `topic` 或 `topic:tag`。Topic 校验字符与保留名称；Tag 不能空白或含控制字符。
- 默认客户端 Producer 的最大尝试次数设为 1，由 Outbox 负责重试；lease 时长必须超过 RocketMQ request timeout。Broker 客户端异常分类为可重试、永久拒绝或结果未知。

Kafka 属性（`reliable-event.kafka`，未知属性会报错）：

- `bootstrap-servers`：必填。
- `max-block=1s`、`request-timeout=3s`、`delivery-timeout=5s`、`linger=0s`。
- `custom-producer-send-budget`：默认未设置。只在应用自行提供唯一一个 `Producer<String,byte[]>` 时必填；其 lease 时长必须严格大于该预算再加 1 秒数据库状态更新预留。
- `shutdown-timeout=5s`、`max-body-bytes=4194304`。
- `mappings.<event-type>.topic`：必填；映射 Topic 必须为 1–249 个字母、数字、点、下划线或连字符，且不能是 `.` 或 `..`。
- `producer-properties`：可选额外 Kafka 属性；不允许覆盖由库控制的确认、幂等、重试、时间预算、序列化器、bootstrap、分区器、拦截器等设置，未知 Kafka 属性会被拒绝。
- 自动创建的 Producer 使用 `acks=all`、幂等、`max.in.flight.requests.per.connection=5`、最大重试次数，并以 Future 等待有效 Broker topic/partition/offset 确认。超时或等待异常归类为结果未知。自建 Producer 的关闭由应用负责。

## 数据库、建表与迁移

当前 DDL 使用 MySQL 8 的 `JSON`、InnoDB、`UTC_TIMESTAMP(3)`、`TIMESTAMPADD` 和 `DATETIME(3)`。连接与数据库会话应使用 UTC；两个示例 JDBC URL 均指定 `connectionTimeZone=UTC`。本库没有自动迁移器，不会在应用启动时建表或执行 SQL。

新库执行 [`reliable-event-outbox.sql`](../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql)，会创建 `reliable_event_outbox` 和 `reliable_event_identity`，含发布扫描、租约恢复和发布保留相关索引。Outbox 保存 payload/headers JSON、状态、可用/下次尝试时间、尝试次数/上限、租约、版本、兜底恢复信息、最后错误和发布/创建/更新时间。identity 表保存自增 EventId、唯一业务事件键、登记时间及成功发布时间。

已有数据库应先检查实际表结构，只应用适用的增量 SQL：

- `reliable-event-outbox-m4-5.sql`：为旧 Outbox 加 `first_available_at`；历史事件首次可用时间无法还原，需由运维决定补数。
- `reliable-event-outbox-m7-2.sql`：发布保留清理索引。
- `reliable-event-outbox-m8-1.sql`：为兜底租约恢复保存停止原因和失败分类。旧状态 `4` 保留数值并解释为 `FAILED`；已有 `reliable_event_replay_audit` 表归应用管理，库不会自动删除。
- `reliable-event-identity-m7-1.sql` 与 `...-check.sql`：旧库启用 identity 表时，在暂停登记的维护窗口创建/回填并检查 ID、键和 AUTO_INCREMENT 连续性。
- `reliable-event-identity-retention.sql` 与 `...-backfill.sql`：启用 identity 到期清理前增加 `published_at` 与索引，并在暂停登记和清理时回填。迁移脚本说明了已被旧版 Outbox 清理删除的孤立身份如何开始其新保留窗口。

迁移由部署方排期和执行；按目标版本检查脚本注释、`SHOW CREATE TABLE` 与校验 SQL，并在写入恢复前验证一致性。清理 identity 会释放该 `(event_type,event_key)` 的唯一键，之后同一业务键可能被重新登记为新事件；保留期应覆盖业务去重窗口。

## 失败处理与清理

原 `DeadEventOperations` 查询、详情、分页、重放 API，以及 JDBC 实现和审计表建表脚本已移除。相关配置属性也已删除。应用负责失败事件的查询、重投、补偿和删除，并自行管理旧审计表；本库不会自动删除已有审计表。应用可提供唯一的 `EventFallbackHandler` Bean；上下文含原始 JSON payload/headers、失败次数、停止原因、失败分类和错误摘要，不暴露内部存储类型。状态写入失败后兜底可能再次触发，应用应按业务键或事件 ID 保证幂等。

发布行清理默认关闭。显式打开 `published-retention-enabled` 并设置 `published-retention` 后，独立调度器按批次删除超过期限的 `PUBLISHED` Outbox 行；若同时启用 identity 保留，先将 Outbox 成功时间复制到 identity，再在同一事务中清理到期且没有 Outbox 对应行的身份。只有 `PUBLISHED` 行会由此任务清理；其他状态包括 `FAILED` 与 `FALLBACK_COMPLETED` 均保留给应用处理。清理按调度周期分批运行，不替代备份或审计保留策略。

## 运行示例

### RocketMQ 示例

在仓库根目录，Docker 可用时启动示例 MySQL、RocketMQ Proxy，并初始化 topic、consumer group、示例表和 Outbox：

```powershell
./reliable-event-example/scripts/bootstrap.ps1
mvn -pl reliable-event-example -am -DskipTests install
mvn -pl reliable-event-example spring-boot:run
```

应用默认监听 `8090`，POST `http://localhost:8090/orders`，JSON 为 `{"itemCode":"book","quantity":2}`，再 GET `/orders/{id}` 查看处理计数。该示例默认使用 MySQL 主机端口 `3307`、RocketMQ Proxy `8081`，配置在 `reliable-event-example/src/main/resources/application.yml`。停容器时从 `reliable-event-example` 执行 `docker compose down`；带 `-v` 会删除示例 MySQL 数据卷。

### Kafka 示例

在 `reliable-event-kafka-example` 目录启动隔离的 Kafka/MySQL：

```powershell
docker compose -f docker-compose.yaml up -d
```

从仓库根目录运行应用：

```powershell
mvn -pl reliable-event-kafka-example -am -DskipTests install
$env:EXAMPLE_KAFKA_CREATE_TOPIC = 'true'
mvn -pl reliable-event-kafka-example spring-boot:run
```

示例默认 HTTP `8082`、MySQL `13307`、Kafka `19092`。只有示例的 `EXAMPLE_KAFKA_CREATE_TOPIC=true` 会创建 topic；库自身不会创建。配置在 `reliable-event-kafka-example/src/main/resources/application.yml`。

两套示例都仅用于演示。Consumer 表结构位于各自 `src/main/resources/schema/example-tables.sql`，与 ReliableEvent 的 Outbox/identity schema 分开。

## 可观测性、日志和部署

有 Micrometer `MeterRegistry` 时会注册发送成功/失败计数、按失败类型分开的发送耗时、确认延迟、租约恢复、状态提交、清理、快照失败、调度失败、Worker 数量和 Outbox 快照指标。快照包括 backlog、failed、ready、ready 最老年龄、未完成超期数量/年龄及缺失首次可用时间数量。快照在独立线程和有超时的单条数据库查询中刷新；首次成功快照之前相关 Gauge 为空值。核心指标名以 `reliable_event.` 开头，例如 `publish.success`、`publish.failure`、`publish.duration`、`backlog`、`failed`、`ready`、`unfinished.overdue`、`snapshot.last_success_timestamp`、`scheduler.running`。Micrometer 会按注册表规则映射为 Prometheus 名称。指标不使用事件 ID、业务键、Trace ID 或错误内容作为标签。

例如 Micrometer counter `reliable_event.publish.success` 在 Prometheus registry 中导出为 `reliable_event_publish_success_total`；`reliable_event.publish.duration` 是按 `outcome` 标记的 Timer，导出为 `reliable_event_publish_duration_seconds_*` 系列。可选仪表盘和告警按 Prometheus 暴露名查询。

若应用另外启用 Micrometer Tracing，库会在登记和每次发布尝试建立 span，并调用应用提供的 Micrometer `Propagator` 提取/注入上下文；库传入的 carrier getter/setter 只放行 W3C `traceparent` 和 `tracestate`，不传 baggage。发布端只把严格匹配 W3C `00-<trace-id>-<span-id>-<flags>` 且 trace/span ID 非全零的 `traceparent` 当作父上下文；实际解析与生成仍由应用的 `Propagator` 完成。发布尝试每次生成独立发送上下文，不改写数据库中保存的登记头。Tracing 缺失或自身失败不应改变发布结果。Starter 不附带 Tracing SDK、Prometheus registry 或 OTLP exporter；依赖与采样/export 配置由应用决定。

结构化日志事件包括 `reliable_event.publish.failed`、`send.succeeded`、`publish.persisted`、`publication.state_update_failed`、`lease.recovered`、`retention.completed` 和调度/指标错误。普通日志会带 event ID/type/key、attempt 与 owner 等诊断字段；日志平台应按业务键敏感程度配置访问和保留。

本地观测栈的定义位于 `observability/`：loopback 绑定的 Prometheus `9090`、Grafana `3000`、Tempo `3200/4317/4318`、Alertmanager `9093`；脚本 `start.ps1`、`validate-rules.ps1`、`accept-platform.ps1`、`stop.ps1` 启停和验收。配置的规则阈值是演示值，部署时需结合业务期限、退避、轮询周期及数据库时钟调整。Grafana 示例启用匿名 Viewer，适用于本地演示配置；不能直接视为生产访问控制方案。运行详细规则验收会启动 Docker/应用并产生 `target/evidence` 证据，普通构建不会启动此栈。

## 测试、CI 与基准

根 CI (`.github/workflows/ci.yml`) 在 Java 17、Python 3.13、Docker 环境顺序运行 benchmark 报告脚本单测，再执行 `mvn clean verify`；验证步骤要求产生 Maven 测试报告且无失败、错误或跳过。RocketMQ 测试使用固定主机端口，故 reactor 顺序运行。模块测试包含 core SPI 校验、MySQL Testcontainers 的事务/并发/重试/重放/租约/清理/进程故障测试、RocketMQ/Kafka 适配器测试、Spring 自动配置约束，以及两种真实 Broker 示例集成和消费者追踪测试。它们需要 Docker 和镜像/依赖可用；文档更新本身未运行这些测试。

完整源码构建命令是：

```powershell
mvn clean verify
python -m unittest discover -s reliable-event-benchmark/scripts/tests -v
```

基准程序位于 `reliable-event-benchmark`。其脚本会运行隔离 MySQL/RocketMQ、生成测试数据、启动一个或多个发布进程和独立 Broker 接收探针，采集原始 CSV/日志、运行环境与 SQL 计划，再由 Python 脚本汇总。首次运行的最小流程如下：

```powershell
mvn -pl reliable-event-benchmark -am -DskipTests package
./reliable-event-benchmark/scripts/bootstrap.ps1
./reliable-event-benchmark/scripts/run-once.ps1 -RunId local-1 -Count 10000
```

`run-once.ps1` 会在专用 `reliable_event_benchmark` 数据库中重置两张 ReliableEvent 表；不要把 `BENCHMARK_JDBC_URL` 指向业务库。`run-matrix.ps1 -Profile pilot` 运行小型基线、多实例和丢失确认场景；`full` 会执行大量运行并消耗较多时间、容器与磁盘。历史 `.zip`、`.csv`、`.sha256` 文件是原始证据，不代表本次文档任务执行的结果；不在这里复述历史性能结论。项目当前只保证基准程序和脚本存在，不保证当前机器上 Docker、Broker 或某组性能数据可用。

## 当前限制

- 生产库层面仅有 MySQL/InnoDB 实现，未提供 schema migration runner。
- 投递语义是至少一次；Broker 接受消息与 Outbox 状态提交不是原子事务。
- 没有通用消费 SDK、自动去重存储、管理 API、用户身份授权或死信 UI。
- 清理/重放会影响数据与后续去重窗口，必须由应用配置和受限运维操作承担治理。
- 同一数据库上的 worker、业务事务和 Broker 状态均需独立监控；`PUBLISHED` 不代表下游完成。

## 许可证

Apache License 2.0，见 [`LICENSE`](../LICENSE)。
