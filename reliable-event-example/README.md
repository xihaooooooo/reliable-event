# ReliableEvent 原创示例

这个小应用演示：创建订单时在同一 MySQL 事务中登记 `order-created` 事件；Starter 自动向 RocketMQ 发送；消费者用 `(event_type, event_key)` 去重，并在事务提交后 ACK。它使用虚构订单，不含私有优惠券项目代码。

运行环境：JDK 17 或更高版本（编译目标为 17）、Maven 3.9、Docker Desktop 或兼容的 Docker Engine。示例固定使用 MySQL 8.0.36、RocketMQ 5.5.0；占用本机 3307、8081 和 8090 端口。首次拉取镜像所需时间取决于网络。

## 从空环境启动

在仓库根目录执行 PowerShell：

```powershell
./reliable-event-example/scripts/bootstrap.ps1
mvn -pl reliable-event-example -am -DskipTests install
mvn -pl reliable-event-example spring-boot:run
```

第一条命令启动 MySQL、NameServer、Broker/Proxy，等待 Broker 注册和 Topic 路由可见，再创建 Topic/消费者组和三张示例表以及正式 Outbox 表。重复执行不会清除已有数据。第二条命令安装本地模块依赖，第三条启动示例应用。RocketMQ Java 客户端连的是 Proxy 的 gRPC 端口 `localhost:8081`。

打开另一个 PowerShell 窗口：

```powershell
$created = Invoke-RestMethod -Uri http://localhost:8090/orders -Method Post -ContentType 'application/json' -Body '{"itemCode":"book","quantity":2}'
$created
./reliable-event-example/scripts/wait-order.ps1 -OrderId $created.orderId
```

创建接口返回 `orderId` 和 `eventId`。等待命令有 60 秒截止时间，最终的 `handledCount` 应为 `1`。也可以直接访问 `GET http://localhost:8090/orders/{orderId}`；处理尚未完成时 `handledCount` 为 `0`。

从仓库根目录进入示例目录后，可以用以下只读 SQL 对照生产与消费事实：

```powershell
Set-Location reliable-event-example
docker compose exec -T mysql mysql -uexample -pexample reliable_event_example -e 'SELECT id,event_type,event_key,status,attempt_count FROM reliable_event_outbox ORDER BY id DESC LIMIT 10'
docker compose exec -T mysql mysql -uexample -pexample reliable_event_example -e 'SELECT event_type,event_key,event_id FROM example_consumed_event ORDER BY consumed_at DESC LIMIT 10'
docker compose exec -T mysql mysql -uexample -pexample reliable_event_example -e 'SELECT order_id,handled_count FROM example_order_effect ORDER BY order_id DESC LIMIT 10'
```

Outbox 状态编码为 `0=PENDING`、`1=PUBLISHING`、`2=PUBLISHED`、`3=RETRY_WAIT`、`4=DEAD`。正常路径中对应事件最终为 `PUBLISHED`，`attempt_count` 至少为 1，消费者去重表和处理结果各有一行。`PUBLISHED` 只说明生产端收到 Broker 成功回执；消费者结果须以示例表为准。

## 链路追踪

示例包含 Spring Boot Actuator 与 Micrometer OpenTelemetry Bridge，使用 W3C Trace Context；`management.tracing.sampling.probability` 默认设为 `1.0` 以便本地演示，可用 `EXAMPLE_TRACE_SAMPLING_PROBABILITY` 覆盖。Starter 不附加或强制 exporter。M8.4 已用测试内存 SpanExporter 验证真实 HTTP、MySQL、RocketMQ 和消费者 spans 的 parent 关系；基础配置关闭 OTLP export，可选 M8.5 `observability` profile 启用 exporter 并将 trace 发送到本地 Tempo。订单响应仅含订单和事件 ID，不含 trace ID。Grafana 看板顶部的 `Trace ID` 输入框支持粘贴 `target/evidence/m8.5-platform-*/acceptance-summary.json` 中的 32 位十六进制 trace ID；也可由调用方在请求 `traceparent` 中指定 trace ID。第 14 面板显示对应 spans 和 parent 关系。匿名 Viewer 使用此看板入口；Explore 页面不属于匿名查看流程。完整启动说明见[本地可观测性指南](../observability/README.md)。

消费 loop 在事务代理的 handler 返回后记录 `processed` 或 `idempotent_skip`，再独立记录 ACK 结果。处理异常不 ACK；ACK 失败不会把已提交的业务处理记成回滚，Broker 重投后由消费身份表保持一次业务效果。缺少 Tracer/Propagator、设置 `reliable-event.tracing-enabled=false` 或 trace API 出错时仍执行原 handler/ACK 路径。只有标准 W3C `traceparent` 与可选 `tracestate` 参与提取，不传播 baggage。

## 故障与重复消息

在应用正常连接后，进入 `reliable-event-example` 目录，执行 `docker compose pause broker`，然后回到仓库根目录创建一个新订单。创建请求只写 MySQL，不等待 Broker。使用上面的 Outbox 查询观察该事件进入 `RETRY_WAIT` 且尝试次数增加。执行 `docker compose unpause broker`，随后运行 `wait-order.ps1`；退避到期后事件会自动发送，消费者最终处理一次。若故障演示中断，先执行 `docker compose unpause broker` 再关闭服务。

重复消息的确定性验证命令：

```powershell
mvn -pl reliable-event-example -am '-Dtest=OrderExampleMysqlIntegrationTest,ExampleApplicationEndToEndTest' '-Dsurefire.failIfNoSpecifiedTests=false' test
```

MySQL 测试用同一事件身份的两次处理调用证明业务效果只提交一次，并验证业务更新失败时去重记录随事务回滚。M8.4 端到端测试通过真实 HTTP、MySQL 和 RocketMQ 自动发布消费链路，用 Spring Boot SDK 内存 SpanExporter 验证 HTTP 上游、登记、发送尝试与 broker 实际消息属性中的父子关系；另一个消费环用例验证处理异常不 ACK、重复处理只提交一次和 ACK 失败的分离语义。已有端到端测试还覆盖发送成功后丢弃回执触发重试，验证两条消息有不同的 Broker Message ID、相同的可靠事件身份，消费结果仍只有一次。M6.4 测试还会启动真实 MySQL 与 RocketMQ：制造 `DEAD`、让两个操作者竞争一次重放、以正确映射重启服务后核对 Starter 自动发送及审计；另在真实 Broker 收到消息但回执丢失后重放，核对稳定身份和消费者的一次业务效果。端到端测试使用固定的本机 8081 端口，运行前先停止示例 Compose 中的 Broker，避免端口冲突。

## 人工处理 DEAD 事件

示例只依赖 Starter，并包含 Java 调用示例 [`DeadEventManualProcedure`](src/main/java/dev/reliableevent/example/DeadEventManualProcedure.java)。要启用其 `DeadEventOperations` 依赖，设置 `reliable-event.dead-operations-enabled=true`；默认关闭，不影响订单发布。启用重放前，在示例 MySQL 库执行一次[重放审计表 SQL](../reliable-event-jdbc/src/main/resources/schema/reliable-event-replay-audit-m6-2.sql)。示例不提供管理 HTTP 端点，实际接入时由应用将此 Java 服务接到已有的受限运维流程，并鉴权。

一次处置按以下顺序进行：

1. 获授权的操作者调用 `list(50)` 定位事件，再调用 `inspect(new EventId(id))` 获取 `DeadEventLookup.Dead` 详情和当前版本。详情中的业务键、失败摘要只在受限界面展示，不写普通日志。
2. 用事件 ID、业务键和时间核对 Broker 消息、消费者去重记录与业务效果。尤其遇到发送结果未知、超时或状态更新失败时，Broker 可能已经接收；重放可能再次投递，消费者必须保持幂等。
3. 修复原失败原因，例如 Topic、Proxy、网络或权限。若消费者已完成业务效果，先判断是否需要重放；不能仅凭生产端 `DEAD` 判定消息丢失。
4. 再次查询当前 `DEAD` 详情，以其版本调用 `replayAfterVerification(details, authenticatedOperator, reason)`。`authenticatedOperator` 取自真实登录身份，`reason` 写明核对事实和修复内容，不能使用固定占位值。`Replayed` 给出审计 ID；`NotFound`、`NotDead` 或 `VersionMismatch` 时重新查询，不自动重试重放。
5. 用[运维指南中的审计 SQL](../docs/OPERATIONS.md#状态与只读排查)和 Outbox 状态观察该 ID 后续进入 `PENDING`、`PUBLISHING`、`PUBLISHED`、`RETRY_WAIT` 或再次 `DEAD`；`PUBLISHED` 后仍须核对消费者业务效果。审计表和详情均限制访问与保留期限。

可用上面的端到端测试命令复现 M6.4 的两条受控重放路径。目标映射修复路径在重放前关闭扫描，重放后以正确映射重启应用，由 Starter 自动发布；未知结果路径为了确定性使用 JDBC 发布 Worker 显式推进。操作记录和未覆盖边界见[M6 完成记录](../docs/progress/M6_COMPLETED.md)。

## 配置与清理

新建示例库时，正式 Outbox SQL 同时创建 `reliable_event_identity`。M7 端到端测试还会以 1 秒测试保留期自动清理已发布行，核对身份仍在、再次登记返回原 ID、Broker 无新消息且消费者业务效果仍为一次。实际应用的清理默认关闭；开启前必须按[运维指南](../docs/OPERATIONS.md#建表和迁移)完成旧数据回填及全实例写入协议切换。示例默认 JDBC URL 使用 `connectionTimeZone=UTC`。

可用 `EXAMPLE_JDBC_URL`、`EXAMPLE_DB_USER`、`EXAMPLE_DB_PASSWORD`、`EXAMPLE_ROCKETMQ_ENDPOINT`、`EXAMPLE_ROCKETMQ_TOPIC`、`EXAMPLE_ROCKETMQ_GROUP`、`EXAMPLE_HTTP_PORT` 覆盖示例配置。Topic 或消费者组改名时需自行创建对应 RocketMQ 资源；`bootstrap.ps1` 初始化的是默认名称。真实凭据不要提交到仓库。

若 HTTP 8090 端口被占用，可在启动应用前设置 `$env:EXAMPLE_HTTP_PORT='18090'`，并在请求 URL 与 `wait-order.ps1 -BaseUrl http://localhost:18090` 中使用相同端口。若容器端口冲突，先用 `docker compose ps` 检查，再修改 `compose.yaml` 的主机端口和相应环境变量。Proxy 测试配置把 Broker 地址发布为 `127.0.0.1`，因此默认固定映射为 8081；更改 Proxy 端口需要同步检查路由可达性。

停止应用后，在 `reliable-event-example` 目录执行 `docker compose down` 可停止示例容器并保留 MySQL 数据卷。只有确认不再需要示例数据时才运行 `docker compose down -v`；后者会删除该 Compose 项目的 MySQL 数据卷。
