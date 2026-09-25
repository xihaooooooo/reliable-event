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

## 故障与重复消息

在应用正常连接后，进入 `reliable-event-example` 目录，执行 `docker compose pause broker`，然后回到仓库根目录创建一个新订单。创建请求只写 MySQL，不等待 Broker。使用上面的 Outbox 查询观察该事件进入 `RETRY_WAIT` 且尝试次数增加。执行 `docker compose unpause broker`，随后运行 `wait-order.ps1`；退避到期后事件会自动发送，消费者最终处理一次。若故障演示中断，先执行 `docker compose unpause broker` 再关闭服务。

重复消息的确定性验证命令：

```powershell
mvn -pl reliable-event-example -am '-Dtest=OrderExampleMysqlIntegrationTest,ExampleApplicationEndToEndTest' '-Dsurefire.failIfNoSpecifiedTests=false' test
```

MySQL 测试用同一事件身份的两次处理调用证明业务效果只提交一次，并验证业务更新失败时去重记录随事务回滚。端到端测试先通过 HTTP 创建订单，验证自动发送和消费；再在首次真实 Broker 发送成功后丢弃回执，触发重试，验证两条消息有不同的 Broker Message ID、相同的可靠事件身份，消费结果仍只有一次。端到端测试使用固定的本机 8081 端口，运行前先停止示例 Compose 中的 Broker，避免端口冲突。`DEAD` 事件可用上述只读 SQL 查看；示例没有人工重放接口。

## 配置与清理

可用 `EXAMPLE_JDBC_URL`、`EXAMPLE_DB_USER`、`EXAMPLE_DB_PASSWORD`、`EXAMPLE_ROCKETMQ_ENDPOINT`、`EXAMPLE_ROCKETMQ_TOPIC`、`EXAMPLE_ROCKETMQ_GROUP`、`EXAMPLE_HTTP_PORT` 覆盖示例配置。Topic 或消费者组改名时需自行创建对应 RocketMQ 资源；`bootstrap.ps1` 初始化的是默认名称。真实凭据不要提交到仓库。

若 HTTP 8090 端口被占用，可在启动应用前设置 `$env:EXAMPLE_HTTP_PORT='18090'`，并在请求 URL 与 `wait-order.ps1 -BaseUrl http://localhost:18090` 中使用相同端口。若容器端口冲突，先用 `docker compose ps` 检查，再修改 `compose.yaml` 的主机端口和相应环境变量。Proxy 测试配置把 Broker 地址发布为 `127.0.0.1`，因此默认固定映射为 8081；更改 Proxy 端口需要同步检查路由可达性。

停止应用后，在 `reliable-event-example` 目录执行 `docker compose down` 可停止示例容器并保留 MySQL 数据卷。只有确认不再需要示例数据时才运行 `docker compose down -v`；后者会删除该 Compose 项目的 MySQL 数据卷。
