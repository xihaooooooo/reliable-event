# M8.5 完成记录

完成日期：2026-10-03（Asia/Shanghai）

状态：M8.1–M8.5 已完成；M8.6 尚未开始。

M8.5 交付了独立可选的本地可观测性栈、Prometheus 规则、Alertmanager 本地接收路由、Tempo OTLP 接收、Grafana provisioning、运行/验证脚本和操作说明。Grafana 看板现有 14 个面板：顶部 Trace ID 输入框供匿名 Viewer 按 trace ID 查询，第 14 面板展示 Tempo trace；Explore 不属于匿名查看流程。订单响应不包含 trace ID；验收 trace ID 保存在验收摘要中，调用方也可通过请求 `traceparent` 指定。第 14 面板位于 y=29，与位于 y=24 的第 12、13 面板分行显示。

Prometheus v3.14.0 官方镜像中的 `promtool` 实际执行 config check、rules check 和 rules test，三条命令均退出 0。规则文件包含 16 项（4 条 recording rule、12 条 alert），规则测试覆盖 20 个场景。成功的逐命令日志与退出码见[规则验收汇总](../../target/evidence/m8.5-promtool-20261002-233836-5446765e/all-commands.log)；早期失败尝试保留在[首次规则验收日志](../../target/evidence/m8.5-promtool-20261002-233537-172df66a/all-commands.log)，没有覆盖失败证据。

Java 17 全仓 `mvn -Dmaven.repo.local=C:\Users\20659\.m2\repository -Pobservability clean verify` 通过：8 个模块、34 份 Surefire XML、204 项测试，0 失败、0 错误、0 跳过。原始日志见[全仓 clean verify](../../target/evidence/m8.5-full-clean-verify-20261002-232910.log)，退出码为 0。

此前完整平台验收为[验收摘要及 API 证据目录](../../target/evidence/m8.5-platform-20261003-005306/acceptance-summary.json)，其[原始日志](../../target/evidence/m8.5-platform-final-accept-v2-20261003-005306.log)退出码为 0。它记录了真实订单 `orderId=4`、`eventId=4`，Tempo/Grafana 代理返回 `http post /orders`、`reliable-event.register`、`reliable-event.publish`、`example.order.consume` 四个 spans，Trace ID 为 `3d24b113f81b4845bd34f7ef07ef0ae3`，parent span 为 `198c43710c374a17`；Grafana 返回 16 个面板查询结果并有实时样本，Alertmanager 的同一验收告警完成 firing 与 resolved receiver 往返。Root 另以匿名 Viewer 在浏览器验证 Trace ID 输入框 `52cf356aef034d62baa990a39a38a82a` 渲染出四个 spans。平台验收目录保留 health、metrics、dashboard/datasource/query、trace 和 receiver 原始响应。

2026-10-03 10:54（Asia/Shanghai）发现宿主 JVM 与本地 Alertmanager receiver 已退出，Prometheus `up{job="reliable-event-example"}` 为 0。按现有脚本仅重启 observability Compose 项目、记录的示例 JVM 与 receiver；Compose stop/start 退出码均为 0，Maven 使用 `-Pobservability -DskipTests package` 成功，命名数据卷保留，原 MySQL/RocketMQ 服务未停止。当前最新[平台验收摘要及 API 证据目录](../../target/evidence/m8.5-platform-20261003-105535/acceptance-summary.json)记录订单 `orderId=5`、`eventId=5`、Trace ID `abb0bed92bc4401da626ceba4dbf0f89`、四个 HTTP/登记/发布/消费 spans、16 个面板查询（含实时样本）及 receiver firing/resolved；[平台验收原始日志](../../target/evidence/m8.5-platform-recovery-accept-20261003-105535.log)和[退出码](../../target/evidence/m8.5-platform-recovery-accept-20261003-105535.log.exit-code.txt)均记录成功。stop/start 与示例打包日志、退出码保存在 `target/evidence/m8.5-session-recovery-*` 和 `target/evidence/m8.5-example-package-20261003-105417.log*`。Grafana API 同时确认第 10 面板 ALERTS 查询 `instant=true`，第 12/13 面板 y=24、第 14 面板 y=29。

Grafana 固定镜像为官方 `grafana/grafana:12.4.12`，经官方 Registry manifest 链及 config、13 个压缩 layer digest、gzip CRC 和 RootFS `diff_id` 核验后载入。官方 config digest `sha256:b54753e9dc7100e9a507f70535c5a1ba68f8e29b8b3bc04bd4cb273cbe6dc267` 与本机 Docker image ID `sha256:c4688b2fa2b7d58eb2749dedef38a1711ed4c70cbda823ccdf1dfbe72bce1aab` 不同；镜像平台均为 `linux/amd64`，Docker inspect 的 13 个 RootFS layer diff_id 与官方 config 完全一致。该差异与核验依据如实记录于[官方层校验日志](../../target/evidence/m8.5-image-grafana-verify-archive-20261003-003504-879.log)、[镜像 inspect](../../target/evidence/m8.5-image-grafana-rootfs-inspect-20261003-003906-884.log)及[Docker load 日志](../../target/evidence/m8.5-image-grafana-docker-load-20261003-003545-260.log)。此前镜像下载失败及恢复过程的日志均保留在 `target/evidence/`。

M8.5 规则测试与本地平台演示不替代 M8.6 的真实 Broker/数据库故障演练、开销检查或生产阈值确定；M8.6 仍待计划和实施。
