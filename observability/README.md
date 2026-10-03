# ReliableEvent local observability stack

This is an optional M8.5 local stack for the existing original order example. Its Compose project has its own Docker network and service volumes. Prometheus reaches the host-run example at `host.docker.internal:8090`; the application continues to use the example Compose services on host ports 3307 (MySQL) and 8081 (RocketMQ Proxy). The stack does not copy or replace the order application, MySQL, RocketMQ, or the reliable publishing protocol.

Compose-published ports bind to loopback: Prometheus `9090`, Grafana `3000`, Tempo query API `3200`, Tempo OTLP gRPC/HTTP `4317`/`4318`, and Alertmanager `9093`. The Alertmanager evidence receiver runs as a hidden PowerShell 7 (`pwsh`) listener on host port `19080` so the Docker bridge can reach it; it accepts only loopback and the dynamically discovered observability Compose subnet. Port `9080` is used by a Windows audio service on the reference host, so the demo deliberately uses `19080`. The receiver writes validated, compact webhook records to a session-specific `target/evidence/m8.5-alert-receiver-<timestamp>.jsonl` file and serves the current session at `http://localhost:19080/alerts`. Alertmanager sends only to this local receiver. There are no external notification routes or credentials. Grafana anonymous access is Viewer-only and is suitable only for this loopback demo.

## Start and verify

Use PowerShell 7 (`pwsh`) and Docker Desktop with Linux containers. Bootstrap the already-existing example dependencies and schema once, then start the optional stack and example profile:

```powershell
./reliable-event-example/scripts/bootstrap.ps1
./observability/scripts/start.ps1 -StartExample
./observability/scripts/validate-rules.ps1
./observability/scripts/accept-platform.ps1
```

The startup script starts the observability Compose project with `--pull missing`: it reuses cached pinned image tags and downloads only tags that are absent. It then builds the example with Java 17 and the Maven `observability` profile, and starts the packaged example JVM in a hidden process. It records process IDs and separate stdout/stderr logs under `target/evidence/`. The profile adds only the example's Prometheus registry and OTLP exporter. Base `application.yml` explicitly disables OTLP export; the optional `application-observability.yml` exposes `/actuator/prometheus`, names the service `reliable-event-example`, and enables the Boot-managed OTLP exporter at `http://localhost:4318/v1/traces` with the existing 100% demo sampling default. The Starter itself does not gain a Prometheus or OpenTelemetry SDK dependency.

Open Grafana at `http://localhost:3000/d/reliable-event-ops/reliable-event-operations`. The 14-panel dashboard queries Prometheus and Tempo. The order response contains order and event IDs, not a trace ID. Paste a 32-character hexadecimal trace ID from `target/evidence/m8.5-platform-*/acceptance-summary.json` into the dashboard’s `Trace ID` textbox. A caller can also set the trace ID in the request’s W3C `traceparent` header and use that same ID. Panel 14 renders the matching Tempo trace, including spans and parent relationships. The platform acceptance script makes an order request and verifies the same trace through Grafana’s Tempo data-source proxy. Anonymous access is Viewer-only, and the anonymous Viewer cannot use the Explore page; the dashboard textbox is the supported trace lookup path. The runbook is panel 11. Prometheus and Alertmanager are available at `http://localhost:9090` and `http://localhost:9093`.

`validate-rules.ps1` runs the pinned Prometheus `promtool` against Prometheus config, alert rules, and rule unit samples; each command has its own raw log/exit-code record. `accept-platform.ps1` saves the actuator health and full scrape, order request/result, Tempo trace and parent IDs, Grafana dashboard and data-source query responses, each dashboard panel query, and firing/resolved Alertmanager webhook records in a timestamped directory below `target/evidence/`.

Stop the services and optional sample JVM with:

```powershell
./observability/scripts/stop.ps1 -StopExample
```

This stops only the observability Compose project and processes recorded by its startup script. Named volumes and saved evidence remain. To delete the Prometheus, Grafana, Alertmanager, and Tempo data volumes, first verify the current Compose project and then explicitly run `docker compose -f observability/docker-compose.yml down -v` from the repository root. Do not stop or remove the separate example MySQL/RocketMQ Compose project as part of this cleanup. Stop it separately from `reliable-event-example` only when its data is no longer needed.

## Targets and alert expectations

The default target labels declare `outbox_store=reliable-event-example`, `expected_scheduler=true`, `scheduler_mode=automatic`, `expected_snapshot=true`, and `snapshot_mode=automatic`. Keep the same `outbox_store` value on every scrape target that reads the same logical Outbox database. Do not collapse `instance` labels: Prometheus derives each from the distinct target address. For two host instances on different ports, add both addresses as targets and preserve the logical store label. Snapshot gauges represent the shared database and are filtered for freshness per instance, then grouped with `max`; never sum duplicate snapshots. Per-instance counters are converted to rate/increase before summing.

The scheduler alert expects an automatic scheduler only when `expected_scheduler=true` and `scheduler_mode=automatic`. Set `expected_scheduler=false` for intentionally disabled scheduling or a truly manual-only run. For manual snapshot refreshes, set `expected_snapshot=true` and `snapshot_mode=manual`; the rule uses its explicit 120-second freshness constant. Change that rule if the intended operator refresh cadence changes. An automatic scheduler with periodic sampling disabled can still refresh at automatic-cycle boundaries, so keep `snapshot_mode=automatic` and `expected_snapshot=true`; `periodic_enabled=0` does not silence freshness monitoring. Use `expected_snapshot=false` only if the deployment has no refresh path whose freshness should be monitored.

The alert rules use demonstration thresholds: an unfinished event older than 60 seconds for five minutes; automatic snapshot freshness of 60 seconds; manual snapshot freshness of 120 seconds; scheduler heartbeat age of 60 seconds with a three-minute firing grace; and a minimum of five sender attempts before a 25% failure-ratio alert. They are not production SLAs. Set thresholds against the retry schedule, actual polling and refresh intervals, database/application clock skew, and business deadlines.

The overview uses the actual Micrometer Prometheus scrape names: gauges such as `reliable_event_unfinished_overdue` and `reliable_event_snapshot_last_success_timestamp`, counters such as `reliable_event_publish_success_total` and `reliable_event_publication_persisted_total`, and timer series such as `reliable_event_publish_duration_seconds_count`. Never add EventId, TraceId, business keys, or error text as metric labels.

## Pinned images and official references

The Compose file pins Prometheus `v3.14.0`, Alertmanager `v0.34.1`, Grafana `12.4.12`, and Tempo `2.10.8`. These versions were checked against the official project release pages on 2026-10-02. The Tempo receiver explicitly binds OTLP to `0.0.0.0` for Docker; its HTTP query API stays on the loopback-published host port.

- [Prometheus official Docker installation](https://prometheus.io/docs/prometheus/latest/installation/) and [3.14.0 release](https://github.com/prometheus/prometheus/releases/tag/v3.14.0)
- [Alertmanager 0.34.1 release](https://github.com/prometheus/alertmanager/releases/tag/v0.34.1)
- [Grafana official Docker installation](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/) and [12.4.12 release](https://github.com/grafana/grafana/releases/tag/v12.4.12)
- [Tempo OTLP receiver setup](https://grafana.com/docs/tempo/latest/set-up-for-tracing/instrument-send/set-up-collector/otel-collector/) and [2.10.8 release](https://github.com/grafana/tempo/releases/tag/v2.10.8)
- [Spring Boot 3.5 tracing](https://docs.spring.io/spring-boot/3.5/reference/actuator/tracing.html) and [OTLP exporter properties](https://docs.spring.io/spring-boot/3.5/appendix/application-properties/index.html)
- [Prometheus rule tests](https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/) and [promtool](https://prometheus.io/docs/prometheus/latest/command-line/promtool/)

M8.5 platform acceptance is complete; its rule, Java, platform API, and Viewer dashboard evidence is indexed in [the completion record](../docs/progress/M8_5_COMPLETED.md). It does not perform M8.6 broker-failure, database-failure, or performance exercises.
