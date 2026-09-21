# ReliableEvent 当前交接

## 当前进度

M0 和 M1 已完成。项目已经具备最小发布闭环：

```text
事务内 publish → PENDING → 扫描到期事件 → Fake 发送 → PUBLISHED
```

已经实现事务校验、JSON 序列化、重复事件键幂等登记和未来事件过滤。

## 验证方式

启动 Docker，使用 Java 17 执行：

```bash
mvn verify
```

当前共有 5 个 MySQL 5.7 集成测试，全部通过。

## 当前边界

- 发送端仍是测试用 Fake 适配，没有接入 RocketMQ；
- 只验证单实例发布；
- 尚未实现失败重试、死信、租约和宕机恢复；
- Spring Boot 自动配置和指标尚未开始。

## 下一步

进入 M2，依次实现：

1. 基于版本号条件更新的多实例安全抢占；
2. 双实例并发竞争测试；
3. 带随机抖动的指数退避；
4. 最大尝试次数和 `DEAD` 状态。

实现时继续使用 MySQL 5.7 Testcontainers，不依赖 `SKIP LOCKED`，也不要提前加入 M3 的租约恢复。
