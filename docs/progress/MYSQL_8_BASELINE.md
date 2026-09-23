# MySQL 8.0 基线更正记录

> 日期：2026-09-23

## 更正内容

ReliableEvent `0.1.0` 的数据库支持基线由 MySQL 5.7 调整为 MySQL 8.0，并将自动化测试服务端固定为：

```text
mysql:8.0.36
```

Maven 当前使用 MySQL Connector/J `8.0.33`。服务端版本和 JDBC 驱动版本分别固定，不要求两者补丁号相同。

## 并发策略

MySQL 8.0 已支持 `SKIP LOCKED`，但本次基线调整不同时替换现有并发协议。项目继续使用：

```text
查询候选 id + version
        ↓
按 id + status + version 条件更新
        ↓
受影响行数为 1 才获得执行权
```

原因是该方案已经覆盖候选抢占、租约所有权、状态完成和过期恢复的版本栅栏，并通过真实双 Worker 测试。未来只有在相同环境下完成吞吐、锁等待、事务边界和故障语义对比后，才评估增加 `SKIP LOCKED` 替代策略。

## 兼容性结果

现有表结构和 SQL 可以直接运行在 MySQL 8.0.36，无需数据库迁移脚本调整。重点验证范围包括：

- JSON Payload 和 Headers；
- `DATETIME(3)` 精度；
- `UTC_TIMESTAMP(3)` 数据库时间；
- `TIMESTAMPADD` 租约和退避计算；
- 版本号条件抢占；
- 双 Worker 并发竞争；
- 租约 Owner 和有效期栅栏；
- 过期租约条件恢复；
- 重试、死信和幂等登记。

## 验证结果

使用 `mysql:8.0.36` 执行：

```bash
mvn clean verify
```

结果：

```text
Tests run: 46
Failures: 0
Errors: 0
Skipped: 0
BUILD SUCCESS
```

当前执行环境使用 JDK 21，Maven 编译参数为 `release 17`；项目目标版本仍为 Java 17。
