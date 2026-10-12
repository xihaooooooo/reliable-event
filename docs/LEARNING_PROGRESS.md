# ReliableEvent 学习进度

最后更新：2026-10-12（Asia/Shanghai）

## 教学约定

- 基础薄弱，按主体源码逐类教学，每次讲一个类、接口、枚举或 record，类内附属类型一起讲。
- 2026-10-12 起减少讲解细节，重点说明类型职责、主要方法的工作过程及与其他类型的配合；不逐字段、逐行解释语法，难懂的新术语用简单数据说明。
- 每课在聊天中附上当前类型的可点击源码链接。
- 不讲 example、kafka-example 的示例业务类，基准测试模块也不作为主体学习对象。
- 不恢复旧的订单流程分步教学，不突然展开复杂设计取舍。
- “已讲”不代表掌握；有疑问时回看相应类型。
- 不修改业务代码，不创建提交，不引入 skill 或多代理。
- 本文件只记录进度，不保存详细源码讲解、示例、逐项知识点或源码链接。每讲完一个类型或评估自测后，更新当前位置、下一步和日期。

## 已讲类型

core 的 16 个主体类型已介绍：

1. ReliableEvent
2. EventId
3. ReliableEventPublisher
4. MissingActiveTransactionException
5. ReliableEventSerializationException
6. StoredEvent
7. EventSender
8. SendReceipt
9. EventSendFailureType
10. EventSendException
11. EventHeaderConstraints
12. OutboundEvent
13. EventTransport
14. TransportReceipt
15. TransportFailureType
16. TransportException

jdbc 已介绍 17 个主体类型：

17. JdbcReliableEventPublisher：2026-10-11 已讲；2026-10-12 已补齐当前版本新增字段和事务检查。
18. RegistrationTracer：2026-10-12 已讲，包含嵌套 Registration 接口。
19. JdbcOutboxRepository：2026-10-12 按简化方式介绍职责、主要方法及类型配合。
20. EventStatus：2026-10-12 已讲。
21. EventCandidate：2026-10-12 已讲。
22. ClaimedEvent：2026-10-12 已讲。
23. ExpiredLeaseCandidate：2026-10-12 已讲。
24. OutboxCounts：2026-10-12 已讲。
25. OutboxMetricsSnapshot：2026-10-12 已讲。
26. StaleEventClaimException：2026-10-12 已讲。
27. JdbcOutboxSql：2026-10-12 已讲。
28. OutboundEventMapper：2026-10-12 已讲。
29. JdbcEventPublicationWorker：2026-10-12 已讲。
30. ExponentialBackoff：2026-10-12 已讲。
31. EventFailureSummary：2026-10-12 已讲。
32. PublicationObserver：2026-10-12 已讲。
33. PublicationTracer：2026-10-12 已讲，包含嵌套 Attempt 接口。

合计已介绍 33 个主体类型，掌握情况待评估。

## 当前停在哪里

PublicationTracer（含 Attempt）已按简化方式介绍；已讲不代表掌握。

## 下一步

下一新课为 jdbc 的 internal.recovery.JdbcExpiredLeaseRecovery，继续采用简化讲解方式并附源码链接。

后续模块顺序：继续 jdbc，再学习 rocketmq、kafka、Spring Boot 自动配置及对应传输自动配置模块；结合类型补充相关 SQL、配置、测试和构建依赖。

## 自测状态

- 2026-10-09 已提供 12 道自测题，范围为当时已讲的 11 个 core 类型。
- 尚未收到答案，未评估掌握情况，也未判定需要回看的类型。
- 用户提交答案时，按原题号反馈、解释并记录需要回看的类型。
- 不强制先完成自测才能继续新课；用户要求复习时直接回顾，不出题。
