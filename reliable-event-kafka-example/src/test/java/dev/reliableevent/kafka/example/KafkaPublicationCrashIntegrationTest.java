package dev.reliableevent.kafka.example;

import dev.reliableevent.spi.TransportReceipt;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.persistence.JdbcPublicationStore;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import dev.reliableevent.jdbc.internal.persistence.StaleEventClaimException;
import dev.reliableevent.autoconfigure.ReliableEventScheduler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import io.micrometer.core.instrument.MeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@Timeout(180)
class KafkaPublicationCrashIntegrationTest {
    private static final Duration LEASE = Duration.ofSeconds(12);
    private static final Duration SIGNAL_TIMEOUT = Duration.ofSeconds(45);
    @Container static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("reliable_event_kafka_crash").withUsername("test").withPassword("test");
    @Container static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"))
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private ConfigurableApplicationContext context;
    private JdbcTemplate jdbc;
    private DriverManagerDataSource evidenceDataSource;
    private String topic;
    private String group;
    private ExampleConsumerLoop consumer;
    private Path evidenceDirectory;

    @BeforeEach void setUp() throws Exception {
        topic = "reliable-event-crash-" + UUID.randomUUID().toString().replace("-", "");
        group = "kafka-crash-consumer-" + UUID.randomUUID();
        evidenceDataSource = new DriverManagerDataSource(
                mysqlJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
        new ResourceDatabasePopulator(new ClassPathResource("schema/reliable-event-outbox.sql"),
                new ClassPathResource("schema/example-tables.sql")).execute(evidenceDataSource);
        jdbc = new JdbcTemplate(evidenceDataSource);
        jdbc.update("DELETE FROM example_order_effect");
        jdbc.update("DELETE FROM example_consumed_event");
        jdbc.update("DELETE FROM reliable_event_outbox");
        jdbc.update("DELETE FROM reliable_event_identity");
        jdbc.update("DELETE FROM example_order");
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(20, TimeUnit.SECONDS);
        }
        context = new SpringApplicationBuilder(KafkaExampleApplication.class).run("--server.port=0",
                "--spring.datasource.url=" + mysqlJdbcUrl(),
                "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(),
                "--example.kafka.topic=" + topic,
                "--example.kafka.consumer-group=" + group,
                "--example.consumer.enabled=false",
                "--reliable-event.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--reliable-event.scheduling-enabled=false");
        consumer = new ExampleConsumerLoop(consumerConfiguration(group), topic, Duration.ofMillis(100),
                context.getBean(OrderMessageHandler.class), context.getBean(io.micrometer.tracing.Tracer.class),
                context.getBean(io.micrometer.tracing.propagation.Propagator.class));
        consumer.start();
    }

    @AfterEach void tearDown() {
        if (consumer != null) consumer.stop();
        if (context != null) context.close();
    }

    @Test void killedAfterRealClaimBeforeSendRecoversAfterNaturalLeaseExpiry() throws Exception {
        evidenceDirectory = evidenceDirectory("claim-before-send");
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("book", 2);
        String workerId = "kafka-before-send-" + UUID.randomUUID();
        try (KafkaCrashProcessFixture child = startChild(KafkaCrashMode.AFTER_CLAIM_BEFORE_SEND, workerId)) {
            var ready = child.awaitReady(KafkaCrashMode.AFTER_CLAIM_BEFORE_SEND, workerId, SIGNAL_TIMEOUT);
            assertThat(ready.eventId()).isEqualTo(created.eventId());
            assertClaimed(created.eventId(), workerId);
            timeline("claim_verified event_id=" + created.eventId() + " worker=" + workerId
                    + " status=" + status(created.eventId()) + " attempt_count=" + attemptCount(created.eventId())
                    + " lease_until=" + leaseUntil(created.eventId()));
            var noWireRecords = readWireRecords(Duration.ofMillis(500));
            timeline("broker_before_send record_count=" + noWireRecords.size());
            assertThat(noWireRecords).isEmpty();
            assertThat(child.destroyForcibly(Duration.ofSeconds(10))).isNotZero();
        }

        expireAndRecover(created.eventId());
        assertThat(publishRecoveredInNewJvm("kafka-recovery-before-send")).isOne();
        await(() -> status(created.eventId()) == EventStatus.PUBLISHED.code()
                && committedOffset(group) >= 1 && effectCount(created.orderId()) == 1, Duration.ofSeconds(30));
        var recoveredRecords = readWireRecords(Duration.ofSeconds(3));
        timeline("recovered broker_records=" + describeRecords(recoveredRecords) + " status=" + status(created.eventId())
                + " group_offset=" + committedOffset(group) + " effect_count=" + effectCount(created.orderId()));
        assertThat(recoveredRecords).hasSize(1);
        assertBusinessDeduplication(created.orderId());
    }

    @Test void killedAfterBrokerConfirmationBeforeStateCommitRecoversAndRedeliversSameIdentity() throws Exception {
        evidenceDirectory = evidenceDirectory("broker-confirmed-before-state");
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("cup", 4);
        String workerId = "kafka-after-confirm-" + UUID.randomUUID();
        try (KafkaCrashProcessFixture child = startChild(
                KafkaCrashMode.AFTER_BROKER_CONFIRMATION_BEFORE_STATE_COMMIT, workerId)) {
            var ready = child.awaitReady(KafkaCrashMode.AFTER_BROKER_CONFIRMATION_BEFORE_STATE_COMMIT,
                    workerId, SIGNAL_TIMEOUT);
            assertThat(ready.eventId()).isEqualTo(created.eventId());
            assertClaimed(created.eventId(), workerId);
            await(() -> !readWireRecords(Duration.ofMillis(200)).isEmpty(), Duration.ofSeconds(15));
            var confirmedRecords = readWireRecords(Duration.ofSeconds(1));
            timeline("broker_confirmed_before_kill event_id=" + created.eventId() + " records="
                    + describeRecords(confirmedRecords) + " db_status=" + status(created.eventId()));
            assertThat(child.destroyForcibly(Duration.ofSeconds(10))).isNotZero();
        }

        expireAndRecover(created.eventId());
        assertThat(publishRecoveredInNewJvm("kafka-recovery-after-confirm")).isOne();
        await(() -> status(created.eventId()) == EventStatus.PUBLISHED.code()
                && committedOffset(group) >= 2 && effectCount(created.orderId()) == 1, Duration.ofSeconds(30));
        var duplicateRecords = readWireRecords(Duration.ofSeconds(3));
        timeline("recovered_duplicate records=" + describeRecords(duplicateRecords) + " status="
                + status(created.eventId()) + " group_offset=" + committedOffset(group)
                + " dedupe_rows=" + jdbc.queryForObject("SELECT COUNT(*) FROM example_consumed_event", Long.class)
                + " effect_count=" + effectCount(created.orderId()));
        assertThat(duplicateRecords).hasSize(2);
        assertThat(jdbc.queryForList("SELECT event_id FROM example_consumed_event", String.class))
                .containsExactly(Long.toString(created.eventId()));
        assertBusinessDeduplication(created.orderId());
    }

    @Test void futureEventStaysUnsentUntilItsAvailabilityTime() throws Exception {
        evidenceDirectory = evidenceDirectory("future-event");
        FutureOrder future = registerFutureOrder(Duration.ofSeconds(2));
        timeline("future_registered event_id=" + future.eventId().value() + " available_at=" + future.availableAt());
        assertThat(publishPending(worker(context.getBean(EventTransport.class)))).isZero();
        assertThat(status(future.eventId().value())).isEqualTo(EventStatus.PENDING.code());
        assertThat(readWireRecords(Duration.ofMillis(500))).isEmpty();

        await(() -> jdbc.queryForObject("SELECT next_attempt_at <= ? FROM reliable_event_outbox WHERE id=?",
                Boolean.class, Timestamp.from(Clock.systemUTC().instant()), future.eventId().value()), Duration.ofSeconds(5));
        timeline("future_became_due observed_at=" + Clock.systemUTC().instant());
        assertThat(publishPending(worker(context.getBean(EventTransport.class)))).isEqualTo(1);
        await(() -> status(future.eventId().value()) == EventStatus.PUBLISHED.code()
                && committedOffset(group) >= 1 && effectCount(future.orderId()) == 1, Duration.ofSeconds(30));
        var records = readWireRecords(Duration.ofSeconds(2));
        timeline("future_published records=" + describeRecords(records) + " status=" + status(future.eventId().value())
                + " group_offset=" + committedOffset(group) + " effect_count=" + effectCount(future.orderId()));
        assertThat(records).hasSize(1);
        assertBusinessDeduplication(future.orderId());
    }

    @Test void twoIndependentWorkerJvmsCompeteForOneSharedOutboxCandidate() throws Exception {
        evidenceDirectory = evidenceDirectory("dual-jvm-race");
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("lamp", 1);
        Path barrier = evidenceDirectory.resolve("release-workers.marker");
        String firstId = "kafka-race-a-" + UUID.randomUUID();
        String secondId = "kafka-race-b-" + UUID.randomUUID();
        try (KafkaWorkerProcessFixture first = startWorker(firstId, barrier);
             KafkaWorkerProcessFixture second = startWorker(secondId, barrier)) {
            first.awaitReady(Duration.ofSeconds(45));
            second.awaitReady(Duration.ofSeconds(45));
            Files.writeString(barrier, "go\n", StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE_NEW);
            timeline("race_released workers=" + firstId + "," + secondId + " event_id=" + created.eventId());
            int firstPublished = first.awaitResult(Duration.ofSeconds(60));
            int secondPublished = second.awaitResult(Duration.ofSeconds(60));
            assertThat(firstPublished + secondPublished).isOne();
        }
        await(() -> status(created.eventId()) == EventStatus.PUBLISHED.code()
                && committedOffset(group) >= 1 && effectCount(created.orderId()) == 1, Duration.ofSeconds(30));
        var records = readWireRecords(Duration.ofSeconds(2));
        timeline("race_result records=" + describeRecords(records) + " status=" + status(created.eventId())
                + " attempts=" + attemptCount(created.eventId()) + " group_offset=" + committedOffset(group)
                + " dedupe_rows=" + jdbc.queryForObject("SELECT COUNT(*) FROM example_consumed_event", Long.class)
                + " effect_count=" + effectCount(created.orderId()));
        assertThat(records).hasSize(1);
        assertBusinessDeduplication(created.orderId());
    }

    @Test void brokerAcknowledgementBeforeDatabaseStateFailureIsMeasuredAndRecovered() throws Exception {
        evidenceDirectory = evidenceDirectory("database-state-failure");
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("state-failure", 1);
        MeterRegistry meters = context.getBean(MeterRegistry.class);
        double sendsBefore = meters.get("reliable_event.publish.success").counter().count();
        double persistedBefore = meters.get("reliable_event.publication.persisted").counter().count();
        double failedBefore = meters.get("reliable_event.publication.state_update_failure").counter().count();
        String eventKey = jdbc.queryForObject("SELECT event_key FROM reliable_event_outbox WHERE id=?",
                String.class, created.eventId());
        String constraintName = "ck_m94_kafka_" + created.eventId();
        jdbc.execute("ALTER TABLE reliable_event_outbox ADD CONSTRAINT " + constraintName
                + " CHECK (event_type <> 'order-created' OR event_key <> '" + eventKey
                + "' OR status <> 2)");
        try {
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> publishPending(worker(context.getBean(EventTransport.class)))).isInstanceOf(RuntimeException.class);
        } finally {
            jdbc.execute("ALTER TABLE reliable_event_outbox DROP CHECK " + constraintName);
        }
        assertThat(status(created.eventId())).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(meters.get("reliable_event.publish.success").counter().count()).isEqualTo(sendsBefore + 1);
        assertThat(meters.get("reliable_event.publication.persisted").counter().count()).isEqualTo(persistedBefore);
        assertThat(meters.get("reliable_event.publication.state_update_failure").counter().count())
                .isEqualTo(failedBefore + 1);
        timeline("native_ack_then_db_state_failure event_id=" + created.eventId() + " status="
                + status(created.eventId()) + " send_success="
                + meters.get("reliable_event.publish.success").counter().count() + " persisted="
                + meters.get("reliable_event.publication.persisted").counter().count() + " state_failure="
                + meters.get("reliable_event.publication.state_update_failure").counter().count()
                + " injected_rejection=event-key-scoped-check-constraint"
                + " wire=" + describeRecords(readWireRecords(Duration.ofSeconds(2))));

        expireAndRecover(created.eventId());
        assertThat(publishPending(worker(context.getBean(EventTransport.class)))).isOne();
        await(() -> status(created.eventId()) == EventStatus.PUBLISHED.code()
                && committedOffset(group) >= 2 && effectCount(created.orderId()) == 1, Duration.ofSeconds(30));
        var records = readWireRecords(Duration.ofSeconds(2));
        timeline("native_db_state_recovered records=" + describeRecords(records) + " status="
                + status(created.eventId()) + " group_offset=" + committedOffset(group)
                + " effect_count=" + effectCount(created.orderId()));
        assertThat(records).hasSize(2);
        assertBusinessDeduplication(created.orderId());
    }

    @Test void nativeBrokerOutageConsumesConfiguredAttemptsAndMovesEventToDead() throws Exception {
        evidenceDirectory = evidenceDirectory("native-retry-exhaustion");
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("broker-outage", 1);
        var producer = context.getBean(org.apache.kafka.clients.producer.Producer.class);
        @SuppressWarnings("unchecked")
        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics =
                (Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>) producer.metrics();
        String clientId = metrics.keySet().stream().map(name -> name.tags().get("client-id"))
                .filter(Objects::nonNull).findFirst().orElse("unavailable");
        String resourceBefore = nativeProducerSample("before-broker-outage", producer, clientId);
        jdbc.update("UPDATE reliable_event_outbox SET max_attempts=2 WHERE id=?", created.eventId());
        double failuresBefore = context.getBean(MeterRegistry.class)
                .get("reliable_event.publish.failure").counter().count();
        boolean brokerPaused = false;
        try {
            KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
            brokerPaused = true;
            timeline("native_broker_paused event_id=" + created.eventId() + " max_attempts=2");
            assertThat(publishPending(worker(context.getBean(EventTransport.class)))).isZero();
            assertThat(status(created.eventId())).isEqualTo(EventStatus.RETRY_WAIT.code());
            await(() -> jdbc.queryForObject("SELECT next_attempt_at <= ? FROM reliable_event_outbox WHERE id=?",
                    Boolean.class, Timestamp.from(Clock.systemUTC().instant()), created.eventId()), Duration.ofSeconds(10));
            assertThat(publishPending(worker(context.getBean(EventTransport.class)))).isZero();
            assertThat(status(created.eventId())).isEqualTo(EventStatus.DEAD.code());
            assertThat(attemptCount(created.eventId())).isEqualTo(2);
            timeline("native_retry_exhausted event_id=" + created.eventId() + " status="
                    + status(created.eventId()) + " attempt_count=" + attemptCount(created.eventId())
                    + " send_failure_delta=" + (context.getBean(MeterRegistry.class)
                    .get("reliable_event.publish.failure").counter().count() - failuresBefore));
        } finally {
            if (brokerPaused) KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }
        var possibleLateRecords = readWireRecords(Duration.ofSeconds(3));
        String lastError = jdbc.queryForObject("SELECT last_error FROM reliable_event_outbox WHERE id=?",
                String.class, created.eventId());
        timeline("native_retry_final event_id=" + created.eventId() + " status=" + status(created.eventId())
                + " attempt_count=" + attemptCount(created.eventId()) + " last_error=" + lastError
                + " broker_records=" + describeRecords(possibleLateRecords));
        assertThat(possibleLateRecords).hasSizeLessThanOrEqualTo(2);
        assertThat(possibleLateRecords).allSatisfy(record -> assertThat(header(record, "reliable_event_id"))
                .isEqualTo(Long.toString(created.eventId())));
        if (!possibleLateRecords.isEmpty()) {
            await(() -> committedOffset(group) >= possibleLateRecords.size()
                    && effectCount(created.orderId()) == 1, Duration.ofSeconds(20));
            assertBusinessDeduplication(created.orderId());
        }
        Thread.sleep(500);
        assertThat(status(created.eventId())).isEqualTo(EventStatus.DEAD.code());
        String resourceAfter = nativeProducerSample("after-broker-recovery", producer, clientId);
        timeline(resourceBefore);
        timeline(resourceAfter);
        Files.writeString(evidenceDirectory.resolve("outage-producer-resources.log"),
                resourceBefore + System.lineSeparator() + resourceAfter + System.lineSeparator(), StandardCharsets.UTF_8);
    }

    @Test void nativeProducerMetricsAndOwnedThreadsAreSampledAcrossContextShutdown() throws Exception {
        evidenceDirectory = evidenceDirectory("native-producer-resources");
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("resource-sample", 1);
        var producer = context.getBean(org.apache.kafka.clients.producer.Producer.class);
        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> nativeMetrics = producer.metrics();
        String clientId = nativeMetrics.keySet().stream().map(name -> name.tags().get("client-id"))
                .filter(Objects::nonNull).findFirst().orElse("unavailable");
        String before = nativeProducerSample("before-send", producer, clientId);
        assertThat(publishPending(worker(context.getBean(EventTransport.class)))).isOne();
        await(() -> status(created.eventId()) == EventStatus.PUBLISHED.code(), Duration.ofSeconds(20));
        String afterSend = nativeProducerSample("after-confirmed-send", producer, clientId);
        timeline(before);
        timeline(afterSend);
        consumer.stop();
        consumer = null;
        ConfigurableApplicationContext ownedContext = context;
        context = null;
        ownedContext.close();
        await(() -> clientThreadCount(clientId) == 0, Duration.ofSeconds(10));
        timeline("producer_shutdown client_id=" + clientId + " matching_threads=" + clientThreadCount(clientId)
                + " jvm_threads=" + Thread.getAllStackTraces().size() + " heap_used=" + heapUsedBytes()
                + " process_cpu_load=" + processCpuLoad());
        Files.writeString(evidenceDirectory.resolve("native-producer-resources.log"),
                before + System.lineSeparator() + afterSend + System.lineSeparator()
                        + "shutdown client_id=" + clientId + " client_threads=" + clientThreadCount(clientId)
                        + " jvm_threads=" + Thread.getAllStackTraces().size() + " heap_used=" + heapUsedBytes()
                        + " process_cpu_load=" + processCpuLoad() + System.lineSeparator(),
                StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND);
        assertThat(clientThreadCount(clientId)).isZero();
    }

    @Test void lateBrokerSuccessCannotOverwritePublishedStateAfterNaturalLeaseTakeover() throws Exception {
        evidenceDirectory = evidenceDirectory("late-lease-success");
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("late-lease", 1);
        EventTransport realSender = context.getBean(EventTransport.class);
        CountDownLatch brokerAcknowledged = new CountDownLatch(1);
        CountDownLatch allowStaleDatabaseWrite = new CountDownLatch(1);
        EventTransport blockedAfterAck = event -> {
            var receipt = realSender.send(event);
            brokerAcknowledged.countDown();
            try {
                if (!allowStaleDatabaseWrite.await(LEASE.plusSeconds(30).toMillis(), TimeUnit.MILLISECONDS))
                    throw new IllegalStateException("Timed out waiting to release stale Kafka attempt");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while holding acknowledged Kafka result", interrupted);
            }
            return receipt;
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> staleWorkerResult = executor.submit(() -> publishPending(worker(blockedAfterAck)));
        try {
            assertThat(brokerAcknowledged.await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(status(created.eventId())).isEqualTo(EventStatus.PUBLISHING.code());
            timeline("late_attempt_broker_ack event_id=" + created.eventId() + " lease_until="
                    + leaseUntil(created.eventId()) + " status=" + status(created.eventId()));
            expireAndRecover(created.eventId());
            assertThat(publishRecoveredInNewJvm("kafka-late-lease-takeover")).isOne();
            await(() -> status(created.eventId()) == EventStatus.PUBLISHED.code()
                    && committedOffset(group) >= 2 && effectCount(created.orderId()) == 1, Duration.ofSeconds(30));
            allowStaleDatabaseWrite.countDown();
            try {
                staleWorkerResult.get(15, TimeUnit.SECONDS);
                throw new AssertionError("The stale acknowledged attempt must not publish over the new claim");
            } catch (java.util.concurrent.ExecutionException expected) {
                assertThat(expected.getCause()).isInstanceOf(StaleEventClaimException.class);
            }
        } finally {
            allowStaleDatabaseWrite.countDown();
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        var records = readWireRecords(Duration.ofSeconds(2));
        timeline("late_attempt_rejected event_id=" + created.eventId() + " records=" + describeRecords(records)
                + " status=" + status(created.eventId()) + " attempts=" + attemptCount(created.eventId())
                + " committed_offset=" + committedOffset(group) + " effect_count=" + effectCount(created.orderId()));
        assertThat(records).hasSize(2);
        assertThat(records).allSatisfy(record -> assertThat(header(record, "reliable_event_id"))
                .isEqualTo(Long.toString(created.eventId())));
        assertThat(status(created.eventId())).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCount(created.eventId())).isEqualTo(2);
        assertBusinessDeduplication(created.orderId());
    }

    @Test void externallySuppliedNativeProducerRemainsUserOwnedAfterContextClose() throws Exception {
        evidenceDirectory = evidenceDirectory("user-producer-ownership");
        consumer.stop();
        consumer = null;
        ConfigurableApplicationContext previous = context;
        context = null;
        previous.close();
        org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> userProducer =
                new org.apache.kafka.clients.producer.KafkaProducer<>(Map.ofEntries(
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                KAFKA.getBootstrapServers()),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                org.apache.kafka.common.serialization.StringSerializer.class),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                org.apache.kafka.common.serialization.ByteArraySerializer.class),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all"),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 1_000),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5_000),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, 0),
                        Map.entry(org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG,
                                "m94-user-producer-" + UUID.randomUUID())));
        String clientId = userProducer.metrics().keySet().stream().map(name -> name.tags().get("client-id"))
                .filter(Objects::nonNull).findFirst().orElse("unavailable");
        try {
            context = new SpringApplicationBuilder(KafkaExampleApplication.class)
                    .initializers(application -> application.getBeanFactory()
                            .registerSingleton("userSuppliedKafkaProducer", userProducer))
                    .run("--server.port=0", "--spring.datasource.url=" + mysqlJdbcUrl(),
                            "--spring.datasource.username=" + MYSQL.getUsername(),
                            "--spring.datasource.password=" + MYSQL.getPassword(),
                            "--example.kafka.topic=" + topic, "--example.kafka.consumer-group=" + group,
                            "--example.consumer.enabled=false",
                            "--reliable-event.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                            "--reliable-event.kafka.custom-producer-send-budget=7s",
                            "--reliable-event.scheduling-enabled=false");
            assertThat(context.getBeansOfType(org.apache.kafka.clients.producer.Producer.class)).hasSize(1);
            assertThat(context.getBeanFactory().containsBean("reliableEventKafkaProducerOwner")).isFalse();
            sendRaw(userProducer, "before-context-close");
            ConfigurableApplicationContext suppliedContext = context;
            context = null;
            suppliedContext.close();
            sendRaw(userProducer, "after-context-close");
            timeline("user_producer_survived_context_close client_id=" + clientId
                    + " client_threads=" + clientThreadCount(clientId) + " native_metrics="
                    + nativeProducerSample("user-producer-after-context-close", userProducer, clientId));
            assertThat(clientThreadCount(clientId)).isGreaterThan(0);
        } finally {
            if (context != null) {
                context.close();
                context = null;
            }
            userProducer.close(Duration.ofSeconds(5));
        }
        await(() -> clientThreadCount(clientId) == 0, Duration.ofSeconds(10));
        timeline("user_producer_closed_by_test client_id=" + clientId + " client_threads="
                + clientThreadCount(clientId));
        assertThat(clientThreadCount(clientId)).isZero();
    }

    @Test void fixedHundredEventKafkaLoadEmitsIndependentResourceAndLatencySample() throws Exception {
        evidenceDirectory = evidenceDirectory("fixed-load-100");
        final int eventCount = 100;
        var producer = context.getBean(org.apache.kafka.clients.producer.Producer.class);
        @SuppressWarnings("unchecked")
        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> producerMetrics =
                (Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>) producer.metrics();
        String clientId = producerMetrics.keySet().stream().map(name -> name.tags().get("client-id"))
                .filter(Objects::nonNull).findFirst().orElse("unavailable");
        String producerBefore = nativeProducerSample("fixed-load-before", producer, clientId);
        long registrationStart = System.nanoTime();
        java.util.List<OrderService.CreatedOrder> created = new java.util.ArrayList<>(eventCount);
        for (int i = 0; i < eventCount; i++) created.add(context.getBean(OrderService.class).create("load-" + i, 1));
        long registrationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - registrationStart);
        long publishStart = System.nanoTime();
        int sendCycles = 0;
        while (jdbc.queryForObject("SELECT COUNT(*) FROM reliable_event_outbox WHERE status=?",
                Long.class, EventStatus.PUBLISHED.code()) < eventCount && sendCycles++ < 20) {
            publishPending(worker(context.getBean(EventTransport.class)));
        }
        long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - publishStart);
        await(() -> committedOffset(group) >= eventCount
                && jdbc.queryForObject("SELECT COUNT(*) FROM example_order_effect", Long.class) == eventCount,
                Duration.ofSeconds(45));
        long postPublicationCatchupMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - publishStart) - sendMillis;
        long totalPublishToConsumerCommitMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - publishStart);
        String producerAfter = nativeProducerSample("fixed-load-after", producer, clientId);
        var records = readWireRecords(Duration.ofSeconds(3));
        assertThat(records).hasSize(eventCount);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM example_consumed_event", Long.class))
                .isEqualTo((long) eventCount);
        assertThat(jdbc.queryForObject("SELECT SUM(handled_count) FROM example_order_effect", Long.class))
                .isEqualTo((long) eventCount);
        long distinctIds = records.stream().map(record -> header(record, "reliable_event_id")).distinct().count();
        assertThat(distinctIds).isEqualTo(eventCount);
        timeline("fixed_load_100 registration_ms=" + registrationMillis + " send_ms=" + sendMillis
                + " post_publication_consumer_catchup_tail_ms=" + postPublicationCatchupMillis
                + " total_publish_to_consumer_commit_ms=" + totalPublishToConsumerCommitMillis
                + " send_cycles=" + sendCycles + " wire_count=" + records.size()
                + " distinct_event_ids=" + distinctIds + " committed_offset=" + committedOffset(group)
                + " dedupe_rows=" + jdbc.queryForObject("SELECT COUNT(*) FROM example_consumed_event", Long.class)
                + " effect_sum=" + jdbc.queryForObject("SELECT SUM(handled_count) FROM example_order_effect", Long.class));
        Files.writeString(evidenceDirectory.resolve("performance-sample.csv"),
                "sample,broker_image,brokers,partitions,replication_factor,acks,idempotence,batch_events,batch_size,"
                + "registration_ms,send_ms,post_publication_consumer_catchup_tail_ms,total_publish_to_consumer_commit_ms,"
                        + "send_cycles,wire_records,distinct_event_ids,"
                        + "consumer_committed_offset,dedupe_rows,business_effect_sum,producer_sample,jvm_threads,heap_used,cpu_load\n"
                        + "kafka-fixed-100,apache/kafka:3.9.2,1,1,1,all,true,100,10," + registrationMillis + ","
                        + sendMillis + "," + postPublicationCatchupMillis + ","
                        + totalPublishToConsumerCommitMillis + "," + sendCycles + "," + records.size() + ","
                        + distinctIds + "," + committedOffset(group) + ","
                        + jdbc.queryForObject("SELECT COUNT(*) FROM example_consumed_event", Long.class) + ","
                        + jdbc.queryForObject("SELECT SUM(handled_count) FROM example_order_effect", Long.class)
                        + ",\"" + producerBefore + " | " + producerAfter + "\","
                        + Thread.getAllStackTraces().size() + "," + heapUsedBytes() + "," + processCpuLoad()
                        + System.lineSeparator(), StandardCharsets.UTF_8);
        timeline(producerBefore);
        timeline(producerAfter);
    }

    @Test void repeatedNativeProducerContextShutdownReturnsClientThreadsToBaseline() throws Exception {
        evidenceDirectory = evidenceDirectory("repeated-producer-contexts");
        consumer.stop();
        consumer = null;
        ConfigurableApplicationContext initial = context;
        context = null;
        initial.close();
        for (int cycle = 1; cycle <= 3; cycle++) {
            ConfigurableApplicationContext repeated = new SpringApplicationBuilder(KafkaExampleApplication.class)
                    .run("--server.port=0", "--spring.datasource.url=" + mysqlJdbcUrl(),
                            "--spring.datasource.username=" + MYSQL.getUsername(),
                            "--spring.datasource.password=" + MYSQL.getPassword(),
                            "--example.kafka.topic=" + topic, "--example.kafka.consumer-group=" + group,
                            "--example.consumer.enabled=false",
                            "--reliable-event.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                            "--reliable-event.scheduling-enabled=false");
            String clientId = "unavailable";
            try {
                var producer = repeated.getBean(org.apache.kafka.clients.producer.Producer.class);
                @SuppressWarnings("unchecked")
                Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics =
                        (Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>) producer.metrics();
                clientId = metrics.keySet().stream().map(name -> name.tags().get("client-id"))
                        .filter(Objects::nonNull).findFirst().orElse("unavailable");
                timeline("repeated_context cycle=" + cycle + " " + nativeProducerSample("created", producer, clientId));
            } finally {
                repeated.close();
            }
            String closedClientId = clientId;
            await(() -> clientThreadCount(closedClientId) == 0, Duration.ofSeconds(10));
            String sample = "cycle=" + cycle + " client_id=" + clientId + " owned_threads_after_close="
                    + clientThreadCount(clientId) + " jvm_threads=" + Thread.getAllStackTraces().size()
                    + " heap_used=" + heapUsedBytes() + " process_cpu_load=" + processCpuLoad();
            timeline("repeated_context_closed " + sample);
            Files.writeString(evidenceDirectory.resolve("native-context-repeats.log"),
                    sample + System.lineSeparator(), StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
            assertThat(clientThreadCount(clientId)).isZero();
        }
    }

    @Test void nativeProducerShutdownBoundsAnInFlightSendWhenBrokerIsUnavailable() throws Exception {
        evidenceDirectory = evidenceDirectory("producer-timeout-shutdown");
        consumer.stop();
        consumer = null;
        ConfigurableApplicationContext initial = context;
        context = null;
        initial.close();
        ConfigurableApplicationContext shortShutdown = new SpringApplicationBuilder(KafkaExampleApplication.class)
                .run("--server.port=0", "--spring.datasource.url=" + mysqlJdbcUrl(),
                        "--spring.datasource.username=" + MYSQL.getUsername(),
                        "--spring.datasource.password=" + MYSQL.getPassword(),
                        "--example.kafka.topic=" + topic, "--example.kafka.consumer-group=" + group,
                        "--example.consumer.enabled=false",
                        "--reliable-event.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--reliable-event.kafka.shutdown-timeout=1s",
                        "--reliable-event.scheduling-enabled=false");
        context = shortShutdown;
        var producer = shortShutdown.getBean(org.apache.kafka.clients.producer.Producer.class);
        sendRaw(producer, "warmup-before-outage");
        @SuppressWarnings("unchecked")
        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics =
                (Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>) producer.metrics();
        String clientId = metrics.keySet().stream().map(name -> name.tags().get("client-id"))
                .filter(Objects::nonNull).findFirst().orElse("unavailable");
        boolean paused = false;
        java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> inFlight = null;
        long closeMillis = -1;
        try {
            KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
            paused = true;
            inFlight = producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic,
                    "m94-inflight-shutdown", "inflight-during-broker-pause".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(250);
            assertThat(inFlight.isDone()).as("send should remain in flight while broker is paused").isFalse();
            String pending = nativeProducerSample("in-flight-before-shutdown", producer, clientId);
            long closeStart = System.nanoTime();
            context = null;
            shortShutdown.close();
            closeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStart);
            timeline(pending);
            timeline("producer_timeout_shutdown client_id=" + clientId + " configured_timeout=PT1S"
                    + " close_elapsed_ms=" + closeMillis + " client_threads_after_close="
                    + clientThreadCount(clientId) + " in_flight_done=" + inFlight.isDone());
        } finally {
            if (context != null) {
                context.close();
                context = null;
            }
            if (paused) KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }
        assertThat(closeMillis).isBetween(500L, 10_000L);
        var completedSend = Objects.requireNonNull(inFlight);
        String closedClientId = clientId;
        await(completedSend::isDone, Duration.ofSeconds(10));
        await(() -> clientThreadCount(closedClientId) == 0, Duration.ofSeconds(10));
        var records = readWireRecords(Duration.ofSeconds(3));
        timeline("producer_timeout_shutdown_final records=" + describeRecords(records)
                + " client_threads=" + clientThreadCount(clientId));
        Files.writeString(evidenceDirectory.resolve("timeout-shutdown.log"),
                "configured_timeout=PT1S\nclose_elapsed_ms=" + closeMillis + "\nclient_id=" + clientId
                        + "\nclient_threads_after_close=" + clientThreadCount(clientId)
                        + "\nin_flight_future_done=" + inFlight.isDone() + "\nrecords="
                        + describeRecords(records) + System.lineSeparator(), StandardCharsets.UTF_8);
        assertThat(clientThreadCount(clientId)).isZero();
    }

    @Test void realKafkaSchedulerNormalStopPublishesActiveAndCancelsQueuedCandidates() throws Exception {
        runNativeSchedulerStopScenario(false);
    }

    @Test void realKafkaSchedulerTimeoutStopCancelsQueuedAndLeavesActiveLeaseRecoverable() throws Exception {
        runNativeSchedulerStopScenario(true);
    }

    private void runNativeSchedulerStopScenario(boolean timeout) throws Exception {
        evidenceDirectory = evidenceDirectory(timeout ? "scheduler-timeout-stop" : "scheduler-normal-stop");
        Files.writeString(evidenceDirectory.resolve("effective-config.txt"),
                "scheduler=production-ReliableEventScheduler-context-bean\nworker_threads=1\nqueue_capacity=2\n"
                        + "claim_batch_size=3\npoll_interval=PT24H\nlease=PT12S\nshutdown_timeout="
                        + (timeout ? "PT0.5S" : "PT15S") + "\nstop_gate=test-only PublicationObserver.sendSucceeded "
                        + "after real Kafka receipt\nproducer_acks=all\nproducer_idempotence=true\n"
                        + "producer_max_block_ms=1000\nproducer_request_timeout_ms=3000\n"
                        + "producer_delivery_timeout_ms=5000\nproducer_max_in_flight=5\n",
                StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.APPEND);
        java.util.List<OrderService.CreatedOrder> events = java.util.List.of(
                context.getBean(OrderService.class).create("scheduler-stop-a-" + UUID.randomUUID(), 1),
                context.getBean(OrderService.class).create("scheduler-stop-b-" + UUID.randomUUID(), 1),
                context.getBean(OrderService.class).create("scheduler-stop-c-" + UUID.randomUUID(), 1));
        consumer.stop();
        consumer = null;
        ConfigurableApplicationContext previous = context;
        context = null;
        previous.close();

        SchedulerStopGate observer = new SchedulerStopGate();
        Duration shutdownBudget = timeout ? Duration.ofMillis(500) : Duration.ofSeconds(15);
        ConfigurableApplicationContext schedulerContext = new SpringApplicationBuilder(KafkaExampleApplication.class)
                .initializers(application -> application.getBeanFactory()
                        .registerSingleton("m94KafkaSchedulerStopObserver", observer))
                .run("--server.port=0", "--spring.datasource.url=" + mysqlJdbcUrl(),
                        "--spring.datasource.username=" + MYSQL.getUsername(),
                        "--spring.datasource.password=" + MYSQL.getPassword(),
                        "--example.kafka.topic=" + topic, "--example.kafka.consumer-group=" + group,
                        "--example.consumer.enabled=false",
                        "--reliable-event.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--reliable-event.scheduling-enabled=true", "--reliable-event.claim-batch-size=3",
                        "--reliable-event.worker-threads=1", "--reliable-event.worker-queue-capacity=2",
                        "--reliable-event.lease-duration=12s",
                        "--reliable-event.poll-interval=1d",
                        "--reliable-event.shutdown-timeout=" + (timeout ? "500ms" : "15s"));
        context = schedulerContext;
        ReliableEventScheduler scheduler = schedulerContext.getBean(ReliableEventScheduler.class);
        boolean contextClosed = false;
        try {
            assertThat(observer.schedulerStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(observer.brokerAcknowledged.await(20, TimeUnit.SECONDS)).isTrue();
            await(() -> observer.queuedCandidates.get() == 2 && observer.inflightWorkers.get() == 1,
                    Duration.ofSeconds(10));
            long activeId = observer.activeEventId.get();
            assertThat(activeId).isEqualTo(events.get(0).eventId());
            assertThat(status(activeId)).isEqualTo(EventStatus.PUBLISHING.code());
            assertThat(attemptCount(activeId)).isOne();
            for (int index = 1; index < events.size(); index++) {
                long queuedId = events.get(index).eventId();
                assertThat(status(queuedId)).isEqualTo(EventStatus.PENDING.code());
                assertThat(attemptCount(queuedId)).isZero();
            }
            var acknowledgedRecord = readWireRecords(Duration.ofSeconds(2));
            assertThat(acknowledgedRecord).hasSize(1);
            assertThat(header(acknowledgedRecord.get(0), "reliable_event_id")).isEqualTo(Long.toString(activeId));
            timeline("scheduler_stop_gate timeout=" + timeout
                    + " gate=test-only-PublicationObserver.sendSucceeded-after-realReceipt active_event_id=" + activeId
                    + " queued_event_ids=" + events.subList(1, 3).stream()
                    .map(OrderService.CreatedOrder::eventId).toList() + " queue_count=" + observer.queuedCandidates.get()
                    + " inflight=" + observer.inflightWorkers.get() + " db_statuses="
                    + events.stream().map(value -> status(value.eventId())).toList()
                    + " db_attempts=" + events.stream().map(value -> attemptCount(value.eventId())).toList()
                    + " broker_records=" + describeRecords(acknowledgedRecord));

            java.util.concurrent.CompletableFuture<Void> closing = java.util.concurrent.CompletableFuture.runAsync(
                    schedulerContext::close);
            assertThat(observer.schedulerStopping.await(10, TimeUnit.SECONDS)).isTrue();
            if (!timeout) {
                await(() -> observer.queuedCandidates.get() == 0, Duration.ofSeconds(5));
                observer.release.countDown();
            }
            closing.get(timeout ? 15 : 30, TimeUnit.SECONDS);
            context = null;
            contextClosed = true;
            if (timeout) {
                assertThat(status(activeId)).isEqualTo(EventStatus.PUBLISHING.code());
                observer.release.countDown();
                assertThat(observer.senderReleased.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(observer.workerFinished.await(10, TimeUnit.SECONDS)).isTrue();
                await(() -> schedulerThreadCount("reliable-event-send-") == 0, Duration.ofSeconds(10));
                await(() -> schedulerThreadCount("reliable-event-scan-") == 0, Duration.ofSeconds(10));
                assertThat(status(activeId)).isEqualTo(EventStatus.PUBLISHING.code());
                assertThat(observer.queuedCandidates.get()).isZero();
                assertThat(observer.inflightWorkers.get()).isZero();
                for (int index = 1; index < events.size(); index++) {
                    long queuedId = events.get(index).eventId();
                    assertThat(status(queuedId)).isEqualTo(EventStatus.PENDING.code());
                    assertThat(attemptCount(queuedId)).isZero();
                }
                recoverAfterClosedContextNaturally(activeId);
                assertThat(status(activeId)).isEqualTo(EventStatus.RETRY_WAIT.code());
                assertThat(attemptCount(activeId)).isOne();
            } else {
                assertThat(observer.workerFinished.await(10, TimeUnit.SECONDS)).isTrue();
                await(() -> schedulerThreadCount("reliable-event-send-") == 0, Duration.ofSeconds(10));
                await(() -> schedulerThreadCount("reliable-event-scan-") == 0, Duration.ofSeconds(10));
                assertThat(status(activeId)).isEqualTo(EventStatus.PUBLISHED.code());
                assertThat(attemptCount(activeId)).isOne();
                assertThat(observer.queuedCandidates.get()).isZero();
                assertThat(observer.inflightWorkers.get()).isZero();
                for (int index = 1; index < events.size(); index++) {
                    long queuedId = events.get(index).eventId();
                    assertThat(status(queuedId)).isEqualTo(EventStatus.PENDING.code());
                    assertThat(attemptCount(queuedId)).isZero();
                }
            }
            var finalRecords = readWireRecords(Duration.ofSeconds(2));
            timeline("scheduler_stop_final timeout=" + timeout + " active_status=" + status(activeId)
                    + " queued_statuses=" + events.subList(1, 3).stream()
                    .map(value -> status(value.eventId())).toList()
                    + " queued_attempts=" + events.subList(1, 3).stream()
                    .map(value -> attemptCount(value.eventId())).toList()
                    + " scheduler_queue=" + observer.queuedCandidates.get()
                    + " worker_inflight=" + observer.inflightWorkers.get()
                    + " worker_finished=" + observer.finishedWorkers.get()
                    + " worker_threads=" + schedulerThreadCount("reliable-event-send-")
                    + " scan_threads=" + schedulerThreadCount("reliable-event-scan-")
                    + " broker_records=" + describeRecords(finalRecords));
            assertThat(finalRecords).hasSize(1);
            assertThat(header(finalRecords.get(0), "reliable_event_id")).isEqualTo(Long.toString(activeId));
        } finally {
            observer.release.countDown();
            if (!contextClosed) {
                scheduler.stop();
                schedulerContext.close();
                context = null;
            }
        }
    }

    private void recoverAfterClosedContextNaturally(long eventId) throws InterruptedException {
        await(() -> jdbc.queryForObject("SELECT lease_until <= ? FROM reliable_event_outbox WHERE id=?",
                Boolean.class, Timestamp.from(Clock.systemUTC().instant()), eventId), LEASE.plusSeconds(5));
        timeline("scheduler_timeout_lease_natural event_id=" + eventId + " observed_at="
                + Clock.systemUTC().instant() + " " + mysqlClockSnapshot(eventId));
        int recovered = new JdbcExpiredLeaseRecovery(jdbc, new DataSourceTransactionManager(evidenceDataSource),
                10, new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0))
                .recoverExpiredLeases();
        assertThat(recovered).isOne();
    }

    private static int schedulerThreadCount(String namePrefix) {
        return (int) Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive).filter(thread -> thread.getName().startsWith(namePrefix)).count();
    }

    private static final class SchedulerStopGate implements PublicationObserver {
        private final java.util.concurrent.atomic.AtomicInteger queuedCandidates = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger inflightWorkers = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger finishedWorkers = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicLong activeEventId = new java.util.concurrent.atomic.AtomicLong(-1);
        private final CountDownLatch brokerAcknowledged = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch senderReleased = new CountDownLatch(1);
        private final CountDownLatch workerFinished = new CountDownLatch(1);
        private final CountDownLatch schedulerStarted = new CountDownLatch(1);
        private final CountDownLatch schedulerStopping = new CountDownLatch(1);

        @Override public void candidateQueued() { queuedCandidates.incrementAndGet(); }
        @Override public void candidateDequeued() { queuedCandidates.decrementAndGet(); }
        @Override public void workerStarted() { inflightWorkers.incrementAndGet(); }
        @Override public void workerFinished() {
            inflightWorkers.decrementAndGet();
            finishedWorkers.incrementAndGet();
            workerFinished.countDown();
        }
        @Override public void schedulerState(boolean enabled, boolean running) {
            if (enabled && running) schedulerStarted.countDown();
            if (enabled && !running && schedulerStarted.getCount() == 0) schedulerStopping.countDown();
        }
        @Override public void sendSucceeded(dev.reliableevent.jdbc.internal.model.ClaimedEvent event,
                                            TransportReceipt receipt,
                                            long elapsedNanos, java.time.Instant at) {
            activeEventId.set(event.event().id().value());
            brokerAcknowledged.countDown();
            while (release.getCount() != 0) {
                try { release.await(); }
                catch (InterruptedException ignored) {
                    // Keep the acknowledged Worker blocked until the test proves stop/close state.
                }
            }
            senderReleased.countDown();
        }
    }

    private void sendRaw(org.apache.kafka.clients.producer.Producer<String, byte[]> producer, String key)
            throws Exception {
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key,
                ("m9.4-resource-" + key).getBytes(StandardCharsets.UTF_8)))
                .get(10, TimeUnit.SECONDS);
    }

    private String nativeProducerSample(String phase, org.apache.kafka.clients.producer.Producer<?, ?> producer,
                                        String clientId) {
        Map<String, Double> selected = producer.metrics().entrySet().stream()
                .filter(entry -> entry.getKey().group().equals("producer-metrics"))
                .filter(entry -> entry.getKey().name().contains("buffer")
                        || entry.getKey().name().contains("waiting-threads"))
                .collect(java.util.stream.Collectors.toMap(entry -> entry.getKey().name(),
                        entry -> entry.getValue().metricValue() instanceof Number number
                                ? number.doubleValue() : Double.NaN, (left, right) -> left));
        int clientThreads = clientThreadCount(clientId);
        return phase + " client_id=" + clientId + " producer_metrics="
                + (selected.isEmpty() ? "unavailable" : selected) + " waiting_threads="
                + selected.entrySet().stream().filter(entry -> entry.getKey().contains("waiting-threads"))
                .map(Map.Entry::getValue).findFirst().orElse(Double.NaN)
                + " client_owned_threads=" + clientThreads + " jvm_threads=" + Thread.getAllStackTraces().size()
                + " heap_used=" + heapUsedBytes() + " process_cpu_load=" + processCpuLoad();
    }

    private int clientThreadCount(String clientId) {
        if (clientId == null || clientId.equals("unavailable")) return -1;
        String marker = "| " + clientId;
        return (int) Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive).filter(thread -> thread.getName().contains(marker)).count();
    }

    private static long heapUsedBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static String processCpuLoad() {
        java.lang.management.OperatingSystemMXBean bean = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        return bean instanceof com.sun.management.OperatingSystemMXBean extended
                ? Double.toString(extended.getProcessCpuLoad()) : "unavailable";
    }

    private int publishRecoveredInNewJvm(String workerIdPrefix) throws Exception {
        String workerId = workerIdPrefix + "-" + UUID.randomUUID();
        Path barrier = evidenceDirectory.resolve(workerId + ".release");
        try (KafkaWorkerProcessFixture recovery = startWorker(workerId, barrier)) {
            recovery.awaitReady(Duration.ofSeconds(45));
            Files.writeString(barrier, "go\n", StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE_NEW);
            timeline("recovery_instance_released worker=" + workerId);
            return recovery.awaitResult(Duration.ofSeconds(60));
        }
    }

    private KafkaWorkerProcessFixture startWorker(String workerId, Path barrier) throws Exception {
        return KafkaWorkerProcessFixture.start(workerId, mysqlJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword(),
                KAFKA.getBootstrapServers(), topic, evidenceDirectory, barrier);
    }

    private FutureOrder registerFutureOrder(Duration delay) {
        java.time.Instant availableAt = Clock.systemUTC().instant().plus(delay);
        TransactionTemplate transaction = new TransactionTemplate(
                context.getBean(org.springframework.transaction.PlatformTransactionManager.class));
        return transaction.execute(status -> {
            GeneratedKeyHolder key = new GeneratedKeyHolder();
            jdbc.update(connection -> {
                var statement = connection.prepareStatement(
                        "INSERT INTO example_order (item_code, quantity, created_at) VALUES (?, ?, UTC_TIMESTAMP(3))",
                        java.sql.Statement.RETURN_GENERATED_KEYS);
                statement.setString(1, "future-item");
                statement.setInt(2, 3);
                return statement;
            }, key);
            long orderId = Objects.requireNonNull(key.getKey()).longValue();
            EventId eventId = context.getBean(ReliableEventPublisher.class).publish(new ReliableEvent<>(
                    OrderService.EVENT_TYPE, Long.toString(orderId),
                    new OrderCreatedPayload(orderId, "future-item", 3), availableAt,
                    Map.of("source", "m9.4-kafka-future-test")));
            return new FutureOrder(orderId, eventId, availableAt);
        });
    }

    private KafkaCrashProcessFixture startChild(KafkaCrashMode mode, String workerId) throws Exception {
        return KafkaCrashProcessFixture.start(mode, workerId, mysqlJdbcUrl(), MYSQL.getUsername(),
                MYSQL.getPassword(), KAFKA.getBootstrapServers(), topic, evidenceDirectory);
    }

    private String mysqlJdbcUrl() {
        return MYSQL.getJdbcUrl() + "?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true";
    }

    private void assertClaimed(long eventId, String workerId) {
        assertThat(jdbc.queryForObject("SELECT status FROM reliable_event_outbox WHERE id=?", Integer.class, eventId))
                .isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(jdbc.queryForObject("SELECT lease_owner FROM reliable_event_outbox WHERE id=?", String.class, eventId))
                .isEqualTo(workerId);
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM reliable_event_outbox WHERE id=?", Integer.class, eventId))
                .isOne();
        timeline("mysql_clock event_id=" + eventId + " " + mysqlClockSnapshot(eventId));
    }

    private void expireAndRecover(long eventId) throws InterruptedException {
        timeline("lease_wait_start event_id=" + eventId + " lease_until=" + leaseUntil(eventId)
                + " " + mysqlClockSnapshot(eventId));
        await(() -> jdbc.queryForObject("SELECT lease_until <= ? FROM reliable_event_outbox WHERE id=?",
                Boolean.class, Timestamp.from(Clock.systemUTC().instant()), eventId), LEASE.plusSeconds(5));
        timeline("lease_naturally_expired event_id=" + eventId + " observed_at=" + Clock.systemUTC().instant()
                + " " + mysqlClockSnapshot(eventId));
        int recovered = new JdbcExpiredLeaseRecovery(jdbc,
                context.getBean(org.springframework.transaction.PlatformTransactionManager.class), 10,
                new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0))
                .recoverExpiredLeases();
        assertThat(recovered).isOne();
        timeline("lease_recovered event_id=" + eventId + " status=" + status(eventId));
        assertThat(status(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
    }

    private java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>> readWireRecords(Duration timeout) {
        var configuration = new java.util.HashMap<String, Object>(consumerConfiguration("wire-" + UUID.randomUUID()));
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, byte[]> wire = new KafkaConsumer<>(configuration)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            wire.assign(java.util.List.of(partition));
            wire.seekToBeginning(java.util.List.of(partition));
            java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>> records = new java.util.ArrayList<>();
            do {
                try {
                    for (var record : wire.poll(Duration.ofMillis(200)).records(partition)) records.add(record);
                }
                catch (org.apache.kafka.common.errors.WakeupException interrupted) {
                    Thread.currentThread().interrupt();
                    return records;
                }
            } while (System.nanoTime() < deadline);
            return records;
        }
    }

    private Path evidenceDirectory(String label) throws Exception {
        Path directory = Path.of("..", "target", "evidence", "m9.4-kafka-crash-" + label + "-"
                + java.time.LocalDateTime.now().toString().replace(':', '-') + "-" + UUID.randomUUID());
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("effective-config.txt"),
                "mysql-image=mysql:8.0.36\nkafka-image=apache/kafka:3.9.2\nlease=PT12S\ntopic=" + topic
                        + "\nconsumer-group=" + group + "\n", StandardCharsets.UTF_8);
        return directory;
    }

    private void timeline(String line) {
        try { Files.writeString(evidenceDirectory.resolve("timeline.log"), Clock.systemUTC().instant() + " " + line
                + System.lineSeparator(), StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND); }
        catch (java.io.IOException failure) { throw new IllegalStateException("Unable to write Kafka evidence", failure); }
    }

    private String leaseUntil(long eventId) {
        Timestamp timestamp = jdbc.queryForObject("SELECT lease_until FROM reliable_event_outbox WHERE id=?",
                Timestamp.class, eventId);
        return timestamp == null ? "null" : timestamp.toInstant().toString();
    }
    private String mysqlClockSnapshot(long eventId) {
        Map<String, Object> clock = jdbc.queryForMap("""
                SELECT @@session.time_zone AS session_zone, UTC_TIMESTAMP(3) AS utc_now, NOW(3) AS session_now,
                       DATE_FORMAT(lease_until, '%Y-%m-%d %H:%i:%s.%f') AS lease_datetime,
                       lease_until <= UTC_TIMESTAMP(3) AS expired_by_database,
                       lease_until <= ? AS expired_by_bound_clock
                FROM reliable_event_outbox WHERE id=?
                """, Timestamp.from(Clock.systemUTC().instant()), eventId);
        return clock.toString();
    }
    private int attemptCount(long eventId) {
        return jdbc.queryForObject("SELECT attempt_count FROM reliable_event_outbox WHERE id=?", Integer.class, eventId);
    }
    private record FutureOrder(long orderId, EventId eventId, java.time.Instant availableAt) { }
    private String describeRecords(java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>> records) {
        return records.stream().map(record -> "event_id=" + header(record, "reliable_event_id")
                + ",partition=" + record.partition() + ",offset=" + record.offset()).toList().toString();
    }
    private static String header(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record, String key) {
        var header = record.headers().lastHeader(key);
        return header == null || header.value() == null ? "missing" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private Map<String, Object> consumerConfiguration(String consumerGroup) {
        return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroup, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    }

    private JdbcEventPublicationWorker worker(EventTransport sender) {
        Clock clock = Clock.systemUTC();
        String workerId = "kafka-crash-recovery-test";
        PublicationObserver observer = context.getBean(PublicationObserver.class);
        JdbcPublicationStore store = new JdbcPublicationStore(jdbc,
                context.getBean(org.springframework.transaction.PlatformTransactionManager.class),
                clock, workerId, LEASE, observer);
        return new JdbcEventPublicationWorker(store, sender, clock,
                new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0),
                observer, context.getBean(PublicationTracer.class));
    }

    private int publishPending(JdbcEventPublicationWorker worker) {
        Clock clock = Clock.systemUTC();
        PublicationObserver observer = context.getBean(PublicationObserver.class);
        JdbcPublicationStore store = new JdbcPublicationStore(jdbc,
                context.getBean(org.springframework.transaction.PlatformTransactionManager.class),
                clock, "kafka-crash-recovery-test", LEASE, observer);
        int published = 0;
        for (var candidate : store.findDueEventCandidates(10)) {
            var claim = store.claimCandidate(candidate);
            if (claim.isPresent() && worker.publishClaimedEvent(claim.get())) {
                published++;
            }
        }
        return published;
    }

    private int status(long id) {
        return jdbc.queryForObject("SELECT status FROM reliable_event_outbox WHERE id=?", Integer.class, id);
    }
    private long effectCount(long orderId) {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM example_order_effect WHERE order_id=?", Long.class, orderId);
        return count == null ? 0 : count;
    }
    private void assertBusinessDeduplication(long orderId) {
        assertThat(jdbc.queryForObject("SELECT handled_count FROM example_order_effect WHERE order_id=?",
                Integer.class, orderId)).isOne();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM example_consumed_event", Long.class)).isOne();
    }
    private long committedOffset(String consumerGroup) {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets(consumerGroup).all().get(5, TimeUnit.SECONDS);
            var offset = offsets.get(consumerGroup).get(new TopicPartition(topic, 0));
            return offset == null ? -1 : offset.offset();
        } catch (Exception failure) { throw new IllegalStateException("Unable to read Kafka consumer offset", failure); }
    }
    private static void await(java.util.function.BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).as("condition should hold within " + timeout).isTrue();
    }
}
