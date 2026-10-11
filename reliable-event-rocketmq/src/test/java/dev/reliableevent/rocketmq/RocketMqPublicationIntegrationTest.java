package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.TransportReceipt;
import dev.reliableevent.jdbc.JdbcReliableEventPublisher;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.spi.TransportFailureType;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Testcontainers(disabledWithoutDocker = true)
@Timeout(180)
class RocketMqPublicationIntegrationTest {

    private static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Duration LEASE_DURATION = Duration.ofSeconds(30);

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("reliable_event_rocketmq_test")
            .withUsername("test")
            .withPassword("test");

    private static RocketMqTestEnvironment environment;
    private static Producer producer;
    private static SimpleConsumer consumer;
    private static RocketMqEventTransport sender;
    private static JdbcTemplate jdbcTemplate;
    private static DataSourceTransactionManager transactionManager;

    @BeforeAll
    static void startDependencies() throws Exception {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(MYSQL.getDriverClassName());
        dataSource.setUrl(mysqlJdbcUrlUtc());
        dataSource.setUsername(MYSQL.getUsername());
        dataSource.setPassword(MYSQL.getPassword());
        jdbcTemplate = new JdbcTemplate(dataSource);
        assertThat(jdbcTemplate.queryForObject("SELECT @@session.time_zone", String.class))
                .as("MySQL test sessions use UTC for lease and retry timestamps")
                .isIn("UTC", "+00:00");
        transactionManager = new DataSourceTransactionManager(dataSource);
        new ResourceDatabasePopulator(
                new ClassPathResource("schema/reliable-event-outbox.sql")
        ).execute(dataSource);

        environment = new RocketMqTestEnvironment();
        environment.start();
        ClientConfiguration configuration = environment.clientConfiguration(Duration.ofSeconds(3));
        producer = PROVIDER.newProducerBuilder()
                .setClientConfiguration(configuration)
                .setTopics(RocketMqTestEnvironment.TOPIC)
                .setMaxAttempts(1)
                .build();
        consumer = PROVIDER.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(RocketMqTestEnvironment.CONSUMER_GROUP)
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(Map.of(
                        RocketMqTestEnvironment.TOPIC,
                        FilterExpression.SUB_ALL
                ))
                .build();
        sender = sender(producer, 1024 * 1024);
    }

    @AfterAll
    static void stopDependencies() throws Exception {
        if (environment != null) {
            environment.unpauseBroker();
        }
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
        if (environment != null) {
            environment.close();
        }
    }

    @BeforeEach
    void clearOutbox() {
        jdbcTemplate.update("DELETE FROM reliable_event_outbox");
        jdbcTemplate.update("DELETE FROM reliable_event_identity");
    }

    @Test
    void publishesACommittedOutboxEventToRocketMq() throws Exception {
        String eventKey = "normal-" + System.nanoTime();
        EventId eventId = publish(eventKey, new TaskPayload(101));

        assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isOne();

        MessageView message = receiveMessages(eventKey, 1).get(0);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(message.getTopic()).isEqualTo(RocketMqTestEnvironment.TOPIC);
        assertThat(message.getTag()).contains(RocketMqTestEnvironment.TAG);
        assertThat(message.getKeys()).contains(eventKey);
        assertThat(OBJECT_MAPPER.readTree(readBody(message.getBody())).path("taskId").asLong())
                .isEqualTo(101L);
        assertThat(message.getProperties())
                .containsEntry("source", "publication-integration-test")
                .containsEntry(
                        RocketMqMessageFactory.EVENT_ID_PROPERTY,
                        Long.toString(eventId.value())
                )
                .containsEntry(RocketMqMessageFactory.EVENT_TYPE_PROPERTY, "coupon-task-execute")
                .containsEntry(RocketMqMessageFactory.EVENT_KEY_PROPERTY, eventKey);
        assertThat(message.getMessageId().toString()).isNotBlank();
        consumer.ack(message);
    }

    @Test
    void publishesAttemptTraceHeadersToRocketMqAndKeepsOutboxRegistrationHeaders() throws Exception {
        String eventKey = "trace-attempt-" + System.nanoTime();
        String registrationTraceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
        EventId eventId = new TransactionTemplate(transactionManager).execute(status ->
                new JdbcReliableEventPublisher(jdbcTemplate, OBJECT_MAPPER).publish(new ReliableEvent<>(
                        "coupon-task-execute", eventKey, new TaskPayload(111), Instant.now().minusSeconds(1),
                        Map.of("source", "publication-integration-test", "traceparent", registrationTraceparent))));
        String attemptTraceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-cccccccccccccccc-01";
        PublicationTracer tracer = claimed -> {
            try {
                Map<String, String> headers = new java.util.LinkedHashMap<>(OBJECT_MAPPER.readValue(
                        claimed.event().headersJson(), OBJECT_MAPPER.getTypeFactory()
                                .constructMapType(Map.class, String.class, String.class)));
                headers.put("traceparent", attemptTraceparent);
                StoredEvent copy = new StoredEvent(claimed.event().id(), claimed.event().eventType(),
                        claimed.event().eventKey(), claimed.event().payloadJson(),
                        OBJECT_MAPPER.writeValueAsString(headers));
                return new PublicationTracer.Attempt() {
                    @Override public StoredEvent eventForSend() { return copy; }
                    @Override public void sendSucceeded(TransportReceipt receipt) { }
                    @Override public void sendFailed(TransportFailureType type, Throwable failure) { }
                    @Override public void stateUpdated(String status, boolean pending) { }
                    @Override public void stateUpdateFailed(String status, Throwable failure, boolean rejected) { }
                    @Override public void close() { }
                };
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        };
        JdbcEventPublicationWorker tracedWorker = new JdbcEventPublicationWorker(jdbcTemplate,
                transactionManager, sender, Clock.systemUTC(), 10, "rocketmq-trace-worker", LEASE_DURATION,
                new ExponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30), 0.0, () -> 0.0),
                dev.reliableevent.jdbc.internal.observation.PublicationObserver.NOOP, tracer);

        assertThat(tracedWorker.publishDueEvents()).isOne();
        MessageView message = receiveMessages(eventKey, 1).get(0);
        assertThat(message.getProperties())
                .containsEntry("traceparent", attemptTraceparent)
                .containsEntry("source", "publication-integration-test");
        String persistedHeaders = jdbcTemplate.queryForObject(
                "SELECT headers FROM reliable_event_outbox WHERE id = ?", String.class, eventId.value());
        assertThat(OBJECT_MAPPER.readTree(persistedHeaders).path("traceparent").asText())
                .isEqualTo(registrationTraceparent);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        consumer.ack(message);
    }

    @Test
    void missingDestinationBecomesDeadWithoutCallingTheProducer() throws Exception {
        Producer unusedProducer = mock(Producer.class);
        RocketMqEventTransport missingDestinationSender = new RocketMqEventTransport(PROVIDER, unusedProducer, new MapEventDestinationResolver(Map.of()));
        EventId eventId = publish("missing-" + System.nanoTime(), new TaskPayload(102));

        assertThat(worker(missingDestinationSender, Clock.systemUTC()).publishDueEvents()).isZero();

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(lastErrorOf(eventId)).contains("No RocketMQ destination is configured")
                .doesNotContain("TaskPayload");
        verify(unusedProducer, never()).send(any(Message.class));
    }

    @Test
    void oversizedBodyBecomesDeadWithoutCallingTheProducer() throws Exception {
        Producer unusedProducer = mock(Producer.class);
        RocketMqEventTransport smallBodySender = sender(unusedProducer, 32);
        EventId eventId = publish(
                "oversized-" + System.nanoTime(),
                new LargePayload("x".repeat(128))
        );

        assertThat(worker(smallBodySender, Clock.systemUTC()).publishDueEvents()).isZero();

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(lastErrorOf(eventId)).contains("exceeds the configured limit")
                .doesNotContain("xxxxxxxx");
        verify(unusedProducer, never()).send(any(Message.class));
    }

    @Test
    void brokerOutageRetriesAndPublishesAfterRecovery() throws Exception {
        String eventKey = "outage-" + System.nanoTime();
        EventId eventId = publish(eventKey, new TaskPayload(103));

        environment.pauseBroker();
        try {
            assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isZero();
        } finally {
            environment.unpauseBroker();
        }

        Instant nextAttemptAt = nextAttemptAtOf(eventId);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(nextAttemptAt).isAfter(Instant.now().minusSeconds(1));

        assertThat(worker(sender, Clock.fixed(nextAttemptAt, ZoneOffset.UTC))
                .publishDueEvents()).isOne();

        MessageView message = receiveMessages(eventKey, 1).get(0);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        consumer.ack(message);
    }

    @Test
    void resultUnknownAfterRealSendProducesAnExpectedDuplicate() throws Exception {
        String eventKey = "unknown-" + System.nanoTime();
        EventId eventId = publish(eventKey, new TaskPayload(104));
        AtomicBoolean firstCall = new AtomicBoolean(true);
        AtomicReference<String> firstMessageId = new AtomicReference<>();
        EventTransport discardFirstReceipt = event -> {
            TransportReceipt receipt = sender.send(event);
            if (firstCall.getAndSet(false)) {
                firstMessageId.set(receipt.brokerMessageId().orElse(null));
                throw new TransportException(TransportFailureType.RESULT_UNKNOWN,
                        "Test discarded a successful RocketMQ receipt"
                );
            }
            return receipt;
        };

        assertThat(worker(discardFirstReceipt, Clock.systemUTC()).publishDueEvents()).isZero();

        Instant nextAttemptAt = nextAttemptAtOf(eventId);
        assertThat(firstMessageId.get()).isNotBlank();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();

        assertThat(worker(sender, Clock.fixed(nextAttemptAt, ZoneOffset.UTC))
                .publishDueEvents()).isOne();

        List<MessageView> messages = receiveMessages(eventKey, 2);
        Set<String> messageIds = new HashSet<>();
        for (MessageView message : messages) {
            messageIds.add(message.getMessageId().toString());
            assertThat(message.getTopic()).isEqualTo(RocketMqTestEnvironment.TOPIC);
            assertThat(message.getTag()).contains(RocketMqTestEnvironment.TAG);
            assertThat(message.getKeys()).contains(eventKey);
            assertThat(OBJECT_MAPPER.readTree(readBody(message.getBody()))
                    .path("taskId").asLong()).isEqualTo(104L);
            assertThat(message.getProperties())
                    .containsEntry(
                            RocketMqMessageFactory.EVENT_ID_PROPERTY,
                            Long.toString(eventId.value())
                    );
            consumer.ack(message);
        }
        assertThat(messageIds).hasSize(2).contains(firstMessageId.get());
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
    }

    @Test
    void futureEventIsNotSentBeforeItsAvailabilityTime() throws Exception {
        String eventKey = "future-" + System.nanoTime();
        Instant availableAt = Instant.now().plusSeconds(2);
        EventId eventId = publishAt(eventKey, new TaskPayload(105), availableAt, 3);

        assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isZero();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PENDING.code());
        assertThat(attemptCountOf(eventId)).isZero();
        assertThat(countMessages(eventKey)).isZero();

        await().atMost(Duration.ofSeconds(5))
                .until(() -> !Instant.now().isBefore(availableAt));
        assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isOne();
        MessageView message = receiveMessages(eventKey, 1).get(0);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(message.getProperties()).containsEntry(
                RocketMqMessageFactory.EVENT_ID_PROPERTY, Long.toString(eventId.value()));
        consumer.ack(message);
    }

    @Test
    void pausedBrokerExhaustsRealRetriesAndRecordsPossibleLateAcceptances() throws Exception {
        String eventKey = "exhausted-" + System.nanoTime();
        EventId eventId = publishWithMaxAttempts(eventKey, 2);
        Path evidence = Path.of("target", "evidence", "m9.4-rocketmq-retry-exhaustion");
        Files.createDirectories(evidence);
        Path timeline = evidence.resolve(eventKey + ".timeline");
        environment.pauseBroker();
        int firstAttempt = 0;
        try {
            assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isZero();
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
            firstAttempt = attemptCountOf(eventId);
            jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                    eventId.value());
            assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isZero();

            assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
            assertThat(attemptCountOf(eventId)).isEqualTo(2);
            assertThat(firstAttempt).isOne();
        } finally {
            environment.unpauseBroker();
        }
        List<MessageView> possiblyAccepted = receiveAvailableMessages(eventKey);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        Files.writeString(timeline, "broker=RocketMQ 5.5.0 paused during both real Producer attempts\n" +
                "firstAttemptCount=" + firstAttempt + "\nsecondAttemptCount=" + attemptCountOf(eventId) +
                "\nsecondAttemptState=DEAD\nnextAttemptAdvance=SQL test acceleration\n" +
                "possibleLateBrokerAcceptances=" + possiblyAccepted.size() + "\n" +
                "acceptedMessageIds=" + messageIds(possiblyAccepted) + "\n" +
                "postUnpauseOutboxState=DEAD; no further application retry\n");
    }

    @Test
    void independentJvmCrashBoundariesAreRecoveredAgainstRealRocketMq() throws Exception {
        verifyIndependentJvmCrash("claim-before-send", 1);
        verifyIndependentJvmCrash("confirmed-before-state-commit", 2);
    }

    @Test
    void lateRealBrokerConfirmationCannotCommitAnExpiredLease() throws Exception {
        String eventKey = "late-real-confirmation-" + System.nanoTime();
        EventId eventId = publish(eventKey, new TaskPayload(109));
        CountDownLatch brokerConfirmed = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        EventTransport delayedRealConfirmation = event -> {
            TransportReceipt receipt = sender.send(event);
            brokerConfirmed.countDown();
            try {
                if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("parent did not release late-confirmation worker");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("late-confirmation worker interrupted", interrupted);
            }
            return receipt;
        };
        var executor = Executors.newSingleThreadExecutor();
        Path evidence = Path.of("target", "evidence", "m9.4-rocketmq-late-confirmation");
        Files.createDirectories(evidence);
        try {
            JdbcEventPublicationWorker delayedWorker = worker(delayedRealConfirmation, Clock.systemUTC());
            Future<Integer> result = executor.submit(delayedWorker::publishDueEvents);
            assertThat(brokerConfirmed.await(30, TimeUnit.SECONDS)).isTrue();
            MessageView first = receiveMessages(eventKey, 1).get(0);
            assertThat(first.getProperties()).containsEntry(
                    RocketMqMessageFactory.EVENT_ID_PROPERTY, Long.toString(eventId.value()));
            consumer.ack(first);

            jdbcTemplate.update("UPDATE reliable_event_outbox SET lease_until=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                    eventId.value());
            JdbcExpiredLeaseRecovery recovery = new JdbcExpiredLeaseRecovery(jdbcTemplate,
                    transactionManager, 8,
                    new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0));
            assertThat(recovery.recoverExpiredLeases()).isOne();
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
            releaseWorker.countDown();
            assertThatThrownBy(() -> result.get(20, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(dev.reliableevent.jdbc.internal.persistence.StaleEventClaimException.class);
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());

            jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                    eventId.value());
            assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isOne();
            MessageView retried = receiveMessages(eventKey, 1).get(0);
            assertThat(retried.getProperties()).containsEntry(
                    RocketMqMessageFactory.EVENT_ID_PROPERTY, Long.toString(eventId.value()));
            consumer.ack(retried);
            assertThat(attemptCountOf(eventId)).isEqualTo(2);
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
            Files.writeString(evidence.resolve(eventKey + ".timeline"),
                    "firstBrokerSend=confirmed\nfirstRealMessage=1\n" +
                            "leaseExpiry=SQL test injection, not elapsed production lease time\n" +
                            "recovery=RETRY_WAIT\nlateConfirmation=real receipt after lease recovery\n" +
                            "lateStateCommit=stale claim rejected\nsecondRealMessage=1\n" +
                            "sameEventId=" + eventId.value() + "\nfinalStatus=PUBLISHED\n");
        } finally {
            releaseWorker.countDown();
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void realBrokerConfirmationSurvivesRejectedPublishedStateUpdateAndRetriesWithSameIdentity()
            throws Exception {
        String eventKey = "published-state-rejected-" + System.nanoTime();
        EventId eventId = publish(eventKey, new TaskPayload(110));
        String constraint = "reject_published_" + Math.abs(eventId.value());
        AtomicInteger sendSuccesses = new AtomicInteger();
        AtomicInteger stateFailures = new AtomicInteger();
        AtomicInteger committedStates = new AtomicInteger();
        MessageView first;
        PublicationObserver observer = new PublicationObserver() {
            @Override public void sendSucceeded(dev.reliableevent.jdbc.internal.model.ClaimedEvent event,
                                                TransportReceipt receipt, long elapsedNanos, Instant at) {
                sendSuccesses.incrementAndGet();
            }
            @Override public void stateUpdateFailed(String status, boolean ownershipRejected) {
                stateFailures.incrementAndGet();
            }
            @Override public void stateTransitionCommitted(String status) {
                committedStates.incrementAndGet();
            }
        };
        jdbcTemplate.execute("ALTER TABLE reliable_event_outbox ADD CONSTRAINT " + constraint +
                " CHECK (event_key <> '" + eventKey + "' OR status <> 2)");
        try {
            JdbcEventPublicationWorker failingStateWorker = new JdbcEventPublicationWorker(
                    jdbcTemplate, transactionManager, sender, Clock.systemUTC(), 8,
                    "rocketmq-state-reject-worker", LEASE_DURATION,
                    new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0),
                    observer);
            assertThatThrownBy(failingStateWorker::publishDueEvents)
                    .isInstanceOf(RuntimeException.class);
            assertThat(sendSuccesses).hasValue(1);
            assertThat(stateFailures).hasValue(1);
            assertThat(committedStates).hasValue(0);
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
            assertThat(attemptCountOf(eventId)).isOne();
            first = receiveMessages(eventKey, 1).get(0);
            assertThat(first.getProperties()).containsEntry(
                    RocketMqMessageFactory.EVENT_ID_PROPERTY, Long.toString(eventId.value()));
            consumer.ack(first);
        } finally {
            jdbcTemplate.execute("ALTER TABLE reliable_event_outbox DROP CHECK " + constraint);
        }

        jdbcTemplate.update("UPDATE reliable_event_outbox SET lease_until=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                eventId.value());
        JdbcExpiredLeaseRecovery recovery = new JdbcExpiredLeaseRecovery(jdbcTemplate,
                transactionManager, 8,
                new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0));
        assertThat(recovery.recoverExpiredLeases()).isOne();
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                eventId.value());
        assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isOne();
        MessageView second = receiveMessages(eventKey, 1).get(0);
        assertThat(second.getProperties()).containsEntry(
                RocketMqMessageFactory.EVENT_ID_PROPERTY, Long.toString(eventId.value()));
        assertThat(second.getMessageId()).isNotEqualTo(first.getMessageId());
        consumer.ack(second);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);

        Path evidence = Path.of("target", "evidence", "m9.4-rocketmq-state-update-failure");
        Files.createDirectories(evidence);
        Files.writeString(evidence.resolve(eventKey + ".timeline"),
                "firstBrokerConfirmation=real\nfirstOutboxStatus=PUBLISHING\n" +
                        "stateUpdateFailure=MySQL CHECK constraint rejects PUBLISHED for this event id\n" +
                        "stateCommitCallbacks=" + committedStates.get() + "\n" +
                        "stateUpdateFailedCallbacks=" + stateFailures.get() + "\n" +
                        "firstBrokerMessageId=" + first.getMessageId() + "\n" +
                        "leaseExpiry=SQL test injection, not elapsed production lease time\n" +
                        "recovery=RETRY_WAIT\nsecondBrokerMessageId=" + second.getMessageId() + "\n" +
                        "realMessages=2\nstableEventId=" + eventId.value() +
                        "\nfinalStatus=PUBLISHED\n");
    }

    @Test
    void twoIndependentJvmsRaceForOneSharedOutboxClaim() throws Exception {
        String eventKey = "two-jvm-race-" + System.nanoTime();
        EventId eventId = publish(eventKey, new TaskPayload(108));
        Path evidence = Path.of("target", "evidence", "m9.4-rocketmq-independent-jvm");
        Files.createDirectories(evidence);
        Path gate = evidence.resolve(eventKey + ".gate");
        Path firstReady = evidence.resolve(eventKey + ".first.ready");
        Path secondReady = evidence.resolve(eventKey + ".second.ready");
        Path firstLog = evidence.resolve(eventKey + ".first.log");
        Path secondLog = evidence.resolve(eventKey + ".second.log");
        String classpath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path"));
        Process first = competingChild(classpath, eventId, gate, firstReady, firstLog);
        Process second = null;
        long firstPid = first.pid();
        try {
            second = competingChild(classpath, eventId, gate, secondReady, secondLog);
            long secondPid = second.pid();
            await("both independent JVMs scan the same due candidate")
                    .atMost(Duration.ofSeconds(45))
                    .until(() -> scanned(firstReady) && scanned(secondReady));
            Files.writeString(gate, "release-both-claimers\n");
            await("one JVM wins and the other loses the atomic claim")
                    .atMost(Duration.ofSeconds(30))
                    .until(() -> outcome(firstReady) != null && outcome(secondReady) != null);

            Path winnerReady = outcome(firstReady).equals("won") ? firstReady : secondReady;
            Process winner = outcome(firstReady).equals("won") ? first : second;
            Process loser = winner == first ? second : first;
            assertThat(outcome(firstReady)).isNotEqualTo(outcome(secondReady));
            assertThat(loser.waitFor(15, TimeUnit.SECONDS)).isTrue();
            winner.destroyForcibly();
            assertThat(winner.waitFor(15, TimeUnit.SECONDS)).isTrue();
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
            String winnerOwner = Files.readString(winnerReady).lines()
                    .filter(line -> line.startsWith("owner="))
                    .map(line -> line.substring("owner=".length())).findFirst().orElseThrow();
            assertThat(jdbcTemplate.queryForObject("SELECT lease_owner FROM reliable_event_outbox WHERE id=?",
                    String.class, eventId.value())).isEqualTo(winnerOwner);

            jdbcTemplate.update("UPDATE reliable_event_outbox SET lease_until=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                    eventId.value());
            JdbcExpiredLeaseRecovery recovery = new JdbcExpiredLeaseRecovery(jdbcTemplate,
                    transactionManager, 8,
                    new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0));
            assertThat(recovery.recoverExpiredLeases()).isOne();
            jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                    eventId.value());
            assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isOne();
            MessageView message = receiveMessages(eventKey, 1).get(0);
            consumer.ack(message);
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
            Files.writeString(evidence.resolve(eventKey + ".timeline"),
                    "firstPid=" + firstPid + "\nsecondPid=" + secondPid + "\n" +
                            "firstOutcome=" + outcome(firstReady) + "\nsecondOutcome=" + outcome(secondReady) +
                            "\nloserExit=" + loser.exitValue() + "\nwinnerForcedExit=" + winner.exitValue() +
                            "\nleaseOwner=" + winnerOwner + "\nnewInstancePublished=true\n" +
                            "realRocketMqMessages=1\nfinalStatus=PUBLISHED\n" +
                            "leaseExpiry=SQL test injection, not elapsed production lease time\n" +
                            "nextAttemptAdvance=SQL test acceleration\n");
        } finally {
            stopOwnedChild(first);
            if (second != null) stopOwnedChild(second);
        }
    }

    private Process competingChild(String classpath, EventId eventId, Path gate,
                                   Path ready, Path log) throws Exception {
        Process child = new ProcessBuilder(
                javaExecutable(),
                "-cp", classpath, RocketMqCrashChild.class.getName(), "compete",
                mysqlJdbcUrlUtc(), MYSQL.getUsername(), MYSQL.getPassword(),
                Long.toString(eventId.value()), Integer.toString(RocketMqTestEnvironment.PROXY_GRPC_PORT),
                ready.toAbsolutePath().toString(), gate.toAbsolutePath().toString()
        ).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        return child;
    }

    private String outcome(Path marker) throws Exception {
        if (!Files.exists(marker)) return null;
        return Files.readString(marker).lines()
                .filter(line -> line.startsWith("outcome="))
                .map(line -> line.substring("outcome=".length())).findFirst().orElse(null);
    }

    private boolean scanned(Path marker) throws Exception {
        return Files.exists(marker) && Files.readString(marker).contains("scanned=true");
    }

    private void stopOwnedChild(Process child) throws InterruptedException {
        if (child.isAlive()) child.destroyForcibly();
        if (child.isAlive()) child.waitFor(15, TimeUnit.SECONDS);
    }

    private static String javaExecutable() {
        String executable = System.getProperty("os.name", "").toLowerCase().contains("win")
                ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", executable).toString();
    }

    private static String mysqlJdbcUrlUtc() {
        String url = MYSQL.getJdbcUrl();
        return url + (url.contains("?") ? "&" : "?")
                + "connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true";
    }

    private void verifyIndependentJvmCrash(String mode, int expectedMessages) throws Exception {
        String eventKey = mode + "-" + System.nanoTime();
        EventId eventId = publish(eventKey, new TaskPayload(106));
        Path evidence = Path.of("target", "evidence", "m9.4-rocketmq-independent-jvm");
        Files.createDirectories(evidence);
        Path output = evidence.resolve(eventKey + ".log");
        Path ready = evidence.resolve(eventKey + ".ready");
        String classpath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(
                javaExecutable(),
                "-cp", classpath,
                RocketMqCrashChild.class.getName(), mode,
                mysqlJdbcUrlUtc(), MYSQL.getUsername(), MYSQL.getPassword(),
                Long.toString(eventId.value()), Integer.toString(RocketMqTestEnvironment.PROXY_GRPC_PORT),
                ready.toAbsolutePath().toString()
        ).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        long childPid = child.pid();
        try {
            await("child reaches requested crash boundary")
                    .atMost(Duration.ofSeconds(45))
                    .until(() -> completeBoundary(ready, mode));
            String boundary = Files.readString(ready);
            int priorRealMessages = 0;
            if (mode.equals("confirmed-before-state-commit")) {
                MessageView firstDelivery = receiveMessages(eventKey, 1).get(0);
                priorRealMessages = 1;
                assertThat(firstDelivery.getProperties()).containsEntry(
                        RocketMqMessageFactory.EVENT_ID_PROPERTY, Long.toString(eventId.value()));
                assertThat(OBJECT_MAPPER.readTree(readBody(firstDelivery.getBody())).path("taskId").asLong())
                        .isEqualTo(106L);
                consumer.ack(firstDelivery);
            }
            child.destroyForcibly();
            assertThat(child.waitFor(15, TimeUnit.SECONDS)).as("forcibly stopped child exits").isTrue();
            int childExit = child.exitValue();
            Files.writeString(evidence.resolve(eventKey + ".exitcode"), "pid=" + childPid + "\nexitCode=" + childExit);
            assertThat(childExit).isNotZero();
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());

            jdbcTemplate.update("UPDATE reliable_event_outbox SET lease_until=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                    eventId.value());
            JdbcExpiredLeaseRecovery recovery = new JdbcExpiredLeaseRecovery(
                    jdbcTemplate, transactionManager, 16,
                    new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0));
            assertThat(recovery.recoverExpiredLeases()).isOne();
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
            jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND WHERE id=?",
                    eventId.value());
            assertThat(worker(sender, Clock.systemUTC()).publishDueEvents()).isOne();

            List<MessageView> messages = receiveMessages(eventKey, expectedMessages - priorRealMessages);
            Set<String> stableEventIds = new HashSet<>();
            for (MessageView message : messages) {
                stableEventIds.add(message.getProperties().get(RocketMqMessageFactory.EVENT_ID_PROPERTY));
                assertThat(OBJECT_MAPPER.readTree(readBody(message.getBody())).path("taskId").asLong())
                        .isEqualTo(106L);
                consumer.ack(message);
            }
            assertThat(stableEventIds).containsExactly(Long.toString(eventId.value()));
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
            Files.writeString(evidence.resolve(eventKey + ".timeline"),
                    "childPid=" + childPid + "\nchildExit=" + childExit + "\n" + boundary +
                            "\nstatusAfterCrash=PUBLISHING\n" +
                            "leaseExpiry=SQL test injection, not elapsed production lease time\n" +
                            "nextAttemptAdvance=SQL test acceleration\n" +
                            "recovered=RETRY_WAIT\nrealRocketMqMessages=" + (messages.size() + priorRealMessages) +
                            "\nstableEventId=" + eventId.value() + "\nfinalStatus=PUBLISHED\n");
        } finally {
            stopOwnedChild(child);
        }
    }

    private boolean completeBoundary(Path marker, String mode) throws Exception {
        if (!Files.exists(marker)) return false;
        String contents = Files.readString(marker);
        if (mode.equals("claim-before-send")) {
            return contents.contains("boundary=claimed-before-send")
                    && contents.contains("claimVersion=") && contents.contains("leaseOwner=");
        }
        return contents.contains("boundary=broker-confirmed-before-db-state-commit")
                && contents.contains("messageId=");
    }

    private int countMessages(String eventKey) throws Exception {
        List<MessageView> messages = consumer.receive(16, Duration.ofSeconds(10));
        int matches = 0;
        for (MessageView message : messages) {
            if (message.getKeys().contains(eventKey)) matches++;
            consumer.ack(message);
        }
        return matches;
    }

    private List<MessageView> receiveAvailableMessages(String eventKey) throws Exception {
        List<MessageView> matches = new ArrayList<>();
        for (MessageView message : consumer.receive(16, Duration.ofSeconds(10))) {
            if (message.getKeys().contains(eventKey)) matches.add(message);
            else consumer.ack(message);
        }
        return matches;
    }

    private List<String> messageIds(List<MessageView> messages) throws Exception {
        List<String> ids = new ArrayList<>();
        for (MessageView message : messages) {
            ids.add(message.getMessageId().toString());
            consumer.ack(message);
        }
        return ids;
    }

    private EventId publishAt(String eventKey, Object payload, Instant availableAt, int maxAttempts) {
        return new TransactionTemplate(transactionManager).execute(status ->
                new JdbcReliableEventPublisher(jdbcTemplate, OBJECT_MAPPER, maxAttempts).publish(
                        new ReliableEvent<>("coupon-task-execute", eventKey, payload, availableAt,
                                Map.of("source", "publication-integration-test"))));
    }

    private EventId publishWithMaxAttempts(String eventKey, int maxAttempts) {
        return publishAt(eventKey, new TaskPayload(107), Instant.now().minusSeconds(1), maxAttempts);
    }

    private static RocketMqEventTransport sender(Producer targetProducer, int maxBodyBytes) {
        return new RocketMqEventTransport(PROVIDER, targetProducer, new MapEventDestinationResolver(Map.of(
                        "coupon-task-execute",
                        new RocketMqDestination(
                                RocketMqTestEnvironment.TOPIC,
                                RocketMqTestEnvironment.TAG
                        )
                )), maxBodyBytes);
    }

    private EventId publish(String eventKey, Object payload) {
        JdbcReliableEventPublisher publisher = new JdbcReliableEventPublisher(
                jdbcTemplate,
                OBJECT_MAPPER
        );
        return new TransactionTemplate(transactionManager).execute(status -> publisher.publish(
                new ReliableEvent<>(
                        "coupon-task-execute",
                        eventKey,
                        payload,
                        Instant.now().minusSeconds(1),
                        Map.of("source", "publication-integration-test")
                )
        ));
    }

    private JdbcEventPublicationWorker worker(EventTransport targetSender, Clock clock) {
        return new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                targetSender,
                clock,
                10,
                "rocketmq-publication-worker",
                LEASE_DURATION,
                new ExponentialBackoff(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(30),
                        0.0,
                        () -> 0.0
                )
        );
    }

    private List<MessageView> receiveMessages(String eventKey, int expectedCount) throws Exception {
        List<MessageView> matches = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (matches.size() < expectedCount && System.nanoTime() < deadline) {
            List<MessageView> batch = consumer.receive(16, Duration.ofSeconds(15));
            for (MessageView message : batch) {
                if (message.getKeys().contains(eventKey)) {
                    matches.add(message);
                } else {
                    consumer.ack(message);
                }
            }
        }
        assertThat(matches).hasSize(expectedCount);
        return matches;
    }

    private int statusOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM reliable_event_outbox WHERE id = ?",
                Integer.class,
                eventId.value()
        );
    }

    private int attemptCountOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM reliable_event_outbox WHERE id = ?",
                Integer.class,
                eventId.value()
        );
    }

    private Instant nextAttemptAtOf(EventId eventId) {
        Timestamp value = jdbcTemplate.queryForObject(
                "SELECT next_attempt_at FROM reliable_event_outbox WHERE id = ?",
                Timestamp.class,
                eventId.value()
        );
        return value.toInstant();
    }

    private String lastErrorOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT last_error FROM reliable_event_outbox WHERE id = ?",
                String.class,
                eventId.value()
        );
    }

    private String readBody(ByteBuffer body) {
        byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private record TaskPayload(long taskId) {
    }

    private record LargePayload(String value) {
    }
}
