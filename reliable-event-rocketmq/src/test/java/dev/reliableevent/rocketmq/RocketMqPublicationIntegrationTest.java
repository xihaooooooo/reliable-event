package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.JdbcReliableEventPublisher;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
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

import static org.assertj.core.api.Assertions.assertThat;
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
    private static RocketMqEventSender sender;
    private static JdbcTemplate jdbcTemplate;
    private static DataSourceTransactionManager transactionManager;

    @BeforeAll
    static void startDependencies() throws Exception {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(MYSQL.getDriverClassName());
        dataSource.setUrl(MYSQL.getJdbcUrl());
        dataSource.setUsername(MYSQL.getUsername());
        dataSource.setPassword(MYSQL.getPassword());
        jdbcTemplate = new JdbcTemplate(dataSource);
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
    void missingDestinationBecomesDeadWithoutCallingTheProducer() throws Exception {
        Producer unusedProducer = mock(Producer.class);
        RocketMqEventSender missingDestinationSender = new RocketMqEventSender(
                PROVIDER,
                unusedProducer,
                new MapEventDestinationResolver(Map.of()),
                OBJECT_MAPPER
        );
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
        RocketMqEventSender smallBodySender = sender(unusedProducer, 32);
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
        EventSender discardFirstReceipt = event -> {
            SendReceipt receipt = sender.send(event);
            if (firstCall.getAndSet(false)) {
                firstMessageId.set(receipt.messageId());
                throw EventSendException.resultUnknown(
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

    private static RocketMqEventSender sender(Producer targetProducer, int maxBodyBytes) {
        return new RocketMqEventSender(
                PROVIDER,
                targetProducer,
                new MapEventDestinationResolver(Map.of(
                        "coupon-task-execute",
                        new RocketMqDestination(
                                RocketMqTestEnvironment.TOPIC,
                                RocketMqTestEnvironment.TAG
                        )
                )),
                OBJECT_MAPPER,
                maxBodyBytes
        );
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

    private JdbcEventPublicationWorker worker(EventSender targetSender, Clock clock) {
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
