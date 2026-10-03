package dev.reliableevent.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.DeadEventDetails;
import dev.reliableevent.jdbc.DeadEventLookup;
import dev.reliableevent.jdbc.DeadEventOperations;
import dev.reliableevent.jdbc.DeadEventReplayRequest;
import dev.reliableevent.jdbc.DeadEventReplayResult;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.rocketmq.MapEventDestinationResolver;
import dev.reliableevent.rocketmq.RocketMqEventSender;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.springframework.boot.actuate.autoconfigure.tracing.SdkTracerProviderBuilderCustomizer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers(disabledWithoutDocker = true)
@Timeout(180)
class ExampleApplicationEndToEndTest {

    private static final String TOPIC = "reliable-event-example-test";
    private static final String GROUP = "reliable-event-example-app-test";
    private static final String PROBE_GROUP = "reliable-event-example-probe-test";
    private static final String ROCKET_HOME = "/home/rocketmq/rocketmq-5.5.0";
    private static final String BROKER_CONFIG_PATH = ROCKET_HOME + "/conf/example-test.conf";
    private static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("reliable_event_example_e2e")
            .withUsername("test")
            .withPassword("test");

    private static Network network;
    private static GenericContainer<?> nameserver;
    private static FixedPortContainer broker;

    private ConfigurableApplicationContext context;
    private JdbcTemplate jdbc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void startRocketMq() throws Exception {
        DockerImageName image = DockerImageName.parse("apache/rocketmq:5.5.0");
        network = Network.newNetwork();
        nameserver = new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases("nameserver")
                .withCommand("sh", "mqnamesrv")
                .waitingFor(Wait.forLogMessage(".*Name Server boot success.*\\n", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        broker = new FixedPortContainer(image)
                .withNetwork(network)
                .withNetworkAliases("broker")
                .withEnv("NAMESRV_ADDR", "nameserver:9876")
                .withCopyToContainer(Transferable.of("""
                        brokerClusterName=DefaultCluster
                        brokerName=broker-a
                        brokerId=0
                        deleteWhen=04
                        fileReservedTime=1
                        brokerRole=ASYNC_MASTER
                        flushDiskType=ASYNC_FLUSH
                        brokerIP1=127.0.0.1
                        autoCreateTopicEnable=false
                        """, 0644), BROKER_CONFIG_PATH)
                .withCommand("sh", "mqbroker", "--enable-proxy", "-c", BROKER_CONFIG_PATH)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
        broker.bindFixedPort(8081, 8081);
        nameserver.start();
        broker.start();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250))
                .until(() -> command("clusterList -n nameserver:9876").getStdout().contains("broker-a"));
        assertThat(command("updateTopic -n nameserver:9876 -c DefaultCluster -t " + TOPIC)
                .getExitCode()).isZero();
        for (String group : List.of(GROUP, PROBE_GROUP)) {
            assertThat(command("updateSubGroup -n nameserver:9876 -c DefaultCluster -g " + group)
                    .getExitCode()).isZero();
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250))
                .until(() -> command("topicRoute -n nameserver:9876 -t " + TOPIC)
                        .getStdout().contains("broker-a"));
    }

    @AfterAll
    static void stopRocketMq() {
        if (broker != null) { broker.stop(); }
        if (nameserver != null) { nameserver.stop(); }
        if (network != null) { network.close(); }
    }

    @BeforeEach
    void createTables() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
        new ResourceDatabasePopulator(
                new ClassPathResource("schema/reliable-event-outbox.sql"),
                new ClassPathResource("schema/reliable-event-replay-audit-m6-2.sql"),
                new ClassPathResource("schema/example-tables.sql")
        ).execute(dataSource);
        jdbc = new JdbcTemplate(dataSource);
    }

    @AfterEach
    void closeContext() {
        if (context != null) { context.close(); }
    }

    @Test
    void httpOrderIsAutomaticallyPublishedAndConsumed() throws Exception {
        startApplication(true);
        Created created = postOrder("notebook", 2);

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    assertThat(status(created.eventId())).isEqualTo(2);
                    assertThat(effectCount(created.orderId())).isOne();
                });
        assertThat(consumedCount(created.orderId())).isOne();
        assertThat(getOrder(created.orderId()).path("handledCount").asInt()).isOne();
    }

    @Test
    void realHttpRegistrationPublicationAndBrokerConsumptionShareOneTrace() throws Exception {
        startApplication(true);
        MemorySpanExporter.reset();
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints("localhost:8081")
                .setRequestTimeout(Duration.ofSeconds(10))
                .enableSsl(false)
                .build();
        try (SimpleConsumer probe = PROVIDER.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(PROBE_GROUP)
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(Map.of(TOPIC, FilterExpression.SUB_ALL))
                .build()) {
            String upstream = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
            Created created = postOrder("trace-order", 7, upstream);
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> {
                        assertThat(status(created.eventId())).isEqualTo(2);
                        assertThat(effectCount(created.orderId())).isOne();
                    });
            MessageView brokerMessage = receiveFor(probe, Long.toString(created.orderId()), 1).get(0);
            String deliveredParent = brokerMessage.getProperties().get("traceparent");
            assertThat(deliveredParent).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");

            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                    .untilAsserted(() -> {
                        List<SpanData> spans = MemorySpanExporter.finishedSpans();
                        SpanData server = spans.stream()
                                .filter(span -> span.getKind() == SpanKind.SERVER)
                                .filter(span -> span.getTraceId().equals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
                                .findFirst().orElseThrow();
                        SpanData registration = named(spans, "reliable-event.register",
                                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                        SpanData publication = named(spans, "reliable-event.publish",
                                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                        SpanData consumption = named(spans, "example.order.consume",
                                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                        assertThat(server.getParentSpanId()).isEqualTo("bbbbbbbbbbbbbbbb");
                        assertThat(registration.getTraceId()).isEqualTo(server.getTraceId());
                        assertThat(registration.getParentSpanId()).isEqualTo(server.getSpanId());
                        assertThat(publication.getTraceId()).isEqualTo(registration.getTraceId());
                        assertThat(publication.getParentSpanId()).isEqualTo(registration.getSpanId());
                        assertThat(deliveredParent.split("-")[2]).isEqualTo(publication.getSpanId());
                        assertThat(consumption.getTraceId()).isEqualTo(publication.getTraceId());
                        assertThat(consumption.getParentSpanId()).isEqualTo(publication.getSpanId());
                        assertThat(consumption.getAttributes().get(
                                AttributeKey.stringKey("reliable_event.consume.result"))).isEqualTo("processed");
                        assertThat(consumption.getAttributes().get(
                                AttributeKey.stringKey("reliable_event.ack.result"))).isEqualTo("success");
                    });
            assertThat(consumedCount(created.orderId())).isOne();
        }
    }

    @Test
    void consumerSeparatesBusinessFailureDuplicateSkipAndAckFailure() throws Exception {
        startApplication(false, false, 8, "--example.consumer.enabled=false");
        MemorySpanExporter.reset();
        Created created = postOrder("consumer-results", 2);
        ObjectMapper appMapper = context.getBean(ObjectMapper.class);
        MessageView invalid = exampleMessage(created,
                new OrderCreatedPayload(created.orderId(), "wrong-item", 2), appMapper);
        MessageView valid = exampleMessage(created,
                new OrderCreatedPayload(created.orderId(), "consumer-results", 2), appMapper);
        SimpleConsumer fakeConsumer = mock(SimpleConsumer.class);
        AtomicInteger receiveCalls = new AtomicInteger();
        AtomicInteger acknowledgements = new AtomicInteger();
        CountDownLatch attemptedAcks = new CountDownLatch(2);
        when(fakeConsumer.receive(anyInt(), any(Duration.class))).thenAnswer(invocation -> {
            if (receiveCalls.getAndIncrement() == 0) return List.of(invalid, valid, valid);
            new CountDownLatch(1).await();
            return List.of();
        });
        doAnswer(invocation -> {
            attemptedAcks.countDown();
            if (acknowledgements.incrementAndGet() == 1) {
                throw new IllegalStateException("simulated acknowledgement failure");
            }
            return null;
        }).when(fakeConsumer).ack(any(MessageView.class));

        ExampleConsumerLoop loop = new ExampleConsumerLoop(fakeConsumer,
                context.getBean(OrderMessageHandler.class), context.getBean(io.micrometer.tracing.Tracer.class),
                context.getBean(io.micrometer.tracing.propagation.Propagator.class));
        loop.start();
        assertThat(attemptedAcks.await(10, TimeUnit.SECONDS)).isTrue();
        loop.stop();

        assertThat(acknowledgements).hasValue(2);
        assertThat(consumedCount(created.orderId())).isOne();
        assertThat(effectCount(created.orderId())).isOne();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<SpanData> spans = MemorySpanExporter.finishedSpans().stream()
                    .filter(span -> span.getName().equals("example.order.consume")).toList();
            assertThat(spans).hasSize(3);
            assertThat(spans).anySatisfy(span -> assertThat(span.getAttributes().get(
                    AttributeKey.stringKey("reliable_event.consume.result"))).isEqualTo("failed"));
            assertThat(spans).anySatisfy(span -> {
                assertThat(span.getAttributes().get(AttributeKey.stringKey(
                        "reliable_event.consume.result"))).isEqualTo("processed");
                assertThat(span.getAttributes().get(AttributeKey.stringKey(
                        "reliable_event.ack.result"))).isEqualTo("failed");
            });
            assertThat(spans).anySatisfy(span -> {
                assertThat(span.getAttributes().get(AttributeKey.stringKey(
                        "reliable_event.consume.result"))).isEqualTo("idempotent_skip");
                assertThat(span.getAttributes().get(AttributeKey.stringKey(
                        "reliable_event.ack.result"))).isEqualTo("success");
            });
            assertThat(spans).allSatisfy(span -> assertThat(span.getParentSpanId())
                    .isEqualTo("0000000000000000"));
        });
    }

    @Test
    void lostSuccessfulReceiptProducesTwoBrokerMessagesButOneBusinessEffect() throws Exception {
        startApplication(false);
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints("localhost:8081")
                .setRequestTimeout(Duration.ofSeconds(10))
                .enableSsl(false)
                .build();
        try (SimpleConsumer probe = PROVIDER.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(PROBE_GROUP)
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(Map.of(TOPIC, FilterExpression.SUB_ALL))
                .build()) {
            Created created = postOrder("pencil", 4);
            EventSender realSender = context.getBean(EventSender.class);
            AtomicBoolean first = new AtomicBoolean(true);
            EventSender receiptLost = event -> {
                var receipt = realSender.send(event);
                if (first.getAndSet(false)) {
                    throw EventSendException.resultUnknown("Test discarded a successful Broker receipt");
                }
                return receipt;
            };
            worker(receiptLost, Clock.systemUTC()).publishDueEvents();
            assertThat(status(created.eventId())).isEqualTo(3);

            Instant retryAt = jdbc.queryForObject(
                    "SELECT next_attempt_at FROM reliable_event_outbox WHERE id = ?",
                    (result, row) -> result.getTimestamp(1).toInstant(), created.eventId());
            worker(realSender, Clock.fixed(retryAt, ZoneOffset.UTC)).publishDueEvents();
            assertThat(status(created.eventId())).isEqualTo(2);

            List<MessageView> received = receiveFor(probe, Long.toString(created.orderId()), 2);
            Set<String> messageIds = new HashSet<>();
            for (MessageView message : received) {
                messageIds.add(message.getMessageId().toString());
                assertThat(message.getTopic()).isEqualTo(TOPIC);
                assertThat(message.getTag()).contains("created");
                assertThat(message.getKeys()).contains(Long.toString(created.orderId()));
                assertThat(message.getProperties())
                        .containsEntry("reliable_event_id", Long.toString(created.eventId()))
                        .containsEntry("reliable_event_type", OrderService.EVENT_TYPE)
                        .containsEntry("reliable_event_key", Long.toString(created.orderId()));
                ByteBuffer body = message.getBody().asReadOnlyBuffer();
                byte[] bytes = new byte[body.remaining()];
                body.get(bytes);
                JsonNode payload = mapper.readTree(bytes);
                assertThat(payload.path("orderId").asLong()).isEqualTo(created.orderId());
                assertThat(payload.path("itemCode").asText()).isEqualTo("pencil");
                assertThat(payload.path("quantity").asInt()).isEqualTo(4);
            }
            assertThat(messageIds).hasSize(2);
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> assertThat(effectCount(created.orderId())).isOne());
            assertThat(consumedCount(created.orderId())).isOne();
        }
    }

    @Test
    void twoOperatorsCompeteToReplayADeadEventAfterRepairingItsDestination() throws Exception {
        startApplication(false, true, 1);
        Created created = postOrder("eraser", 3);
        EventId eventId = new EventId(created.eventId());
        EventSender missingDestination = new RocketMqEventSender(
                PROVIDER, context.getBean(Producer.class),
                new MapEventDestinationResolver(Map.of()), mapper);
        assertThat(worker(missingDestination, Clock.systemUTC()).publishDueEvents()).isZero();
        assertThat(status(created.eventId())).isEqualTo(4);

        DeadEventOperations operations = context.getBean(DeadEventOperations.class);
        assertThat(operations.firstPage(50).events()).anySatisfy(
                summary -> assertThat(summary.id()).isEqualTo(eventId));
        DeadEventDetails inspected = deadDetails(operations, eventId);
        assertThat(inspected.lastError()).contains("No RocketMQ destination");
        long deadVersion = inspected.version();

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var operators = Executors.newFixedThreadPool(2);
        try {
            Future<DeadEventReplayResult> first = operators.submit(() -> replayAfterSignal(
                    operations, inspected, "operator-a", ready, start));
            Future<DeadEventReplayResult> second = operators.submit(() -> replayAfterSignal(
                    operations, inspected, "operator-b", ready, start));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            List<DeadEventReplayResult> results = List.of(first.get(), second.get());
            assertThat(results.stream().filter(DeadEventReplayResult.Replayed.class::isInstance).count())
                    .isOne();
            assertThat(results.stream().filter(DeadEventReplayResult.NotDead.class::isInstance).count())
                    .isOne();
            DeadEventReplayResult.Replayed replayed = results.stream()
                    .filter(DeadEventReplayResult.Replayed.class::isInstance)
                    .map(DeadEventReplayResult.Replayed.class::cast).findFirst().orElseThrow();
            assertThat(replayed.previousVersion()).isEqualTo(deadVersion);
            assertThat(replayed.newVersion()).isEqualTo(deadVersion + 1);
            assertThat(status(created.eventId())).isZero();
            assertThat(attemptCount(created.eventId())).isZero();
            assertThat(auditCount(created.eventId())).isOne();
            assertThat(jdbc.queryForObject(
                    "SELECT operator_id FROM reliable_event_replay_audit WHERE id = ?",
                    String.class, replayed.auditId())).isIn("operator-a", "operator-b");

            // Restart with the repaired mapping and normal Starter scheduling.
            context.close();
            context = null;
            startApplication(true, true, 1);
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> {
                        assertThat(status(created.eventId())).isEqualTo(2);
                        assertThat(effectCount(created.orderId())).isOne();
                    });
            assertThat(status(created.eventId())).isEqualTo(2);
            assertThat(consumedCount(created.orderId())).isOne();
            assertThat(jdbc.queryForObject("""
                    SELECT a.new_version FROM reliable_event_replay_audit a
                    WHERE a.id = ? AND a.event_id = ? AND a.result = 'REQUEUED'
                    """, Long.class, replayed.auditId(), created.eventId()))
                    .isEqualTo(deadVersion + 1);
        } finally {
            operators.shutdownNow();
        }
    }

    @Test
    void replayAfterUnknownResultKeepsIdentityAndConsumerEffectIdempotent() throws Exception {
        startApplication(false, true, 1);
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints("localhost:8081")
                .setRequestTimeout(Duration.ofSeconds(10))
                .enableSsl(false)
                .build();
        try (SimpleConsumer probe = PROVIDER.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(PROBE_GROUP)
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(Map.of(TOPIC, FilterExpression.SUB_ALL))
                .build()) {
            Created created = postOrder("ruler", 5);
            EventSender realSender = context.getBean(EventSender.class);
            EventSender receiptLost = event -> {
                realSender.send(event);
                throw EventSendException.resultUnknown("Test discarded a successful Broker receipt");
            };
            assertThat(worker(receiptLost, Clock.systemUTC()).publishDueEvents()).isZero();
            assertThat(status(created.eventId())).isEqualTo(4);
            List<MessageView> firstDelivery = receiveFor(probe,
                    Long.toString(created.orderId()), 1);
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> assertThat(effectCount(created.orderId())).isOne());

            DeadEventOperations operations = context.getBean(DeadEventOperations.class);
            DeadEventDetails inspected = deadDetails(operations, new EventId(created.eventId()));
            assertThat(inspected.lastError()).contains("Test discarded a successful Broker receipt");
            var result = operations.replay(new DeadEventReplayRequest(
                    inspected.id(), inspected.version(), "operator-after-check",
                    "Verified Broker delivery and consumer effect before controlled replay"));
            assertThat(result).isInstanceOf(DeadEventReplayResult.Replayed.class);
            DeadEventReplayResult.Replayed replayed = (DeadEventReplayResult.Replayed) result;
            assertThat(auditCount(created.eventId())).isOne();
            assertThat(attemptCount(created.eventId())).isZero();

            assertThat(worker(realSender, Clock.systemUTC()).publishDueEvents()).isOne();
            List<MessageView> secondDelivery = receiveFor(probe,
                    Long.toString(created.orderId()), 1);
            MessageView first = firstDelivery.get(0);
            MessageView second = secondDelivery.get(0);
            assertThat(first.getMessageId().toString()).isNotEqualTo(second.getMessageId().toString());
            for (MessageView message : List.of(first, second)) {
                assertThat(message.getKeys()).contains(Long.toString(created.orderId()));
                assertThat(message.getProperties())
                        .containsEntry("reliable_event_id", Long.toString(created.eventId()))
                        .containsEntry("reliable_event_type", OrderService.EVENT_TYPE)
                        .containsEntry("reliable_event_key", Long.toString(created.orderId()));
            }
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> assertThat(effectCount(created.orderId())).isOne());
            assertThat(consumedCount(created.orderId())).isOne();
            assertThat(status(created.eventId())).isEqualTo(2);
            assertThat(attemptCount(created.eventId())).isOne();
            assertThat(jdbc.queryForObject(
                    "SELECT previous_attempt_count FROM reliable_event_replay_audit WHERE id = ?",
                    Integer.class, replayed.auditId())).isOne();
        }
    }

    @Test
    void publishedRetentionKeepsIdentityAndDoesNotSendAgain() throws Exception {
        startApplication(true, false, 8,
                "--reliable-event.published-retention-enabled=true",
                "--reliable-event.published-retention=1s",
                "--reliable-event.cleanup-interval=100ms",
                "--reliable-event.cleanup-batch-size=10");
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints("localhost:8081")
                .setRequestTimeout(Duration.ofSeconds(10))
                .enableSsl(false)
                .build();
        try (SimpleConsumer probe = PROVIDER.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(PROBE_GROUP)
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(Map.of(TOPIC, FilterExpression.SUB_ALL))
                .build()) {
            Created created = postOrder("retained", 1);
            List<MessageView> first = receiveFor(probe, Long.toString(created.orderId()), 1);
            assertThat(first.get(0).getProperties())
                    .containsEntry("reliable_event_id", Long.toString(created.eventId()));
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> {
                        assertThat(effectCount(created.orderId())).isOne();
                        assertThat(jdbc.queryForObject(
                                "SELECT COUNT(*) FROM reliable_event_outbox WHERE id = ?",
                                Long.class, created.eventId())).isZero();
                    });
            assertThat(jdbc.queryForObject(
                    "SELECT COUNT(*) FROM reliable_event_identity WHERE id = ?",
                    Long.class, created.eventId())).isOne();

            EventId repeated = new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
                    .execute(status -> context.getBean(ReliableEventPublisher.class).publish(
                            new ReliableEvent<>(OrderService.EVENT_TYPE,
                                    Long.toString(created.orderId()),
                                    new OrderCreatedPayload(created.orderId(), "changed", 99),
                                    Instant.now(), Map.of())));
            assertThat(repeated).isEqualTo(new EventId(created.eventId()));
            assertThat(jdbc.queryForObject(
                    "SELECT COUNT(*) FROM reliable_event_outbox WHERE id = ?",
                    Long.class, created.eventId())).isZero();
            assertThat(effectCount(created.orderId())).isOne();
            Created fresh = postOrder("after-cleanup", 2);
            assertThat(fresh.eventId()).isGreaterThan(created.eventId());
            assertThat(receiveFor(probe, Long.toString(fresh.orderId()), 1).get(0).getProperties())
                    .containsEntry("reliable_event_id", Long.toString(fresh.eventId()));
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> assertThat(effectCount(fresh.orderId())).isOne());
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (System.nanoTime() < deadline) {
                for (MessageView message : probe.receive(8, Duration.ofSeconds(15))) {
                    assertThat(message.getKeys()).doesNotContain(Long.toString(created.orderId()));
                    probe.ack(message);
                }
            }
        }
    }

    private DeadEventReplayResult replayAfterSignal(DeadEventOperations operations,
                                                     DeadEventDetails inspected, String operator,
                                                     CountDownLatch ready, CountDownLatch start)
            throws InterruptedException {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for operator start");
        }
        return operations.replay(new DeadEventReplayRequest(
                inspected.id(), inspected.version(), operator, "Repaired the event destination"));
    }

    private DeadEventDetails deadDetails(DeadEventOperations operations, EventId id) {
        DeadEventLookup lookup = operations.lookup(id);
        assertThat(lookup).isInstanceOf(DeadEventLookup.Dead.class);
        return ((DeadEventLookup.Dead) lookup).event();
    }

    private void startApplication(boolean scheduling) {
        startApplication(scheduling, false, 8);
    }

    private void startApplication(boolean scheduling, boolean deadOperations, int maxAttempts) {
        startApplication(scheduling, deadOperations, maxAttempts, new String[0]);
    }

    private void startApplication(boolean scheduling, boolean deadOperations, int maxAttempts,
                                  String... extraProperties) {
        String[] properties = new String[] {
                        "--server.port=0",
                        "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                        "--spring.datasource.username=" + MYSQL.getUsername(),
                        "--spring.datasource.password=" + MYSQL.getPassword(),
                        "--example.rocketmq.topic=" + TOPIC,
                        "--example.rocketmq.consumer-group=" + GROUP,
                        "--reliable-event.scheduling-enabled=" + scheduling,
                        "--reliable-event.dead-operations-enabled=" + deadOperations,
                        "--reliable-event.max-attempts=" + maxAttempts,
                        "--reliable-event.poll-interval=100ms",
                        "--reliable-event.initial-retry-delay=100ms",
                        "--reliable-event.max-retry-delay=100ms",
                        "--reliable-event.rocketmq.endpoints=localhost:8081",
                        "--reliable-event.rocketmq.request-timeout=10s",
                        "--reliable-event.rocketmq.mappings.order-created.destination=" + TOPIC + ":created"
                };
        String[] all = java.util.Arrays.copyOf(properties, properties.length + extraProperties.length);
        System.arraycopy(extraProperties, 0, all, properties.length, extraProperties.length);
        context = new SpringApplicationBuilder(ExampleApplication.class, TracingTestConfiguration.class)
                .run(all);
    }

    private JdbcEventPublicationWorker worker(EventSender sender, Clock clock) {
        return new JdbcEventPublicationWorker(jdbc,
                context.getBean(PlatformTransactionManager.class), sender, clock,
                10, "example-test-worker", Duration.ofSeconds(30),
                new ExponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(100), 0, () -> 0));
    }

    private Created postOrder(String itemCode, int quantity) throws Exception {
        return postOrder(itemCode, quantity, null);
    }

    private Created postOrder(String itemCode, int quantity, String traceparent) throws Exception {
        int port = ((ServletWebServerApplicationContext) context).getWebServer().getPort();
        HttpRequest.Builder builder = HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/orders"))
                .header("Content-Type", "application/json");
        if (traceparent != null) builder.header("traceparent", traceparent);
        HttpRequest request = builder.POST(HttpRequest.BodyPublishers.ofString(
                "{\"itemCode\":\"" + itemCode + "\",\"quantity\":" + quantity + "}"))
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request,
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(201);
        JsonNode body = mapper.readTree(response.body());
        return new Created(body.path("orderId").asLong(), body.path("eventId").asLong());
    }

    private MessageView exampleMessage(Created created, OrderCreatedPayload payload,
                                       ObjectMapper appMapper) throws Exception {
        MessageView message = mock(MessageView.class);
        when(message.getProperties()).thenReturn(Map.of(
                "reliable_event_type", OrderService.EVENT_TYPE,
                "reliable_event_key", Long.toString(created.orderId()),
                "reliable_event_id", Long.toString(created.eventId())));
        when(message.getKeys()).thenReturn(List.of(Long.toString(created.orderId())));
        when(message.getBody()).thenReturn(ByteBuffer.wrap(appMapper.writeValueAsBytes(payload)));
        return message;
    }

    private JsonNode getOrder(long id) throws Exception {
        int port = ((ServletWebServerApplicationContext) context).getWebServer().getPort();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port
                + "/orders/" + id)).GET().build();
        return mapper.readTree(HttpClient.newHttpClient().send(request,
                HttpResponse.BodyHandlers.ofString()).body());
    }

    private List<MessageView> receiveFor(SimpleConsumer consumer, String key, int expected)
            throws Exception {
        List<MessageView> matches = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (matches.size() < expected && System.nanoTime() < deadline) {
            for (MessageView message : consumer.receive(8, Duration.ofSeconds(15))) {
                if (message.getKeys().contains(key)) {
                    matches.add(message);
                }
                consumer.ack(message);
            }
        }
        assertThat(matches).hasSize(expected);
        return matches;
    }

    private int status(long eventId) {
        return jdbc.queryForObject("SELECT status FROM reliable_event_outbox WHERE id = ?",
                Integer.class, eventId);
    }

    private int attemptCount(long eventId) {
        return jdbc.queryForObject(
                "SELECT attempt_count FROM reliable_event_outbox WHERE id = ?",
                Integer.class, eventId);
    }

    private long auditCount(long eventId) {
        return jdbc.queryForObject(
                "SELECT COUNT(*) FROM reliable_event_replay_audit WHERE event_id = ?",
                Long.class, eventId);
    }

    private int effectCount(long orderId) {
        Integer count = jdbc.queryForObject(
                "SELECT COALESCE(SUM(handled_count), 0) FROM example_order_effect WHERE order_id = ?",
                Integer.class, orderId);
        return count == null ? 0 : count;
    }

    private long consumedCount(long orderId) {
        return jdbc.queryForObject("""
                SELECT COUNT(*) FROM example_consumed_event
                WHERE event_type = ? AND event_key = ?
                """, Long.class, OrderService.EVENT_TYPE, Long.toString(orderId));
    }

    private static SpanData named(List<SpanData> spans, String name, String traceId) {
        return spans.stream().filter(span -> span.getName().equals(name))
                .filter(span -> span.getTraceId().equals(traceId)).findFirst().orElseThrow();
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TracingTestConfiguration {
        @Bean
        MemorySpanExporter exampleMemorySpanExporter() {
            return new MemorySpanExporter();
        }

        @Bean
        SdkTracerProviderBuilderCustomizer memorySpanExporterCustomizer(MemorySpanExporter exporter) {
            return builder -> builder.addSpanProcessor(SimpleSpanProcessor.create(exporter));
        }
    }

    static final class MemorySpanExporter implements SpanExporter {
        private static final CopyOnWriteArrayList<SpanData> FINISHED = new CopyOnWriteArrayList<>();

        static void reset() { FINISHED.clear(); }
        static List<SpanData> finishedSpans() { return List.copyOf(FINISHED); }

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            FINISHED.addAll(spans);
            return CompletableResultCode.ofSuccess();
        }

        @Override public CompletableResultCode flush() { return CompletableResultCode.ofSuccess(); }
        @Override public CompletableResultCode shutdown() { return CompletableResultCode.ofSuccess(); }
    }

    private static org.testcontainers.containers.Container.ExecResult command(String arguments)
            throws Exception {
        return broker.execInContainer("sh", "-c", ROCKET_HOME + "/bin/mqadmin " + arguments);
    }

    private record Created(long orderId, long eventId) { }

    private static final class FixedPortContainer extends GenericContainer<FixedPortContainer> {
        private FixedPortContainer(DockerImageName image) { super(image); }
        private void bindFixedPort(int hostPort, int containerPort) {
            addFixedExposedPort(hostPort, containerPort);
        }
    }
}
