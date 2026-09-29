package dev.reliableevent.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
        context = new SpringApplicationBuilder(ExampleApplication.class)
                .run(
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
                );
    }

    private JdbcEventPublicationWorker worker(EventSender sender, Clock clock) {
        return new JdbcEventPublicationWorker(jdbc,
                context.getBean(PlatformTransactionManager.class), sender, clock,
                10, "example-test-worker", Duration.ofSeconds(30),
                new ExponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(100), 0, () -> 0));
    }

    private Created postOrder(String itemCode, int quantity) throws Exception {
        int port = ((ServletWebServerApplicationContext) context).getWebServer().getPort();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"itemCode\":\"" + itemCode
                        + "\",\"quantity\":" + quantity + "}"))
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request,
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(201);
        JsonNode body = mapper.readTree(response.body());
        return new Created(body.path("orderId").asLong(), body.path("eventId").asLong());
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
