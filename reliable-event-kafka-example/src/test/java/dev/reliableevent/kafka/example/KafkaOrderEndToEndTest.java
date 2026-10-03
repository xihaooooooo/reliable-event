package dev.reliableevent.kafka.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.TestInfo;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@Timeout(180)
class KafkaOrderEndToEndTest {
    private static final String TOPIC_PREFIX = "reliable-event-kafka-example-test-";
    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("reliable_event_kafka_example_test").withUsername("test").withPassword("test");
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"))
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private ConfigurableApplicationContext context;
    private JdbcTemplate jdbc;
    private String consumerGroup;
    private String topic;
    private final java.util.List<ExampleConsumerLoop> consumerLoops = new java.util.ArrayList<>();

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        consumerGroup = "kafka-example-test-" + UUID.randomUUID();
        topic = TOPIC_PREFIX + UUID.randomUUID().toString().replace("-", "");
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
        new ResourceDatabasePopulator(new ClassPathResource("schema/reliable-event-outbox.sql"),
                new ClassPathResource("schema/example-tables.sql")).execute(dataSource);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.update("DELETE FROM example_order_effect");
        jdbc.update("DELETE FROM example_consumed_event");
        jdbc.update("DELETE FROM reliable_event_outbox");
        jdbc.update("DELETE FROM reliable_event_identity");
        jdbc.update("DELETE FROM example_order");
        if (!testInfo.getTestMethod().orElseThrow().getName().equals(
                "missingTopicRetriesToPublishedAfterTopicProvisioning")) {
            createTopic();
        }
        context = new SpringApplicationBuilder(KafkaExampleApplication.class)
                .run("--server.port=0",
                        "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                        "--spring.datasource.username=" + MYSQL.getUsername(),
                        "--spring.datasource.password=" + MYSQL.getPassword(),
                        "--example.kafka.topic=" + topic,
                        "--example.kafka.consumer-group=" + consumerGroup,
                        "--example.consumer.enabled=false",
                        "--example.kafka.create-topic=false",
                        "--reliable-event.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--reliable-event.scheduling-enabled=false",
                        "--reliable-event.poll-interval=50ms",
                        "--reliable-event.initial-retry-delay=50ms",
                        "--reliable-event.max-retry-delay=100ms");
    }

    @AfterEach
    void tearDown() {
        consumerLoops.forEach(ExampleConsumerLoop::stop);
        if (context != null) context.close();
    }

    @Test
    void transactionPublishesThenKafkaConsumerAppliesAnIdentityDuplicateOnlyOnce() throws Exception {
        ExampleConsumerLoop exampleConsumer = new ExampleConsumerLoop(consumerConfiguration(consumerGroup), topic,
                Duration.ofMillis(100), context.getBean(OrderMessageHandler.class));
        consumerLoops.add(exampleConsumer);
        exampleConsumer.start();
        try {
            OrderService.CreatedOrder created = context.getBean(OrderService.class).create("book", 2);
            publishPending();
            await(() -> status(created.eventId()) == 2 && effectCount(created.orderId()) == 1,
                    Duration.ofSeconds(45));
            assertThat(count("example_consumed_event")).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT handled_count FROM example_order_effect WHERE order_id = ?",
                    Integer.class, created.orderId())).isOne();

            ProducerRecord<String, byte[]> duplicate = new ProducerRecord<>(topic, Long.toString(created.orderId()),
                    context.getBean(com.fasterxml.jackson.databind.ObjectMapper.class)
                            .writeValueAsBytes(new OrderCreatedPayload(created.orderId(), "book", 2)));
            duplicate.headers().add("reliable_event_id", Long.toString(created.eventId()).getBytes(StandardCharsets.UTF_8));
            duplicate.headers().add("reliable_event_type", OrderService.EVENT_TYPE.getBytes(StandardCharsets.UTF_8));
            duplicate.headers().add("reliable_event_key", Long.toString(created.orderId()).getBytes(StandardCharsets.UTF_8));
            try (KafkaProducer<String, byte[]> producer = producer()) { producer.send(duplicate).get(10, TimeUnit.SECONDS); }
            await(() -> committed(consumerGroup, 0) >= 2, Duration.ofSeconds(20));
            assertThat(effectCount(created.orderId())).isOne();
            assertThat(count("example_consumed_event")).isOne();
        } finally {
            exampleConsumer.stop();
        }
    }

    @Test
    void databaseFailureLeavesTheFailedOffsetUncommittedAndReplaysItAfterRepair() throws Exception {
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("cup", 1);
        publishPending();
        assertThat(status(created.eventId())).isEqualTo(2);
        jdbc.update("UPDATE example_order SET item_code = 'temporarily-wrong' WHERE id = ?", created.orderId());
        OrderService.CreatedOrder following = context.getBean(OrderService.class).create("lamp", 4);
        publishPending();
        assertThat(status(following.eventId())).isEqualTo(2);

        String retryGroup = consumerGroup + "-retry";
        ExampleConsumerLoop retryingConsumer = new ExampleConsumerLoop(consumerConfiguration(retryGroup), topic,
                Duration.ofMillis(100), context.getBean(OrderMessageHandler.class));
        consumerLoops.add(retryingConsumer);
        retryingConsumer.start();
        try {
            await(() -> retryingConsumer.lastFailedOffset() == 0, Duration.ofSeconds(15));
            assertThat(committed(retryGroup, 0)).isEqualTo(-1);
            assertThat(count("example_consumed_event")).isZero();
            assertThat(count("example_order_effect")).isZero();

            jdbc.update("UPDATE example_order SET item_code = 'cup' WHERE id = ?", created.orderId());
            await(() -> effectCount(created.orderId()) == 1 && effectCount(following.orderId()) == 1
                            && committed(retryGroup, 0) == 2,
                    Duration.ofSeconds(20));
            assertThat(count("example_consumed_event")).isEqualTo(2);
        } finally {
            retryingConsumer.stop();
        }
    }

    @Test
    void unknownConfirmationRetriesTheSameEventIdBeforeMarkingPublished() throws Exception {
        ExampleConsumerLoop exampleConsumer = new ExampleConsumerLoop(consumerConfiguration(consumerGroup), topic,
                Duration.ofMillis(100), context.getBean(OrderMessageHandler.class));
        consumerLoops.add(exampleConsumer);
        exampleConsumer.start();
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("book", 3);
        EventSender broker = context.getBean(EventSender.class);
        AtomicInteger sends = new AtomicInteger();
        java.util.List<Long> eventIds = new java.util.concurrent.CopyOnWriteArrayList<>();
        EventSender dropFirstAcknowledgement = event -> {
            eventIds.add(event.id().value());
            var receipt = broker.send(event); // both attempts send to the real Kafka broker
            if (sends.getAndIncrement() == 0) {
                throw EventSendException.resultUnknown("Injected lost producer confirmation");
            }
            return receipt;
        };
        JdbcEventPublicationWorker worker = worker(dropFirstAcknowledgement);

        try {
            assertThat(worker.publishDueEvents()).isZero();
            assertThat(status(created.eventId())).isEqualTo(3);
            assertThat(jdbc.queryForObject("SELECT attempt_count FROM reliable_event_outbox WHERE id = ?",
                    Integer.class, created.eventId())).isEqualTo(1);

            await(() -> isDue(created.eventId()), Duration.ofSeconds(5));
            assertThat(worker.publishDueEvents()).isEqualTo(1);
            assertThat(status(created.eventId())).isEqualTo(2);
            assertThat(eventIds).containsExactly(created.eventId(), created.eventId());
            await(() -> committed(consumerGroup, 0) == 2, Duration.ofSeconds(20));
            assertThat(effectCount(created.orderId())).isOne();
            assertThat(count("example_consumed_event")).isOne();
        } finally {
            exampleConsumer.stop();
        }
    }

    @Test
    void missingTopicRetriesToPublishedAfterTopicProvisioning() throws Exception {
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("cup", 5);
        JdbcEventPublicationWorker worker = worker(context.getBean(EventSender.class));

        assertThat(worker.publishDueEvents()).isZero();
        assertThat(status(created.eventId())).isEqualTo(3);
        createTopic();
        awaitKafkaAvailable();
        await(() -> isDue(created.eventId()), Duration.ofSeconds(5));

        assertThat(worker.publishDueEvents()).isEqualTo(1);
        assertThat(status(created.eventId())).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT id FROM reliable_event_outbox WHERE id = ?",
                Long.class, created.eventId())).isEqualTo(created.eventId());
    }

    @Test
    void brokerInterruptionRetriesToPublishedAfterBrokerRecovery() throws Exception {
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("jar", 6);
        boolean paused = false;
        try {
            DockerClientFactory.instance().client().pauseContainerCmd(KAFKA.getContainerId()).exec();
            paused = true;
            assertThat(worker(context.getBean(EventSender.class)).publishDueEvents()).isZero();
            assertThat(status(created.eventId())).isEqualTo(3);
        } finally {
            if (paused) {
                DockerClientFactory.instance().client().unpauseContainerCmd(KAFKA.getContainerId()).exec();
            }
        }

        awaitKafkaAvailable();
        await(() -> isDue(created.eventId()), Duration.ofSeconds(5));
        assertThat(worker(context.getBean(EventSender.class)).publishDueEvents()).isEqualTo(1);
        assertThat(status(created.eventId())).isEqualTo(2);
    }

    @Test
    void brokerRejectingOversizedRecordMovesEventToDead() throws Exception {
        setMaxMessageBytes(1);
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("pen", 2);

        assertThat(worker(context.getBean(EventSender.class)).publishDueEvents()).isZero();
        assertThat(status(created.eventId())).isEqualTo(4);
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM reliable_event_outbox WHERE id = ?",
                Integer.class, created.eventId())).isEqualTo(1);
    }

    private Map<String, Object> consumerConfiguration(String group) {
        return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    }

    private KafkaProducer<String, byte[]> producer() {
        return new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"));
    }

    private JdbcEventPublicationWorker worker(EventSender sender) {
        return new JdbcEventPublicationWorker(jdbc,
                context.getBean(org.springframework.transaction.PlatformTransactionManager.class), sender,
                java.time.Clock.systemUTC(), 10, "kafka-example-state-test", Duration.ofSeconds(30),
                new ExponentialBackoff(Duration.ofMillis(10), Duration.ofMillis(10), 0, () -> 0));
    }

    private int publishPending() {
        return worker(context.getBean(EventSender.class)).publishDueEvents();
    }

    private void createTopic() throws Exception {
        try (AdminClient admin = admin()) {
            try {
                admin.createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 1)))
                        .all().get(20, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException exists) {
                if (!(exists.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw exists;
            }
        }
    }

    private void setMaxMessageBytes(int bytes) throws Exception {
        try (AdminClient admin = admin()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            admin.alterConfigs(Map.of(resource, new Config(java.util.List.of(
                    new ConfigEntry("max.message.bytes", Integer.toString(bytes))))))
                    .all().get(20, TimeUnit.SECONDS);
        }
    }

    private void awaitKafkaAvailable() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            try (AdminClient admin = admin()) {
                var descriptions = admin.describeTopics(java.util.List.of(topic)).allTopicNames()
                        .get(2, TimeUnit.SECONDS);
                var partitions = descriptions.get(topic).partitions();
                if (!partitions.isEmpty() && partitions.get(0).leader() != null
                        && partitions.get(0).leader().id() >= 0) return;
            } catch (Exception recovering) {
                Thread.sleep(100);
            }
        }
        throw new AssertionError("Kafka broker did not recover after unpause");
    }

    private AdminClient admin() {
        return AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "default.api.timeout.ms", 5_000, "request.timeout.ms", 3_000));
    }

    private long committed(String group, int partition) {
        Properties config = new Properties();
        config.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(config)) {
            try {
                var offsets = admin.listConsumerGroupOffsets(group).all().get(5, TimeUnit.SECONDS);
                var value = offsets.get(group).get(new TopicPartition(topic, partition));
                return value == null ? -1 : value.offset();
            } catch (Exception failure) { throw new IllegalStateException(failure); }
        }
    }

    private int status(long eventId) {
        return jdbc.queryForObject("SELECT status FROM reliable_event_outbox WHERE id = ?", Integer.class, eventId);
    }
    private boolean isDue(long eventId) {
        return jdbc.queryForObject("""
                SELECT next_attempt_at <= ?
                FROM reliable_event_outbox WHERE id = ?
                """, Boolean.class, Timestamp.from(Clock.systemUTC().instant()), eventId);
    }
    private long effectCount(long orderId) {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM example_order_effect WHERE order_id = ?", Long.class, orderId);
        return count == null ? 0 : count;
    }
    private long count(String table) { return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class); }
    private static void await(java.util.function.BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).as("condition should become true within " + timeout).isTrue();
    }
}
