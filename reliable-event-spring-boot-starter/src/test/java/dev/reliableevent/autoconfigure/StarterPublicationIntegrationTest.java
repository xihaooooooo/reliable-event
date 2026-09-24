package dev.reliableevent.autoconfigure;

import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.jdbc.internal.cycle.JdbcEventPublicationCycle;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers(disabledWithoutDocker = true)
@Timeout(180)
class StarterPublicationIntegrationTest {

    private static final String TOPIC = "reliable-event-starter-test";
    private static final String GROUP = "reliable-event-starter-consumer";
    private static final String ROCKETMQ_HOME = "/home/rocketmq/rocketmq-5.5.0";
    private static final String BROKER_CONFIG_PATH = ROCKETMQ_HOME + "/conf/starter-test.conf";

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("reliable_event_starter_test")
            .withUsername("test")
            .withPassword("test");

    private static Network network;
    private static GenericContainer<?> nameserver;
    private static FixedPortContainer broker;

    @BeforeAll
    static void startRocketMq() throws Exception {
        DockerImageName image = DockerImageName.parse("apache/rocketmq:5.5.0");
        network = Network.newNetwork();
        nameserver = new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases("namesrv")
                .withCommand("sh", "mqnamesrv")
                .waitingFor(Wait.forLogMessage(".*Name Server boot success.*\\n", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        broker = new FixedPortContainer(image)
                .withNetwork(network)
                .withNetworkAliases("broker")
                .withEnv("NAMESRV_ADDR", "namesrv:9876")
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
                .until(() -> command("clusterList -n namesrv:9876").getStdout().contains("broker-a"));
        assertThat(command("updateTopic -n namesrv:9876 -c DefaultCluster -t " + TOPIC).getExitCode())
                .isZero();
        assertThat(command("updateSubGroup -n namesrv:9876 -c DefaultCluster -g " + GROUP).getExitCode())
                .isZero();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250))
                .until(() -> command("topicRoute -n namesrv:9876 -t " + TOPIC)
                        .getStdout().contains("broker-a"));
    }

    @AfterAll
    static void stopRocketMq() {
        if (broker != null) { broker.stop(); }
        if (nameserver != null) { nameserver.stop(); }
        if (network != null) { network.close(); }
    }

    @Test
    void starterRegistersAndPublishesThroughRealServices() throws Exception {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestApplication.class)
                .properties(
                        "spring.main.web-application-type=none",
                        "spring.datasource.url=" + MYSQL.getJdbcUrl(),
                        "spring.datasource.username=" + MYSQL.getUsername(),
                        "spring.datasource.password=" + MYSQL.getPassword(),
                        "reliable-event.max-attempts=3",
                        "reliable-event.rocketmq.endpoints=localhost:8081",
                        "reliable-event.rocketmq.request-timeout=3s",
                        "reliable-event.rocketmq.mappings.coupon-task-execute.destination=" + TOPIC + ":execute"
                ).run()) {
            var dataSource = context.getBean(javax.sql.DataSource.class);
            new ResourceDatabasePopulator(
                    new ClassPathResource("schema/reliable-event-outbox.sql")
            ).execute(dataSource);
            JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
            String eventKey = "starter-" + System.nanoTime();
            EventId eventId = new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
                    .execute(status -> context.getBean(ReliableEventPublisher.class).publish(
                            new ReliableEvent<>(
                                    "coupon-task-execute", eventKey, Map.of("taskId", 101),
                                    Instant.now().minusSeconds(1), Map.of("source", "starter-test")
                            )
                    ));
            assertThat(eventId).isNotNull();
            assertThat(jdbc.queryForObject(
                    "SELECT max_attempts FROM reliable_event_outbox WHERE id = ?",
                    Integer.class, eventId.value()
            )).isEqualTo(3);
            assertThat(jdbc.queryForObject(
                    "SELECT status FROM reliable_event_outbox WHERE id = ?",
                    Integer.class, eventId.value()
            )).isEqualTo(0);

            assertThat(context.getBean(JdbcEventPublicationCycle.class).runOnce().publishedCount())
                    .isOne();
            assertThat(jdbc.queryForObject(
                    "SELECT status FROM reliable_event_outbox WHERE id = ?",
                    Integer.class, eventId.value()
            )).isEqualTo(2);

            ClientConfiguration clientConfig = ClientConfiguration.newBuilder()
                    .setEndpoints("localhost:8081")
                    .setRequestTimeout(Duration.ofSeconds(3))
                    .enableSsl(false)
                    .build();
            try (SimpleConsumer consumer = ClientServiceProvider.loadService()
                    .newSimpleConsumerBuilder()
                    .setClientConfiguration(clientConfig)
                    .setConsumerGroup(GROUP)
                    .setAwaitDuration(Duration.ofSeconds(3))
                    .setSubscriptionExpressions(Map.of(TOPIC, FilterExpression.SUB_ALL))
                    .build()) {
                MessageView message = receive(consumer, eventKey);
                assertThat(message.getTopic()).isEqualTo(TOPIC);
                assertThat(message.getTag()).contains("execute");
                assertThat(message.getKeys()).contains(eventKey);
                assertThat(context.getBean(com.fasterxml.jackson.databind.ObjectMapper.class)
                        .readTree(readBody(message.getBody())).path("taskId").asInt()).isEqualTo(101);
                assertThat(message.getProperties())
                        .containsEntry("source", "starter-test")
                        .containsEntry("reliable_event_id", Long.toString(eventId.value()))
                        .containsEntry("reliable_event_type", "coupon-task-execute")
                        .containsEntry("reliable_event_key", eventKey);
                consumer.ack(message);
            }
        }
    }

    private static MessageView receive(SimpleConsumer consumer, String eventKey) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            List<MessageView> messages = consumer.receive(16, Duration.ofSeconds(15));
            for (MessageView message : messages) {
                if (message.getKeys().contains(eventKey)) {
                    return message;
                }
                consumer.ack(message);
            }
        }
        throw new AssertionError("Timed out waiting for event key " + eventKey);
    }

    private static String readBody(ByteBuffer body) {
        byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static org.testcontainers.containers.Container.ExecResult command(String command)
            throws Exception {
        return broker.execInContainer("sh", "-c", ROCKETMQ_HOME + "/bin/mqadmin " + command);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
    }

    private static final class FixedPortContainer extends GenericContainer<FixedPortContainer> {
        private FixedPortContainer(DockerImageName image) { super(image); }
        private void bindFixedPort(int hostPort, int containerPort) {
            addFixedExposedPort(hostPort, containerPort);
        }
    }
}
