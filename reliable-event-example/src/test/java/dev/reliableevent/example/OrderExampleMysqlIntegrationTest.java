package dev.reliableevent.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.MissingActiveTransactionException;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.SendReceipt;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers(disabledWithoutDocker = true)
class OrderExampleMysqlIntegrationTest {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("reliable_event_example_test")
            .withUsername("test")
            .withPassword("test");

    private ConfigurableApplicationContext context;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
        new ResourceDatabasePopulator(
                new ClassPathResource("schema/reliable-event-outbox.sql"),
                new ClassPathResource("schema/example-tables.sql")
        ).execute(dataSource);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.update("DELETE FROM example_order_effect");
        jdbc.update("DELETE FROM example_consumed_event");
        jdbc.update("DELETE FROM reliable_event_outbox");
        jdbc.update("DELETE FROM example_order");
        context = new SpringApplicationBuilder(ExampleApplication.class, FakeSender.class)
                .run(
                        "--spring.main.web-application-type=none",
                        "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                        "--spring.datasource.username=" + MYSQL.getUsername(),
                        "--spring.datasource.password=" + MYSQL.getPassword(),
                        "--reliable-event.scheduling-enabled=false",
                        "--example.consumer.enabled=false"
                );
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void orderAndOutboxCommitAndRollbackTogether() {
        OrderService service = context.getBean(OrderService.class);
        PlatformTransactionManager manager = context.getBean(PlatformTransactionManager.class);

        new TransactionTemplate(manager).execute(status -> {
            service.create("book", 2);
            status.setRollbackOnly();
            return null;
        });
        assertThat(count("example_order")).isZero();
        assertThat(count("reliable_event_outbox")).isZero();

        OrderService.CreatedOrder created = service.create("book", 2);
        assertThat(count("example_order")).isOne();
        assertThat(count("reliable_event_outbox")).isOne();
        assertThat(jdbc.queryForObject("""
                SELECT event_key FROM reliable_event_outbox WHERE id = ?
                """, String.class, created.eventId())).isEqualTo(Long.toString(created.orderId()));
        assertThat(jdbc.queryForObject("""
                SELECT status FROM reliable_event_outbox WHERE id = ?
                """, Integer.class, created.eventId())).isZero();
    }

    @Test
    void aDuplicateDeliveryChangesTheBusinessResultOnlyOnce() throws Exception {
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("pen", 3);
        OrderCreatedPayload payload = new OrderCreatedPayload(created.orderId(), "pen", 3);
        MessageView first = message(created, payload, "broker-message-1");
        MessageView duplicate = message(created, payload, "broker-message-2");
        OrderMessageHandler handler = context.getBean(OrderMessageHandler.class);

        assertThat(handler.handle(first)).isTrue();
        assertThat(handler.handle(duplicate)).isFalse();
        assertThat(count("example_consumed_event")).isOne();
        assertThat(count("example_order_effect")).isOne();
        assertThat(jdbc.queryForObject("""
                SELECT handled_count FROM example_order_effect WHERE order_id = ?
                """, Integer.class, created.orderId())).isOne();
    }

    @Test
    void invalidBusinessEffectRollsBackItsDeduplicationRecord() throws Exception {
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("cup", 1);
        OrderCreatedPayload wrongPayload = new OrderCreatedPayload(created.orderId(), "wrong", 1);

        assertThatThrownBy(() -> context.getBean(OrderMessageHandler.class)
                .handle(message(created, wrongPayload, "broker-message-wrong")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(count("example_consumed_event")).isZero();
        assertThat(count("example_order_effect")).isZero();
    }

    @Test
    void duplicateBusinessKeyWithDifferentEventIdIsRejected() throws Exception {
        OrderService.CreatedOrder created = context.getBean(OrderService.class).create("cup", 1);
        OrderCreatedPayload payload = new OrderCreatedPayload(created.orderId(), "cup", 1);
        OrderMessageHandler handler = context.getBean(OrderMessageHandler.class);

        assertThat(handler.handle(message(created, payload, "broker-message-1"))).isTrue();
        OrderService.CreatedOrder differentId = new OrderService.CreatedOrder(
                created.orderId(), created.eventId() + 1);
        assertThatThrownBy(() -> handler.handle(message(differentId, payload, "broker-message-2")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different event id");
        assertThat(count("example_consumed_event")).isOne();
        assertThat(count("example_order_effect")).isOne();
    }

    @Test
    void publisherRequiresAnActiveTransactionAndKeepsTheOriginalIdentity() {
        ReliableEventPublisher publisher = context.getBean(ReliableEventPublisher.class);
        ReliableEvent<Map<String, Integer>> event = new ReliableEvent<>(
                "order-created", "manual-key", Map.of("value", 1), Instant.now(), Map.of());
        assertThatThrownBy(() -> publisher.publish(event))
                .isInstanceOf(MissingActiveTransactionException.class);

        TransactionTemplate tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        EventId first = tx.execute(status -> publisher.publish(event));
        EventId second = tx.execute(status -> publisher.publish(new ReliableEvent<>(
                "order-created", "manual-key", Map.of("value", 2), Instant.now(), Map.of())));
        assertThat(second).isEqualTo(first);
        assertThat(count("reliable_event_outbox")).isOne();
    }

    private MessageView message(OrderService.CreatedOrder created, OrderCreatedPayload payload,
                                String messageId) throws Exception {
        MessageView message = mock(MessageView.class);
        when(message.getProperties()).thenReturn(Map.of(
                "reliable_event_type", OrderService.EVENT_TYPE,
                "reliable_event_key", Long.toString(created.orderId()),
                "reliable_event_id", Long.toString(created.eventId())
        ));
        when(message.getKeys()).thenReturn(java.util.List.of(Long.toString(created.orderId())));
        when(message.getBody()).thenReturn(ByteBuffer.wrap(
                context.getBean(ObjectMapper.class).writeValueAsString(payload)
                        .getBytes(StandardCharsets.UTF_8)));
        return message;
    }

    private long count(String table) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class FakeSender {
        @Bean
        EventSender exampleFakeSender() {
            return event -> new SendReceipt("fake-" + event.id().value());
        }
    }
}
