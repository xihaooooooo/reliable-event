package dev.reliableevent.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.MissingActiveTransactionException;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Sql({
        "classpath:schema/reliable-event-outbox.sql",
        "classpath:schema/test-business-record.sql",
        "classpath:schema/clear-test-data.sql"
})
class ReliableEventIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-09-21T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:5.7.44")
            .withDatabaseName("reliable_event_test")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    PlatformTransactionManager transactionManager;

    ObjectMapper objectMapper;
    ReliableEventPublisher publisher;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        publisher = new JdbcReliableEventPublisher(jdbcTemplate, objectMapper, CLOCK, 8);
    }

    @Test
    void businessAndOutboxRowsRollbackTogether() {
        assertThatThrownBy(() -> inTransaction(() -> {
            insertBusinessRecord(101L);
            publisher.publish(event(101L, NOW));
            throw new IntentionalRollbackException();
        })).isInstanceOf(IntentionalRollbackException.class);

        assertThat(businessRowCount()).isZero();
        assertThat(outboxRowCount()).isZero();
    }

    @Test
    void committedEventIsStoredAsPendingJson() throws Exception {
        EventId eventId = inTransaction(() -> {
            insertBusinessRecord(102L);
            return publisher.publish(event(102L, NOW));
        });

        assertThat(eventId.value()).isPositive();
        assertThat(businessRowCount()).isOne();
        assertThat(outboxRowCount()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PENDING.code());

        String payload = jdbcTemplate.queryForObject(
                "SELECT payload FROM reliable_event_outbox WHERE id = ?",
                String.class,
                eventId.value()
        );
        assertThat(objectMapper.readTree(payload).path("taskId").asLong()).isEqualTo(102L);
    }

    @Test
    void publishRequiresAnActiveTransaction() {
        assertThatThrownBy(() -> publisher.publish(event(103L, NOW)))
                .isInstanceOf(MissingActiveTransactionException.class)
                .hasMessageContaining("active database transaction");

        assertThat(outboxRowCount()).isZero();
    }

    @Test
    void duplicateEventIdentityReturnsExistingEvent() {
        List<EventId> eventIds = inTransaction(() -> List.of(
                publisher.publish(event(104L, NOW)),
                publisher.publish(event(104L, NOW.plusSeconds(60)))
        ));

        assertThat(eventIds.get(1)).isEqualTo(eventIds.get(0));
        assertThat(outboxRowCount()).isOne();
    }

    @Test
    void workerPublishesOnlyEventsWhoseAvailableTimeHasArrived() {
        EventId dueEvent = inTransaction(() -> publisher.publish(event(201L, NOW.minusSeconds(1))));
        EventId futureEvent = inTransaction(() -> publisher.publish(event(202L, NOW.plusSeconds(60))));
        FakeEventSender sender = new FakeEventSender();
        JdbcEventPublicationWorker worker = new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                CLOCK,
                50
        );

        int publishedCount = worker.publishDueEvents();

        assertThat(publishedCount).isOne();
        assertThat(sender.sentEvents)
                .extracting(StoredEvent::eventKey)
                .containsExactly("201");
        assertThat(statusOf(dueEvent)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(statusOf(futureEvent)).isEqualTo(EventStatus.PENDING.code());
        assertThat(publishedAtOf(dueEvent)).isNotNull();
        assertThat(publishedAtOf(futureEvent)).isNull();
    }

    private ReliableEvent<CouponTaskPayload> event(long taskId, Instant availableAt) {
        return new ReliableEvent<>(
                "coupon-task-execute",
                Long.toString(taskId),
                new CouponTaskPayload(taskId),
                availableAt,
                Map.of("source", "integration-test")
        );
    }

    private void insertBusinessRecord(long id) {
        jdbcTemplate.update(
                "INSERT INTO test_business_record (id, name) VALUES (?, ?)",
                id,
                "task-" + id
        );
    }

    private <T> T inTransaction(Supplier<T> action) {
        return new TransactionTemplate(transactionManager).execute(status -> action.get());
    }

    private long businessRowCount() {
        return requiredLong("SELECT COUNT(*) FROM test_business_record");
    }

    private long outboxRowCount() {
        return requiredLong("SELECT COUNT(*) FROM reliable_event_outbox");
    }

    private int statusOf(EventId eventId) {
        Integer status = jdbcTemplate.queryForObject(
                "SELECT status FROM reliable_event_outbox WHERE id = ?",
                Integer.class,
                eventId.value()
        );
        if (status == null) {
            throw new IllegalStateException("Event has no status: " + eventId.value());
        }
        return status;
    }

    private Instant publishedAtOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT published_at FROM reliable_event_outbox WHERE id = ?",
                (resultSet, rowNumber) -> {
                    var timestamp = resultSet.getTimestamp("published_at");
                    return timestamp == null ? null : timestamp.toInstant();
                },
                eventId.value()
        );
    }

    private long requiredLong(String sql) {
        Long value = jdbcTemplate.queryForObject(sql, Long.class);
        if (value == null) {
            throw new IllegalStateException("Query did not return a count");
        }
        return value;
    }

    private record CouponTaskPayload(long taskId) {
    }

    private static final class FakeEventSender implements EventSender {

        private final List<StoredEvent> sentEvents = new ArrayList<>();

        @Override
        public SendReceipt send(StoredEvent event) {
            sentEvents.add(event);
            return new SendReceipt("fake-" + event.id().value());
        }
    }

    private static final class IntentionalRollbackException extends RuntimeException {
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
    }
}
