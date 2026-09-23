package dev.reliableevent.jdbc.fault;

import dev.reliableevent.EventId;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
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

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Sql({
        "classpath:schema/reliable-event-outbox.sql",
        "classpath:schema/test-business-record.sql",
        "classpath:schema/test-message-delivery.sql",
        "classpath:schema/clear-test-data.sql"
})
class PublicationProcessFailureIntegrationTest {

    private static final Instant CLAIM_NOW = Instant.parse("2026-09-23T00:00:00Z");
    private static final Duration LEASE_DURATION = Duration.ofSeconds(30);
    private static final Duration READY_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration EXIT_TIMEOUT = Duration.ofSeconds(5);
    private static final String CRASH_WORKER_ID = "process-crash-worker";
    private static final String RECOVERY_WORKER_ID = "process-recovery-worker";

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("reliable_event_process_test")
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

    @Autowired
    DataSource dataSource;

    JdbcOutboxRepository repository;

    @BeforeEach
    void setUp() {
        repository = new JdbcOutboxRepository(jdbcTemplate);
    }

    @Test
    void processKilledAfterClaimBeforeSendIsRecoveredAndPublishedOnce() throws Exception {
        EventId eventId = insertEvent(401L);

        PublicationReadySignal signal = runUntilCrashPointAndKill(
                PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND,
                eventId
        );

        assertThat(signal.mode())
                .isEqualTo(PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND);
        assertThat(signal.eventId()).isEqualTo(eventId.value());
        assertThat(signal.claimVersion()).isOne();
        assertThat(signal.workerId()).isEqualTo(CRASH_WORKER_ID);
        assertThat(deliveriesFor(eventId)).isEmpty();
        assertClaimedByCrashProcess(eventId);

        recoverExpiredLease(eventId);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);

        publishRecoveredEvent(eventId);

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(4L);
        assertThat(publishedAtOf(eventId)).isNotNull();
        assertThat(deliveriesFor(eventId))
                .singleElement()
                .satisfies(delivery -> {
                    assertThat(delivery.eventKey()).isEqualTo("401");
                    assertThat(delivery.workerId()).isEqualTo(RECOVERY_WORKER_ID);
                });
    }

    @Test
    void processKilledAfterDeliveryBeforeStateUpdateCausesExpectedDuplicate()
            throws Exception {
        EventId eventId = insertEvent(402L);

        PublicationReadySignal signal = runUntilCrashPointAndKill(
                PublicationCrashMode.AFTER_DELIVERY_BEFORE_STATE_UPDATE,
                eventId
        );

        assertThat(signal.mode())
                .isEqualTo(PublicationCrashMode.AFTER_DELIVERY_BEFORE_STATE_UPDATE);
        assertThat(signal.eventId()).isEqualTo(eventId.value());
        assertThat(signal.claimVersion()).isOne();
        assertThat(signal.workerId()).isEqualTo(CRASH_WORKER_ID);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(publishedAtOf(eventId)).isNull();
        assertThat(deliveriesFor(eventId))
                .singleElement()
                .satisfies(delivery -> {
                    assertThat(delivery.workerId()).isEqualTo(CRASH_WORKER_ID);
                    assertThat(delivery.messageId()).isEqualTo(signal.messageId());
                });

        recoverExpiredLease(eventId);
        publishRecoveredEvent(eventId);

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(4L);
        assertThat(publishedAtOf(eventId)).isNotNull();

        List<DeliveryRecord> deliveries = deliveriesFor(eventId);
        assertThat(deliveries).hasSize(2);
        assertThat(deliveries)
                .extracting(DeliveryRecord::eventKey)
                .containsOnly("402");
        assertThat(deliveries)
                .extracting(DeliveryRecord::workerId)
                .containsExactly(CRASH_WORKER_ID, RECOVERY_WORKER_ID);
        assertThat(deliveries)
                .extracting(DeliveryRecord::messageId)
                .doesNotHaveDuplicates();
    }

    private PublicationReadySignal runUntilCrashPointAndKill(
            PublicationCrashMode mode,
            EventId eventId
    ) throws Exception {
        PublicationProcessConfig config = new PublicationProcessConfig(
                MYSQL.getJdbcUrl(),
                MYSQL.getUsername(),
                MYSQL.getPassword(),
                CRASH_WORKER_ID,
                LEASE_DURATION,
                CLAIM_NOW
        );

        try (PublicationProcessFixture fixture = PublicationProcessFixture.start(
                mode,
                eventId.value(),
                config
        )) {
            PublicationReadySignal signal = fixture.awaitReady(READY_TIMEOUT);
            assertThat(fixture.isAlive()).isTrue();
            assertThat(signal.mode()).isEqualTo(mode);
            assertThat(signal.eventId()).isEqualTo(eventId.value());
            assertThat(signal.claimVersion()).isOne();
            assertThat(signal.workerId()).isEqualTo(CRASH_WORKER_ID);
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
            assertThat(attemptCountOf(eventId)).isOne();
            assertThat(versionOf(eventId)).isOne();
            assertThat(leaseOwnerOf(eventId)).isEqualTo(CRASH_WORKER_ID);
            assertThat(publishedAtOf(eventId)).isNull();
            if (mode == PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND) {
                assertThat(deliveriesFor(eventId)).isEmpty();
            } else {
                assertThat(deliveriesFor(eventId))
                        .singleElement()
                        .satisfies(delivery -> {
                            assertThat(delivery.workerId()).isEqualTo(CRASH_WORKER_ID);
                            assertThat(delivery.messageId()).isEqualTo(signal.messageId());
                        });
            }

            fixture.destroyForcibly(EXIT_TIMEOUT);

            assertThat(fixture.isAlive()).isFalse();
            return signal;
        }
    }

    private void assertClaimedByCrashProcess(EventId eventId) {
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isOne();
        assertThat(leaseOwnerOf(eventId)).isEqualTo(CRASH_WORKER_ID);
        assertThat(leaseUntilOf(eventId)).isNotNull();
        assertThat(publishedAtOf(eventId)).isNull();
    }

    private void recoverExpiredLease(EventId eventId) {
        int expired = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(3))
                WHERE id = ?
                  AND status = ?
                  AND lease_owner = ?
                  AND version = ?
                """,
                eventId.value(),
                EventStatus.PUBLISHING.code(),
                CRASH_WORKER_ID,
                1L
        );
        assertThat(expired).isOne();

        int recovered = new JdbcExpiredLeaseRecovery(
                jdbcTemplate,
                transactionManager,
                50,
                deterministicBackoff()
        ).recoverExpiredLeases();

        assertThat(recovered).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
        assertThat(lastErrorOf(eventId))
                .isEqualTo(JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR);
    }

    private void publishRecoveredEvent(EventId eventId) {
        Instant nextAttemptAt = nextAttemptAtOf(eventId);
        JdbcEventPublicationWorker worker = new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                new JdbcDeliveryProbeSender(dataSource, RECOVERY_WORKER_ID),
                Clock.fixed(nextAttemptAt, ZoneOffset.UTC),
                50,
                RECOVERY_WORKER_ID,
                LEASE_DURATION,
                deterministicBackoff()
        );

        assertThat(worker.publishDueEvents()).isOne();
    }

    private EventId insertEvent(long taskId) {
        return inTransaction(() -> repository.insert(
                "coupon-task-execute",
                Long.toString(taskId),
                "{\"taskId\":" + taskId + "}",
                "{\"source\":\"process-failure-test\"}",
                CLAIM_NOW.minusSeconds(1),
                CLAIM_NOW.minusSeconds(1),
                2
        ));
    }

    private List<DeliveryRecord> deliveriesFor(EventId eventId) {
        return jdbcTemplate.query(
                """
                SELECT id, event_id, event_key, worker_id, message_id, delivered_at
                FROM test_message_delivery
                WHERE event_id = ?
                ORDER BY id
                """,
                (resultSet, rowNumber) -> new DeliveryRecord(
                        resultSet.getLong("id"),
                        resultSet.getLong("event_id"),
                        resultSet.getString("event_key"),
                        resultSet.getString("worker_id"),
                        resultSet.getString("message_id"),
                        resultSet.getTimestamp("delivered_at").toInstant()
                ),
                eventId.value()
        );
    }

    private int statusOf(EventId eventId) {
        return requiredInt(
                "SELECT status FROM reliable_event_outbox WHERE id = ?",
                eventId
        );
    }

    private int attemptCountOf(EventId eventId) {
        return requiredInt(
                "SELECT attempt_count FROM reliable_event_outbox WHERE id = ?",
                eventId
        );
    }

    private long versionOf(EventId eventId) {
        Long version = jdbcTemplate.queryForObject(
                "SELECT version FROM reliable_event_outbox WHERE id = ?",
                Long.class,
                eventId.value()
        );
        if (version == null) {
            throw new IllegalStateException("Event has no version: " + eventId.value());
        }
        return version;
    }

    private String leaseOwnerOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT lease_owner FROM reliable_event_outbox WHERE id = ?",
                String.class,
                eventId.value()
        );
    }

    private Instant leaseUntilOf(EventId eventId) {
        return nullableInstant(
                "SELECT lease_until FROM reliable_event_outbox WHERE id = ?",
                eventId
        );
    }

    private Instant publishedAtOf(EventId eventId) {
        return nullableInstant(
                "SELECT published_at FROM reliable_event_outbox WHERE id = ?",
                eventId
        );
    }

    private Instant nextAttemptAtOf(EventId eventId) {
        Timestamp timestamp = jdbcTemplate.queryForObject(
                "SELECT next_attempt_at FROM reliable_event_outbox WHERE id = ?",
                Timestamp.class,
                eventId.value()
        );
        if (timestamp == null) {
            throw new IllegalStateException("Event has no next attempt time: " + eventId.value());
        }
        return timestamp.toInstant();
    }

    private String lastErrorOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT last_error FROM reliable_event_outbox WHERE id = ?",
                String.class,
                eventId.value()
        );
    }

    private int requiredInt(String sql, EventId eventId) {
        Integer value = jdbcTemplate.queryForObject(
                sql,
                Integer.class,
                eventId.value()
        );
        if (value == null) {
            throw new IllegalStateException("Query returned no value for event: " + eventId.value());
        }
        return value;
    }

    private Instant nullableInstant(String sql, EventId eventId) {
        return jdbcTemplate.queryForObject(
                sql,
                (resultSet, rowNumber) -> {
                    Timestamp timestamp = resultSet.getTimestamp(1);
                    return timestamp == null ? null : timestamp.toInstant();
                },
                eventId.value()
        );
    }

    private ExponentialBackoff deterministicBackoff() {
        return new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                0.0,
                () -> 0.0
        );
    }

    private <T> T inTransaction(Supplier<T> action) {
        return new TransactionTemplate(transactionManager).execute(status -> action.get());
    }

    private record DeliveryRecord(
            long id,
            long eventId,
            String eventKey,
            String workerId,
            String messageId,
            Instant deliveredAt
    ) {
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
    }
}
