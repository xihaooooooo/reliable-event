package dev.reliableevent.jdbc;

import dev.reliableevent.spi.OutboundEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.EventFallbackContext;
import dev.reliableevent.EventFallbackHandler;
import dev.reliableevent.MissingActiveTransactionException;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.internal.model.OutboxMetricsSnapshot;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.persistence.JdbcPublicationStore;
import dev.reliableevent.jdbc.internal.tracing.RegistrationTracer;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.spi.TransportReceipt;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.Statement;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        "classpath:schema/test-message-delivery.sql",
        "classpath:schema/clear-test-data.sql"
})
class ReliableEventIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReliableEventIntegrationTest.class);

    private static final Instant NOW = Instant.parse("2026-09-21T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final String WORKER_ID = "integration-worker-1";
    private static final String SECOND_WORKER_ID = "integration-worker-2";
    private static final Duration LEASE_DURATION = Duration.ofSeconds(30);

    @Container
    static final TestMySqlContainer MYSQL = TestMySqlContainer.configured("reliable_event_test");

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
    JdbcOutboxRepository repository;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        publisher = new JdbcReliableEventPublisher(jdbcTemplate, objectMapper, CLOCK, 8);
        repository = new JdbcOutboxRepository(jdbcTemplate);
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
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reliable_event_identity", Integer.class)).isZero();
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
    void tracingHeadersCommitAndRollbackWithBusinessAndOutboxRows() throws Exception {
        JdbcReliableEventPublisher tracedPublisher = new JdbcReliableEventPublisher(
                jdbcTemplate, objectMapper, CLOCK, 8, tracingWith("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"));

        EventId committed = inTransaction(() -> tracedPublisher.publish(event(1021L, NOW)));
        String headers = jdbcTemplate.queryForObject(
                "SELECT headers FROM reliable_event_outbox WHERE id = ?", String.class, committed.value());
        assertThat(objectMapper.readTree(headers).path("traceparent").asText())
                .isEqualTo("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");

        assertThatThrownBy(() -> inTransaction(() -> {
            insertBusinessRecord(1022L);
            tracedPublisher.publish(event(1022L, NOW));
            throw new IntentionalRollbackException();
        })).isInstanceOf(IntentionalRollbackException.class);

        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reliable_event_identity WHERE event_key = ?", Long.class, "1022"))
                .isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reliable_event_outbox WHERE event_key = ?", Long.class, "1022"))
                .isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM test_business_record WHERE id = ?", Long.class, 1022L))
                .isZero();
    }

    @Test
    void publicationAttemptUsesPerAttemptHeaderCopyAndLeavesPersistedHeadersUnchanged() throws Exception {
        String registrationTraceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
        EventId eventId = inTransaction(() -> publisher.publish(new ReliableEvent<>(
                "coupon-task-execute", "m8.2-headers", new CouponTaskPayload(1023L), NOW,
                Map.of("source", "m8.2", "traceparent", registrationTraceparent))));
        List<OutboundEvent> sentEvents = new ArrayList<>();
        List<String> traceResults = new ArrayList<>();
        int[] attemptNumber = {0};
        PublicationTracer tracing = claimed -> {
            try {
                int thisAttempt = ++attemptNumber[0];
                Map<String, String> headers = new java.util.LinkedHashMap<>(objectMapper.readValue(
                        claimed.event().headersJson(), objectMapper.getTypeFactory()
                                .constructMapType(Map.class, String.class, String.class)));
                String spanId = String.format("%016x", thisAttempt);
                headers.put("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-" + spanId + "-01");
                StoredEvent copy = new StoredEvent(claimed.event().id(), claimed.event().eventType(),
                        claimed.event().eventKey(), claimed.event().payloadJson(),
                        objectMapper.writeValueAsString(headers));
                return new PublicationTracer.Attempt() {
                    @Override public StoredEvent eventForSend() { return copy; }
                    @Override public void sendSucceeded(TransportReceipt receipt) { traceResults.add("send:success"); }
                    @Override public void sendFailed(TransportFailureType type, Throwable failure) {
                        traceResults.add("send:" + type.name());
                    }
                    @Override public void stateUpdated(String targetStatus, boolean commitPending) {
                        traceResults.add("state:" + targetStatus);
                    }
                    @Override public void stateUpdateFailed(String targetStatus, Throwable failure,
                                                            boolean ownershipRejected) {
                        traceResults.add("state:" + (ownershipRejected ? "ownership_rejected" : "failed"));
                    }
                    @Override public void close() { traceResults.add("close:" + thisAttempt); }
                };
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        };
        EventTransport sender = sent -> {
            sentEvents.add(sent);
            if (sentEvents.size() == 1) throw new TransportException(TransportFailureType.RESULT_UNKNOWN, "receipt lost");
            return new TransportReceipt(Optional.of("m8.2-message-2"), Map.of());
        };
        JdbcEventPublicationWorker worker = new JdbcEventPublicationWorker(
                publicationStore(CLOCK, WORKER_ID, PublicationObserver.NOOP), sender, CLOCK,
                deterministicBackoff(), PublicationObserver.NOOP, tracing);

        assertThat(publishPending(worker, CLOCK)).isZero();
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at = ? WHERE id = ?",
                Timestamp.from(NOW.minusSeconds(1)), eventId.value());
        assertThat(publishPending(worker, CLOCK)).isOne();

        String persistedHeaders = jdbcTemplate.queryForObject(
                "SELECT headers FROM reliable_event_outbox WHERE id = ?", String.class, eventId.value());
        assertThat(objectMapper.readTree(persistedHeaders).path("traceparent").asText())
                .isEqualTo(registrationTraceparent);
        assertThat(sentEvents).hasSize(2);
        List<String> sentSpanIds = sentEvents.stream().map(sent -> {
            try { return sent.headers().get("traceparent").substring(36, 52); }
            catch (Exception failure) { throw new AssertionError(failure); }
        }).toList();
        assertThat(sentSpanIds).containsExactly("0000000000000001", "0000000000000002");
        assertThat(traceResults).contains("send:RESULT_UNKNOWN", "state:RETRY_WAIT", "send:success", "state:PUBLISHED");
        assertThat(attemptNumber[0]).isEqualTo(2);

        EventCandidate staleCandidate = new EventCandidate(eventId, 0);
        assertThat(publicationStore(CLOCK, WORKER_ID, PublicationObserver.NOOP)
                .claimCandidate(staleCandidate)).isEmpty();
        assertThat(attemptNumber[0]).isEqualTo(2);
    }

    @Test
    void repeatedRegistrationDoesNotReplacePersistedTraceContext() throws Exception {
        JdbcReliableEventPublisher firstContext = new JdbcReliableEventPublisher(
                jdbcTemplate, objectMapper, CLOCK, 8, tracingWith("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"));
        JdbcReliableEventPublisher laterContext = new JdbcReliableEventPublisher(
                jdbcTemplate, objectMapper, CLOCK, 8, tracingWith("traceparent", "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01"));

        EventId first = inTransaction(() -> firstContext.publish(event(1023L, NOW)));
        EventId repeated = inTransaction(() -> laterContext.publish(event(1023L, NOW.plusSeconds(90))));
        assertThat(repeated).isEqualTo(first);
        String headers = jdbcTemplate.queryForObject(
                "SELECT headers FROM reliable_event_outbox WHERE id = ?", String.class, first.value());
        assertThat(objectMapper.readTree(headers).path("traceparent").asText())
                .isEqualTo("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
    }

    private RegistrationTracer tracingWith(String key, String value) {
        return headers -> new RegistrationTracer.Registration() {
            @Override public Map<String, String> headers() {
                Map<String, String> copy = new java.util.LinkedHashMap<>(headers);
                copy.put(key, value);
                return Map.copyOf(copy);
            }
            @Override public void succeeded(EventId eventId) { }
            @Override public void failed(Throwable failure) { }
            @Override public void close() { }
        };
    }

    @Test
    void publishRequiresAnActiveTransaction() {
        assertThatThrownBy(() -> publisher.publish(event(103L, NOW)))
                .isInstanceOf(MissingActiveTransactionException.class)
                .hasMessageContaining("active database transaction");

        assertThat(outboxRowCount()).isZero();
    }

    @Test
    void publishRejectsReadOnlyTransactionWithoutLeavingEventRows() {
        TransactionTemplate readOnlyTransaction = new TransactionTemplate(transactionManager);
        readOnlyTransaction.setReadOnly(true);
        readOnlyTransaction.executeWithoutResult(status ->
                assertThatThrownBy(() -> publisher.publish(event(1031L, NOW)))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("read-only"));
        assertNoEventRows();
    }

    @Test
    void publishRejectsDataSourceOutsideTheBusinessTransaction() {
        JdbcTemplate otherJdbc = independentJdbcTemplate();
        JdbcReliableEventPublisher otherPublisher = new JdbcReliableEventPublisher(otherJdbc, objectMapper);
        inTransaction(() -> {
            assertThatThrownBy(() -> otherPublisher.publish(event(1032L, NOW)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Outbox DataSource");
            return null;
        });
        assertNoEventRows();
    }

    @Test
    void publishRejectsUnrelatedAutoCommitConnectionEvenWhenAlreadyBound() {
        JdbcTemplate otherJdbc = independentJdbcTemplate();
        JdbcReliableEventPublisher otherPublisher = new JdbcReliableEventPublisher(otherJdbc, objectMapper);
        inTransaction(() -> {
            assertThat(otherJdbc.queryForObject("SELECT 1", Integer.class)).isOne();
            assertThat(TransactionSynchronizationManager.hasResource(otherJdbc.getDataSource())).isTrue();
            assertThatThrownBy(() -> otherPublisher.publish(event(1033L, NOW)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("auto-commit");
            return null;
        });
        assertNoEventRows();
    }

    private JdbcTemplate independentJdbcTemplate() {
        // Same database, but a different DataSource and connection from the business transaction.
        return new JdbcTemplate(new DriverManagerDataSource(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword()));
    }

    private void assertNoEventRows() {
        assertThat(outboxRowCount()).isZero();
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reliable_event_identity", Integer.class)).isZero();
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
    void firstAvailabilitySurvivesDuplicateAndRetryAndStatusCountsUseDatabaseRows() {
        Instant firstAvailable = NOW.plusSeconds(60);
        EventId future = inTransaction(() -> publisher.publish(event(140L, firstAvailable)));
        EventId duplicate = inTransaction(() -> publisher.publish(event(140L, NOW.plusSeconds(120))));
        EventId failed = inTransaction(() -> publisher.publish(event(141L, NOW)));
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at = ? WHERE id = ?",
                Timestamp.from(firstAvailable.plusSeconds(30)), future.value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET status = ? WHERE id = ?",
                EventStatus.FAILED.code(), failed.value());

        assertThat(duplicate).isEqualTo(future);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT first_available_at FROM reliable_event_outbox WHERE id = ?",
                Timestamp.class, future.value()).toInstant()).isEqualTo(firstAvailable);
        assertThat(repository.countStatuses().backlog()).isOne();
        assertThat(repository.countStatuses().failed()).isOne();

        EventCandidate due = repository.findDueEventCandidates(firstAvailable.plusSeconds(31), 1).get(0);
        ClaimedEvent claimed = inTransaction(() -> repository.claim(due,
                firstAvailable.plusSeconds(31), WORKER_ID, LEASE_DURATION).orElseThrow());
        assertThat(claimed.firstAvailableAt()).isEqualTo(firstAvailable);
    }

    @Test
    void metricsSnapshotUsesOneDatabaseClockAndKeepsFirstAvailabilityAcrossStateChanges() {
        List<EventId> ids = new ArrayList<>();
        for (long taskId = 2000; taskId < 2008; taskId++) {
            long currentTaskId = taskId;
            ids.add(inTransaction(() -> publisher.publish(
                    event(currentTaskId, Instant.now().minusSeconds(600)))));
        }
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 120 SECOND, "
                + "first_available_at=UTC_TIMESTAMP(3)-INTERVAL 300 SECOND WHERE id=?", ids.get(0).value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 120 SECOND, "
                + "first_available_at=NULL WHERE id=?", ids.get(1).value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)+INTERVAL 1 HOUR, "
                + "first_available_at=UTC_TIMESTAMP(3)-INTERVAL 120 SECOND WHERE id=?", ids.get(2).value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=?, "
                + "first_available_at=UTC_TIMESTAMP(3)-INTERVAL 180 SECOND WHERE id=?",
                EventStatus.PUBLISHING.code(), ids.get(3).value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=?, "
                + "next_attempt_at=UTC_TIMESTAMP(3)+INTERVAL 1 HOUR, "
                + "first_available_at=UTC_TIMESTAMP(3)-INTERVAL 240 SECOND WHERE id=?",
                EventStatus.RETRY_WAIT.code(), ids.get(4).value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)+INTERVAL 1 HOUR, "
                + "first_available_at=UTC_TIMESTAMP(3)+INTERVAL 10 MINUTE WHERE id=?", ids.get(5).value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=?, "
                + "next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND, first_available_at=NULL WHERE id=?",
                EventStatus.RETRY_WAIT.code(), ids.get(6).value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=?, "
                + "first_available_at=UTC_TIMESTAMP(3)-INTERVAL 1 HOUR WHERE id=?",
                EventStatus.FAILED.code(), ids.get(7).value());

        OutboxMetricsSnapshot pending = repository.readMetricsSnapshot(2);
        assertThat(pending.backlog()).isEqualTo(6);
        assertThat(pending.failed()).isOne();
        assertThat(pending.ready()).isEqualTo(3);
        assertThat(pending.readyOldestAgeSeconds()).isBetween(295.0, 315.0);
        assertThat(pending.unfinishedOverdue()).isEqualTo(4);
        assertThat(pending.unfinishedOldestAgeSeconds()).isBetween(295.0, 315.0);
        assertThat(pending.unfinishedTimestampMissing()).isEqualTo(2);
        Timestamp originalFirstAvailable = jdbcTemplate.queryForObject(
                "SELECT first_available_at FROM reliable_event_outbox WHERE id=?", Timestamp.class, ids.get(0).value());

        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=? WHERE id=?",
                EventStatus.PUBLISHING.code(), ids.get(0).value());
        OutboxMetricsSnapshot publishing = repository.readMetricsSnapshot(2);
        assertThat(publishing.ready()).isEqualTo(2);
        assertThat(publishing.unfinishedOverdue()).isEqualTo(4);
        assertThat(publishing.unfinishedOldestAgeSeconds()).isBetween(295.0, 315.0);
        assertThat(jdbcTemplate.queryForObject("SELECT first_available_at FROM reliable_event_outbox WHERE id=?",
                Timestamp.class, ids.get(0).value())).isEqualTo(originalFirstAvailable);

        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=?, "
                + "next_attempt_at=UTC_TIMESTAMP(3)+INTERVAL 20 MINUTE WHERE id=?",
                EventStatus.RETRY_WAIT.code(), ids.get(0).value());
        OutboxMetricsSnapshot retryWait = repository.readMetricsSnapshot(2);
        assertThat(retryWait.ready()).isEqualTo(2);
        assertThat(retryWait.unfinishedOverdue()).isEqualTo(4);
        assertThat(retryWait.unfinishedOldestAgeSeconds()).isBetween(295.0, 315.0);
        assertThat(jdbcTemplate.queryForObject("SELECT first_available_at FROM reliable_event_outbox WHERE id=?",
                Timestamp.class, ids.get(0).value())).isEqualTo(originalFirstAvailable);

        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=? WHERE id=?",
                EventStatus.PUBLISHED.code(), ids.get(0).value());
        OutboxMetricsSnapshot published = repository.readMetricsSnapshot(2);
        assertThat(published.unfinishedOverdue()).isEqualTo(3);
        assertThat(published.unfinishedOldestAgeSeconds()).isBetween(235.0, 255.0);

        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=? WHERE id=?",
                EventStatus.FAILED.code(), ids.get(3).value());
        assertThat(repository.readMetricsSnapshot(2).unfinishedOverdue()).isEqualTo(2);
    }

    @Test
    void metricsSnapshotDistinguishesAllMissingReadyAgeFromNoReadyRows() {
        EventId missing = inTransaction(() -> publisher.publish(event(2100, Instant.now().minusSeconds(60))));
        EventId future = inTransaction(() -> publisher.publish(event(2101, Instant.now().plusSeconds(600))));
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)-INTERVAL 1 SECOND, "
                + "first_available_at=NULL WHERE id=?", missing.value());
        jdbcTemplate.update("UPDATE reliable_event_outbox SET next_attempt_at=UTC_TIMESTAMP(3)+INTERVAL 10 MINUTE, "
                + "first_available_at=UTC_TIMESTAMP(3)+INTERVAL 10 MINUTE WHERE id=?", future.value());

        TimeZone previousZone = TimeZone.getDefault();
        OutboxMetricsSnapshot onlyMissingReady;
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
            onlyMissingReady = repository.readMetricsSnapshot(2);
        } finally {
            TimeZone.setDefault(previousZone);
        }
        assertThat(onlyMissingReady.ready()).isOne();
        assertThat(onlyMissingReady.readyOldestAgeSeconds()).isNaN();
        assertThat(onlyMissingReady.unfinishedOverdue()).isZero();
        assertThat(onlyMissingReady.unfinishedTimestampMissing()).isOne();

        jdbcTemplate.update("UPDATE reliable_event_outbox SET status=? WHERE id=?",
                EventStatus.PUBLISHED.code(), missing.value());
        OutboxMetricsSnapshot noneReady = repository.readMetricsSnapshot(2);
        assertThat(noneReady.ready()).isZero();
        assertThat(noneReady.readyOldestAgeSeconds()).isZero();
        assertThat(noneReady.unfinishedOverdue()).isZero();
        assertThat(noneReady.unfinishedTimestampMissing()).isZero();
    }

    @Test
    void metricsSnapshotExplainUsesExistingOutboxSchemaWithoutAddingAnIndex() {
        String aggregate = """
                SELECT COALESCE(SUM(status IN (0, 3)), 0) AS backlog,
                       COALESCE(SUM(status = 4), 0) AS failed,
                       COALESCE(SUM(status IN (0, 3) AND next_attempt_at <= UTC_TIMESTAMP(3)), 0) AS ready,
                       MIN(CASE WHEN status IN (0, 3) AND next_attempt_at <= UTC_TIMESTAMP(3)
                                THEN first_available_at END) AS ready_oldest,
                       COALESCE(SUM(status IN (0, 1, 3) AND first_available_at <= UTC_TIMESTAMP(3)), 0) AS overdue,
                       MIN(CASE WHEN status IN (0, 1, 3) AND first_available_at <= UTC_TIMESTAMP(3)
                                THEN first_available_at END) AS overdue_oldest,
                       COALESCE(SUM(status IN (0, 1, 3) AND first_available_at IS NULL), 0) AS missing
                FROM reliable_event_outbox
                """;
        Map<String, Object> plan = jdbcTemplate.queryForMap("EXPLAIN " + aggregate);
        List<String> indexes = jdbcTemplate.queryForList("""
                SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'reliable_event_outbox'
                ORDER BY INDEX_NAME
                """, String.class);

        LOG.info("M8.3 snapshot EXPLAIN: type={}, key={}, rows={}, extra={}, indexes={}",
                plan.get("type"), plan.get("key"), plan.get("rows"), plan.get("Extra"), indexes);
        assertThat(indexes).contains("idx_publish_scan", "idx_lease_recovery",
                "idx_published_retention");
        assertThat(plan.get("type")).isEqualTo("ALL");
    }

    @Test
    void metricsSnapshotStatementTimeoutInterruptsARealBlockedMysqlQuery() throws Exception {
        try (Connection blocker = jdbcTemplate.getDataSource().getConnection();
             Statement lock = blocker.createStatement()) {
            lock.execute("LOCK TABLES reliable_event_outbox WRITE");
            long started = System.nanoTime();
            try {
                assertThatThrownBy(() -> repository.readMetricsSnapshot(1))
                        .isInstanceOf(org.springframework.jdbc.CannotGetJdbcConnectionException.class);
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                assertThat(elapsedMillis).isBetween(750L, 5000L);
            } finally {
                lock.execute("UNLOCK TABLES");
            }
        }
    }

    @Test
    void publishedMetricsWaitForCommitAndFallbackRejectsOuterTransactions() {
        AtomicInteger persisted = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        PublicationObserver observer = new PublicationObserver() {
            @Override public void stateTransitionCommitted(String status) {
                if ("PUBLISHED".equals(status)) persisted.incrementAndGet();
                if ("FAILED".equals(status)) failed.incrementAndGet();
            }
        };

        ClaimedEvent published = claimedPublishedEvent(2200L);
        new TransactionTemplate(transactionManager).execute(status -> {
            assertThat(worker(event -> new TransportReceipt(Optional.of("persisted"), Map.of()), observer)
                    .publishClaimedEvent(published)).isTrue();
            assertThat(persisted).hasValue(0);
            return null;
        });
        assertThat(persisted).hasValue(1);

        ClaimedEvent rolledBackPublish = claimedPublishedEvent(2201L);
        new TransactionTemplate(transactionManager).execute(status -> {
            assertThat(worker(event -> new TransportReceipt(Optional.of("rolled-back"), Map.of()), observer)
                    .publishClaimedEvent(rolledBackPublish)).isTrue();
            status.setRollbackOnly();
            return null;
        });
        assertThat(persisted).hasValue(1);
        assertThat(statusOf(rolledBackPublish.event().id())).isEqualTo(EventStatus.PUBLISHING.code());

        ClaimedEvent terminal = claimedPublishedEvent(2202L);
        assertThatThrownBy(() -> new TransactionTemplate(transactionManager).execute(status ->
                worker(event -> { throw new TransportException(TransportFailureType.NON_RETRYABLE, "invalid"); }, observer)
                        .publishClaimedEvent(terminal)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("outside an active transaction");
        assertThat(statusOf(terminal.event().id())).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(failed).hasValue(0);

        ExpiredLeaseCandidate recoveredTerminal = expiredTerminalCandidate(2204L);
        JdbcExpiredLeaseRecovery recovery = new JdbcExpiredLeaseRecovery(jdbcTemplate, transactionManager,
                100, deterministicBackoff(), observer);
        assertThatThrownBy(() -> new TransactionTemplate(transactionManager).execute(status ->
                recovery.recoverCandidates(List.of(recoveredTerminal))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("outside an active transaction");
        assertThat(failed).hasValue(0);
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void observationFailureDoesNotChangePublishedStateOrLeakMessageContent(CapturedOutput output) {
        EventId id = inTransaction(() -> publisher.publish(new ReliableEvent<>(
                "safe-type", "safe-key", Map.of("secret", "payload-sentinel"),
                NOW.minusSeconds(1), Map.of("token", "header-sentinel"))));
        PublicationObserver broken = new PublicationObserver() {
            @Override
            public void sendSucceeded(ClaimedEvent event, TransportReceipt receipt,
                                      long elapsedNanos, Instant at) {
                throw new IllegalStateException("observer-sentinel");
            }
        };
        JdbcEventPublicationWorker observedWorker = new JdbcEventPublicationWorker(
                publicationStore(CLOCK, WORKER_ID, broken),
                event -> new TransportReceipt(Optional.of("message-safe"), Map.of()),
                CLOCK, deterministicBackoff(), broken, PublicationTracer.NOOP);

        assertThat(publishPending(observedWorker, CLOCK)).isOne();
        assertThat(statusOf(id)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(output.getOut()).contains("event=reliable_event.send.succeeded",
                "eventId=" + id.value(), "eventType=safe-type", "eventKey=safe-key",
                "messageId=message-safe");
        assertThat(output.getOut()).doesNotContain("payload-sentinel", "header-sentinel",
                "observer-sentinel");
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void confirmedReceiptWithoutBrokerIdIsPublishedAndDoesNotLogAnId(CapturedOutput output) {
        EventId id = inTransaction(() -> publisher.publish(new ReliableEvent<>(
                "safe-type", "no-broker-id", Map.of("orderId", 7), NOW.minusSeconds(1), Map.of())));
        JdbcEventPublicationWorker worker = new JdbcEventPublicationWorker(
                publicationStore(CLOCK, WORKER_ID, PublicationObserver.NOOP),
                event -> TransportReceipt.confirmed(), CLOCK, deterministicBackoff(),
                PublicationObserver.NOOP, PublicationTracer.NOOP);

        assertThat(publishPending(worker, CLOCK)).isOne();
        assertThat(statusOf(id)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(output.getOut()).contains("event=reliable_event.send.succeeded", "status=PUBLISHED")
                .doesNotContain("messageId=null", "messageId=");
    }

    @Test
    void migrationKeepsHistoricalAvailabilityUnknown() throws Exception {
        jdbcTemplate.execute("CREATE TABLE reliable_event_outbox_legacy (" +
                "id BIGINT PRIMARY KEY, next_attempt_at DATETIME(3) NOT NULL)");
        try {
            jdbcTemplate.update("INSERT INTO reliable_event_outbox_legacy VALUES (?, ?)",
                    1L, Timestamp.from(NOW));
            String migration = new String(new ClassPathResource(
                    "schema/reliable-event-outbox-m4-5.sql").getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8).replace("reliable_event_outbox\n",
                    "reliable_event_outbox_legacy\n");
            jdbcTemplate.execute(migration);

            assertThat(jdbcTemplate.queryForObject(
                    "SELECT first_available_at FROM reliable_event_outbox_legacy WHERE id = 1",
                    Timestamp.class)).isNull();
        } finally {
            jdbcTemplate.execute("DROP TABLE reliable_event_outbox_legacy");
        }
    }

        @Test
    void candidateCanBeClaimedOnlyOnceWithItsOriginalVersion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(105L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);

        assertThat(candidate.id()).isEqualTo(eventId);
        assertThat(candidate.version()).isZero();

        Instant databaseTimeBeforeClaim = databaseNow();
        ClaimedEvent claimedEvent = inTransaction(
                () -> claim(candidate, WORKER_ID).orElseThrow()
        );
        Instant databaseTimeAfterClaim = databaseNow();

        assertThat(claimedEvent.event().id()).isEqualTo(eventId);
        assertThat(claimedEvent.claimVersion()).isOne();
        assertThat(claimedEvent.attemptCount()).isOne();
        assertThat(claimedEvent.leaseOwner()).isEqualTo(WORKER_ID);
        assertThat(claimedEvent.leaseUntil()).isBetween(
                databaseTimeBeforeClaim.plus(LEASE_DURATION),
                databaseTimeAfterClaim.plus(LEASE_DURATION)
        );
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isOne();
        assertThat(leaseOwnerOf(eventId)).isEqualTo(WORKER_ID);
        assertThat(leaseUntilOf(eventId)).isEqualTo(claimedEvent.leaseUntil());

        assertThat(inTransaction(() -> claim(candidate, WORKER_ID))).isEmpty();
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isOne();
    }

    @Test
    void claimRechecksThatCandidateIsStillDue() {
        EventId eventId = inTransaction(() -> publisher.publish(event(106L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET next_attempt_at = ? WHERE id = ?",
                Timestamp.from(NOW.plusSeconds(60)),
                eventId.value()
        );

        assertThat(inTransaction(() -> claim(candidate, WORKER_ID))).isEmpty();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PENDING.code());
        assertThat(attemptCountOf(eventId)).isZero();
        assertThat(versionOf(eventId)).isZero();
    }

    @Test
    void publishedUpdateRejectsAStaleClaimVersion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(107L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> claim(candidate, WORKER_ID).orElseThrow()
        );
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET version = version + 1 WHERE id = ?",
                eventId.value()
        );

        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markPublished(claimedEvent, NOW);
            return null;
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("claim is no longer current");

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(publishedAtOf(eventId)).isNull();
    }

    @Test
    void stateUpdatesRejectAnIncorrectLeaseOwner() {
        EventId eventId = inTransaction(() -> publisher.publish(event(111L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> claim(candidate, WORKER_ID).orElseThrow()
        );
        ClaimedEvent incorrectOwnerClaim = new ClaimedEvent(
                claimedEvent.event(),
                claimedEvent.claimVersion(),
                claimedEvent.attemptCount(),
                claimedEvent.maxAttempts(),
                SECOND_WORKER_ID,
                claimedEvent.leaseUntil()
        );

        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markPublished(incorrectOwnerClaim, NOW);
            return null;
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("leaseOwner=" + SECOND_WORKER_ID);
        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markRetryWait(
                    incorrectOwnerClaim,
                    NOW,
                    NOW.plusSeconds(1),
                    "temporary failure"
            );
            return null;
        })).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> publicationStore(CLOCK, WORKER_ID, PublicationObserver.NOOP).beginFallback(
                incorrectOwnerClaim, "permanent failure", "NON_RETRYABLE", "NON_RETRYABLE"))
                .isInstanceOf(IllegalStateException.class);

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(versionOf(eventId)).isOne();
        assertThat(leaseOwnerOf(eventId)).isEqualTo(WORKER_ID);
        assertThat(leaseUntilOf(eventId)).isNotNull();
        assertThat(publishedAtOf(eventId)).isNull();
        assertThat(lastErrorOf(eventId)).isNull();
    }

    @Test
    void stateUpdatesRejectAnExpiredLease() {
        EventId eventId = inTransaction(() -> publisher.publish(event(112L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> claim(candidate, WORKER_ID).orElseThrow()
        );
        jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(3))
                WHERE id = ?
                """,
                eventId.value()
        );

        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markPublished(claimedEvent, NOW);
            return null;
        })).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markRetryWait(
                    claimedEvent,
                    NOW,
                    NOW.plusSeconds(1),
                    "temporary failure"
            );
            return null;
        })).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> publicationStore(CLOCK, WORKER_ID, PublicationObserver.NOOP).beginFallback(
                claimedEvent, "permanent failure", "NON_RETRYABLE", "NON_RETRYABLE"))
                .isInstanceOf(IllegalStateException.class);

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(versionOf(eventId)).isOne();
        assertThat(leaseOwnerOf(eventId)).isEqualTo(WORKER_ID);
        assertThat(leaseUntilOf(eventId)).isBeforeOrEqualTo(databaseNow());
        assertThat(publishedAtOf(eventId)).isNull();
        assertThat(lastErrorOf(eventId)).isNull();
    }

    @Test
    void retryUpdateRejectsAStaleClaimVersion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(108L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> claim(candidate, WORKER_ID).orElseThrow()
        );
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET version = version + 1 WHERE id = ?",
                eventId.value()
        );

        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markRetryWait(
                    claimedEvent,
                    NOW,
                    NOW.plusSeconds(1),
                    "dev.reliableevent.spi.TransportException: broker unavailable"
            );
            return null;
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("claim is no longer current");

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(lastErrorOf(eventId)).isNull();
    }

    @Test
    void fallbackStartRejectsAStaleClaimVersion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(109L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> claim(candidate, WORKER_ID).orElseThrow()
        );
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET version = version + 1 WHERE id = ?",
                eventId.value()
        );

        assertThatThrownBy(() -> publicationStore(CLOCK, WORKER_ID, PublicationObserver.NOOP).beginFallback(
                    claimedEvent, TransportException.class.getName() + ": missing destination",
                    "NON_RETRYABLE", "NON_RETRYABLE"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("claim is no longer current");

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(lastErrorOf(eventId)).isNull();
    }

    @Test
    void expiredFallbackLeaseIsReclaimedOnlyForFallbackAndRejectsOldCompletion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(110L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).stream()
                .filter(value -> value.id().equals(eventId)).findFirst().orElseThrow();
        ClaimedEvent original = inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        ClaimedEvent oldFallbackClaim = publicationStore(CLOCK, WORKER_ID, PublicationObserver.NOOP)
                .beginFallback(original, "original rejection", "NON_RETRYABLE", "NON_RETRYABLE");
        expireLease(eventId, 1);

        List<EventFallbackContext> handled = new ArrayList<>();
        EventFallbackHandler handler = handled::add;
        JdbcExpiredLeaseRecovery recovery = new JdbcExpiredLeaseRecovery(jdbcTemplate, transactionManager,
                20, deterministicBackoff(), PublicationObserver.NOOP, LEASE_DURATION, handler);
        assertThat(recovery.recoverExpiredLeases()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FALLBACK_COMPLETED.code());
        assertThat(handled).singleElement().satisfies(context -> {
            assertThat(context.stopReason()).isEqualTo(EventFallbackContext.StopReason.NON_RETRYABLE);
            assertThat(context.failureClassification())
                    .isEqualTo(EventFallbackContext.FailureClassification.NON_RETRYABLE);
        });
        assertThatThrownBy(() -> publicationStore(CLOCK, WORKER_ID, PublicationObserver.NOOP)
                .finishFallback(oldFallbackClaim, true, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("claim is no longer current");
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FALLBACK_COMPLETED.code());
        assertThat(publishedAtOf(eventId)).isNull();
    }

    @Test
    void exhaustedRetryWaitCannotBeScannedOrClaimed() {
        ReliableEventPublisher oneAttemptPublisher = publisherWithMaxAttempts(1);
        EventId eventId = inTransaction(
                () -> oneAttemptPublisher.publish(event(110L, NOW.minusSeconds(1)))
        );
        jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?, attempt_count = 1, version = 1
                WHERE id = ?
                """,
                EventStatus.RETRY_WAIT.code(),
                eventId.value()
        );

        assertThat(repository.findDueEventCandidates(NOW, 50)).isEmpty();
        EventCandidate exhaustedCandidate = new EventCandidate(eventId, 1L);
        assertThat(inTransaction(() -> claim(exhaustedCandidate, WORKER_ID))).isEmpty();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isOne();
    }

    @Test
    void workerPublishesOnlyEventsWhoseAvailableTimeHasArrived() {
        EventId dueEvent = inTransaction(() -> publisher.publish(event(201L, NOW.minusSeconds(1))));
        EventId futureEvent = inTransaction(() -> publisher.publish(event(202L, NOW.plusSeconds(60))));
        FakeEventTransport sender = new FakeEventTransport();
        JdbcEventPublicationWorker worker = worker(sender, CLOCK, deterministicBackoff());

        int publishedCount = publishPending(worker, CLOCK);

        assertThat(publishedCount).isOne();
        assertThat(sender.sentEvents)
                .extracting(OutboundEvent::eventKey)
                .containsExactly("201");
        assertThat(statusOf(dueEvent)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(statusOf(futureEvent)).isEqualTo(EventStatus.PENDING.code());
        assertThat(attemptCountOf(dueEvent)).isOne();
        assertThat(attemptCountOf(futureEvent)).isZero();
        assertThat(versionOf(dueEvent)).isEqualTo(2L);
        assertThat(versionOf(futureEvent)).isZero();
        assertThat(publishedAtOf(dueEvent)).isNotNull();
        assertThat(publishedAtOf(futureEvent)).isNull();
        assertThat(leaseOwnerOf(dueEvent)).isNull();
        assertThat(leaseUntilOf(dueEvent)).isNull();
        assertThat(sender.transactionActiveDuringSend).isFalse();
    }

    @Test
    void twoWorkersCompetingForTheSameCandidatePublishOnlyOnce() throws Exception {
        EventId eventId = inTransaction(
                () -> publisher.publish(event(203L, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ConcurrentFakeEventTransport sender = new ConcurrentFakeEventTransport();
        JdbcEventPublicationWorker firstWorker = worker(sender, WORKER_ID);
        JdbcEventPublicationWorker secondWorker = worker(sender, SECOND_WORKER_ID);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        int firstPublished;
        int secondPublished;
        try {
            Future<Integer> firstResult = executor.submit(
                    () -> publishAfterSignal(firstWorker, candidate, WORKER_ID, ready, start)
            );
            Future<Integer> secondResult = executor.submit(
                    () -> publishAfterSignal(secondWorker, candidate, SECOND_WORKER_ID, ready, start)
            );

            assertThat(ready.await(5, TimeUnit.SECONDS))
                    .as("both workers became ready")
                    .isTrue();
            start.countDown();

            firstPublished = firstResult.get(10, TimeUnit.SECONDS);
            secondPublished = secondResult.get(10, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("worker executor terminated")
                    .isTrue();
        }

        assertThat(List.of(firstPublished, secondPublished))
                .containsExactlyInAnyOrder(0, 1);
        assertThat(sender.sendCount()).isOne();
        assertThat(sender.sentEvents())
                .extracting(OutboundEvent::id)
                .containsExactly(eventId);
        assertThat(sender.sentEvents())
                .extracting(OutboundEvent::eventKey)
                .containsExactly("203");
        assertThat(outboxRowCount()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(publishedAtOf(eventId)).isNotNull();
    }

    @Test
    void failedEventWaitsUntilRetryTimeAndThenPublishes() {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(204L, NOW.minusSeconds(1)))
        );
        FailOnceEventTransport sender = new FailOnceEventTransport();
        ExponentialBackoff backoff = deterministicBackoff();

        int firstPublished = publishPending(worker(sender, CLOCK, backoff), CLOCK);

        assertThat(firstPublished).isZero();
        assertThat(sender.sendCount()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(nextAttemptAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
        assertThat(lastErrorOf(eventId))
                .isEqualTo("dev.reliableevent.spi.TransportException: broker unavailable");
        assertThat(publishedAtOf(eventId)).isNull();

        Clock beforeRetry = Clock.fixed(NOW.plusMillis(999), ZoneOffset.UTC);
        int earlyPublished = publishPending(worker(sender, beforeRetry, backoff), beforeRetry);

        assertThat(earlyPublished).isZero();
        assertThat(sender.sendCount()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);

        Clock atRetry = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);
        int retryPublished = publishPending(worker(sender, atRetry, backoff), atRetry);

        assertThat(retryPublished).isOne();
        assertThat(sender.sendCount()).isEqualTo(2);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(4L);
        assertThat(publishedAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));
        assertThat(lastErrorOf(eventId))
                .isEqualTo("dev.reliableevent.spi.TransportException: broker unavailable");
    }

    @Test
    void failedEventDoesNotStopLaterCandidatesInTheSameBatch() {
        EventId failedEvent = inTransaction(
                () -> publisher.publish(event(205L, NOW.minusSeconds(1)))
        );
        EventId publishedEvent = inTransaction(
                () -> publisher.publish(event(206L, NOW.minusSeconds(1)))
        );
        SelectiveFailureEventTransport sender = new SelectiveFailureEventTransport("205");

        int publishedCount = publishPending(worker(sender, CLOCK, deterministicBackoff()), CLOCK);

        assertThat(publishedCount).isOne();
        assertThat(sender.attemptedEventKeys()).containsExactly("205", "206");
        assertThat(statusOf(failedEvent)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(statusOf(publishedEvent)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(failedEvent)).isOne();
        assertThat(attemptCountOf(publishedEvent)).isOne();
        assertThat(versionOf(failedEvent)).isEqualTo(2L);
        assertThat(versionOf(publishedEvent)).isEqualTo(2L);
    }

    @Test
    void retryableFailureFallsBackAfterLastAllowedAttempt() {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(207L, NOW.minusSeconds(1)))
        );
        AlwaysFailingEventTransport sender = new AlwaysFailingEventTransport();
        ExponentialBackoff backoff = deterministicBackoff();

        assertThat(publishPending(worker(sender, CLOCK, backoff), CLOCK)).isZero();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(nextAttemptAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));

        Clock atRetry = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);
        assertThat(publishPending(worker(sender, atRetry, backoff), atRetry)).isZero();

        assertThat(sender.sendCount()).isEqualTo(2);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FAILED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(5L);
        assertThat(nextAttemptAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));
        assertThat(lastErrorOf(eventId)).contains("No EventFallbackHandler", "temporary failure 2");
        assertThat(publishedAtOf(eventId)).isNull();

        Clock longAfterRetry = Clock.fixed(NOW.plusSeconds(3_600), ZoneOffset.UTC);
        assertThat(publishPending(worker(sender, longAfterRetry, backoff), longAfterRetry)).isZero();
        assertThat(sender.sendCount()).isEqualTo(2);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FAILED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(5L);
    }

    @Test
    void unknownSendResultRetriesAndFallsBackAfterLastAllowedAttempt() {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(218L, NOW.minusSeconds(1)))
        );
        ResultUnknownEventTransport sender = new ResultUnknownEventTransport();
        ExponentialBackoff backoff = deterministicBackoff();

        assertThat(publishPending(worker(sender, CLOCK, backoff), CLOCK)).isZero();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(nextAttemptAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));

        Clock atRetry = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);
        assertThat(publishPending(worker(sender, atRetry, backoff), atRetry)).isZero();

        assertThat(sender.sendCount()).isEqualTo(2);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FAILED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(5L);
        assertThat(lastErrorOf(eventId)).contains("No EventFallbackHandler", "RocketMQ response lost");
    }

    @Test
    void nonRetryableFailureFallsBackWithoutComputingBackoff() {
        EventId eventId = inTransaction(
                () -> publisher.publish(event(208L, NOW.minusSeconds(1)))
        );
        NonRetryableEventTransport sender = new NonRetryableEventTransport();

        ExponentialBackoff backoff = org.mockito.Mockito.mock(ExponentialBackoff.class);

        assertThat(publishPending(worker(sender, CLOCK, backoff), CLOCK)).isZero();

        assertThat(sender.sendCount()).isOne();
        org.mockito.Mockito.verifyNoInteractions(backoff);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FAILED.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(3L);
        assertThat(lastErrorOf(eventId)).contains("No EventFallbackHandler", "missing destination");
        assertThat(publishedAtOf(eventId)).isNull();
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
    }

    @Test
    void storeRejectsInvalidLeaseConfiguration() {
        assertThatThrownBy(() -> publicationStore(CLOCK, " ", PublicationObserver.NOOP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workerId");
        assertThatThrownBy(() -> publicationStore(CLOCK, "x".repeat(129), PublicationObserver.NOOP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("128");
        assertThatThrownBy(() -> new JdbcPublicationStore(jdbcTemplate, transactionManager, CLOCK,
                WORKER_ID, Duration.ZERO, PublicationObserver.NOOP)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseDuration");
        assertThatThrownBy(() -> new JdbcPublicationStore(jdbcTemplate, transactionManager, CLOCK,
                WORKER_ID, Duration.ofMillis(-1), PublicationObserver.NOOP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseDuration");
        assertThatThrownBy(() -> new JdbcPublicationStore(jdbcTemplate, transactionManager, CLOCK,
                WORKER_ID, null, PublicationObserver.NOOP)).isInstanceOf(NullPointerException.class)
                .hasMessageContaining("leaseDuration");
    }

    @Test
    void activeLeaseIsNotRecovered() {
        EventId eventId = inTransaction(
                () -> publisher.publish(event(301L, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> claim(candidate, WORKER_ID).orElseThrow()
        );

        assertThat(repository.findExpiredLeaseCandidates(50)).isEmpty();
        assertThat(recovery(50, deterministicBackoff()).recoverExpiredLeases()).isZero();

        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isOne();
        assertThat(leaseOwnerOf(eventId)).isEqualTo(WORKER_ID);
        assertThat(leaseUntilOf(eventId)).isEqualTo(claimedEvent.leaseUntil());
    }

    @Test
    void expiredLeaseWaitsForBackoffAndCanThenBeClaimedAgain() {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(302L, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        expireLease(eventId, 1);

        List<ExpiredLeaseCandidate> expiredCandidates =
                repository.findExpiredLeaseCandidates(50);
        assertThat(expiredCandidates).hasSize(1);
        ExpiredLeaseCandidate expiredCandidate = expiredCandidates.get(0);
        assertThat(expiredCandidate.id()).isEqualTo(eventId);
        assertThat(expiredCandidate.version()).isOne();
        assertThat(expiredCandidate.leaseOwner()).isEqualTo(WORKER_ID);
        assertThat(expiredCandidate.leaseUntil()).isEqualTo(leaseUntilOf(eventId));
        assertThat(expiredCandidate.attemptCount()).isOne();
        assertThat(expiredCandidate.maxAttempts()).isEqualTo(2);

        Instant databaseTimeBeforeRecovery = databaseNow();
        int recoveredCount = recovery(50, deterministicBackoff()).recoverExpiredLeases();
        Instant databaseTimeAfterRecovery = databaseNow();

        assertThat(recoveredCount).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
        assertThat(lastErrorOf(eventId)).isEqualTo(JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR);
        assertThat(publishedAtOf(eventId)).isNull();

        Instant nextAttemptAt = nextAttemptAtOf(eventId);
        assertThat(nextAttemptAt).isBetween(
                databaseTimeBeforeRecovery.plusSeconds(1),
                databaseTimeAfterRecovery.plusSeconds(1)
        );
        assertThat(repository.findDueEventCandidates(
                nextAttemptAt.minusMillis(1),
                50
        )).isEmpty();

        EventCandidate retryCandidate = repository.findDueEventCandidates(
                nextAttemptAt,
                50
        ).get(0);
        ClaimedEvent reclaimedEvent = inTransaction(() -> repository.claim(
                retryCandidate,
                nextAttemptAt,
                SECOND_WORKER_ID,
                LEASE_DURATION
        ).orElseThrow());

        assertThat(reclaimedEvent.attemptCount()).isEqualTo(2);
        assertThat(reclaimedEvent.claimVersion()).isEqualTo(3L);
        assertThat(reclaimedEvent.leaseOwner()).isEqualTo(SECOND_WORKER_ID);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
    }

    @Test
    void expiredLeaseFallsBackWhenAttemptsAreExhausted() {
        ReliableEventPublisher oneAttemptPublisher = publisherWithMaxAttempts(1);
        EventId eventId = inTransaction(
                () -> oneAttemptPublisher.publish(event(303L, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        expireLease(eventId, 1);

        ExponentialBackoff backoff = org.mockito.Mockito.mock(ExponentialBackoff.class);

        int recoveredCount = recovery(50, backoff).recoverExpiredLeases();

        assertThat(recoveredCount).isOne();
        org.mockito.Mockito.verifyNoInteractions(backoff);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FAILED.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(3L);
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
        assertThat(lastErrorOf(eventId)).contains("No EventFallbackHandler",
                JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR);
        assertThat(publishedAtOf(eventId)).isNull();
        assertThat(repository.findDueEventCandidates(
                databaseNow().plusSeconds(3_600),
                50
        )).isEmpty();
    }

    @Test
    void expiredPublishingLeaseFallbackReportsUnknownBrokerResult() {
        ReliableEventPublisher oneAttemptPublisher = publisherWithMaxAttempts(1);
        EventId eventId = inTransaction(() -> oneAttemptPublisher.publish(event(304L, NOW.minusSeconds(1))));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).stream()
                .filter(value -> value.id().equals(eventId)).findFirst().orElseThrow();
        inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        expireLease(eventId, 1);
        List<EventFallbackContext> handled = new ArrayList<>();
        JdbcExpiredLeaseRecovery recovery = new JdbcExpiredLeaseRecovery(jdbcTemplate, transactionManager,
                20, deterministicBackoff(), PublicationObserver.NOOP, LEASE_DURATION, handled::add);

        assertThat(recovery.recoverExpiredLeases()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FALLBACK_COMPLETED.code());
        assertThat(handled).singleElement().satisfies(context -> {
            assertThat(context.stopReason()).isEqualTo(EventFallbackContext.StopReason.LEASE_EXPIRED);
            assertThat(context.failureClassification())
                    .isEqualTo(EventFallbackContext.FailureClassification.RESULT_UNKNOWN);
        });
        assertThat(publishedAtOf(eventId)).isNull();
    }

    @Test
    void leaseRecoveryObserverReportsCommittedTargetAndPreservesLegacyCallbacks() {
        List<Boolean> legacyResults = new ArrayList<>();
        PublicationObserver legacyObserver = new PublicationObserver() {
            @Override public void leaseRecovered(ExpiredLeaseCandidate candidate, boolean failed) {
                legacyResults.add(failed);
            }
        };
        ExpiredLeaseCandidate retryable = expiredLeaseCandidate(305L);
        JdbcExpiredLeaseRecovery retryRecovery = new JdbcExpiredLeaseRecovery(jdbcTemplate, transactionManager,
                20, deterministicBackoff(), legacyObserver, LEASE_DURATION, null);

        new TransactionTemplate(transactionManager).execute(status -> {
            assertThat(retryRecovery.recoverCandidates(List.of(retryable))).isOne();
            assertThat(legacyResults).isEmpty();
            return null;
        });
        assertThat(legacyResults).containsExactly(false);
        assertThat(retryRecovery.recoverCandidates(List.of(retryable))).isZero();
        assertThat(legacyResults).containsExactly(false);

        ExpiredLeaseCandidate terminal = expiredTerminalCandidate(306L);
        JdbcExpiredLeaseRecovery failedRecovery = new JdbcExpiredLeaseRecovery(jdbcTemplate, transactionManager,
                20, deterministicBackoff(), legacyObserver, LEASE_DURATION, null);
        assertThat(failedRecovery.recoverCandidates(List.of(terminal))).isOne();
        assertThat(legacyResults).containsExactly(false, true);

        ExpiredLeaseCandidate completed = expiredTerminalCandidate(307L);
        List<String> targetStates = new ArrayList<>();
        PublicationObserver statusObserver = new PublicationObserver() {
            @Override public void leaseRecovered(ExpiredLeaseCandidate candidate, String targetStatus) {
                targetStates.add(targetStatus);
            }
        };
        JdbcExpiredLeaseRecovery completedRecovery = new JdbcExpiredLeaseRecovery(jdbcTemplate, transactionManager,
                20, deterministicBackoff(), statusObserver, LEASE_DURATION, context -> { });
        assertThat(completedRecovery.recoverCandidates(List.of(completed))).isOne();
        assertThat(targetStates).containsExactly("FALLBACK_COMPLETED");
    }

    @Test
    void staleExpiredLeaseCandidatesCannotOverwriteChangedClaims() {
        ExpiredLeaseCandidate recoveredOnce = expiredLeaseCandidate(314L);
        assertThat(inTransaction(() -> repository.recoverExpiredLeaseToRetryWait(
                recoveredOnce,
                Duration.ofSeconds(1),
                JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR
        ))).isTrue();
        assertThat(inTransaction(() -> repository.recoverExpiredLeaseToRetryWait(
                recoveredOnce,
                Duration.ofSeconds(1),
                JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR
        ))).isFalse();
        assertThat(statusOf(recoveredOnce.id())).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(versionOf(recoveredOnce.id())).isEqualTo(2L);

        ExpiredLeaseCandidate changedVersion = expiredLeaseCandidate(304L);
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET version = version + 1 WHERE id = ?",
                changedVersion.id().value()
        );

        ExpiredLeaseCandidate changedOwner = expiredLeaseCandidate(305L);
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET lease_owner = ? WHERE id = ?",
                SECOND_WORKER_ID,
                changedOwner.id().value()
        );

        ExpiredLeaseCandidate extendedLease = expiredLeaseCandidate(306L);
        jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET lease_until = TIMESTAMPADD(SECOND, 30, UTC_TIMESTAMP(3))
                WHERE id = ?
                """,
                extendedLease.id().value()
        );

        ExpiredLeaseCandidate changedExpiredDeadline = expiredLeaseCandidate(307L);
        jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET lease_until = TIMESTAMPADD(SECOND, -2, UTC_TIMESTAMP(3))
                WHERE id = ?
                """,
                changedExpiredDeadline.id().value()
        );

        for (ExpiredLeaseCandidate staleCandidate : List.of(
                changedVersion,
                changedOwner,
                extendedLease,
                changedExpiredDeadline
        )) {
            assertThat(inTransaction(() -> repository.recoverExpiredLeaseToRetryWait(
                    staleCandidate,
                    Duration.ofSeconds(1),
                    JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR
            ))).isFalse();
            assertThat(statusOf(staleCandidate.id()))
                    .isEqualTo(EventStatus.PUBLISHING.code());
        }
    }

    @Test
    void expiredLeaseRecoveryHonorsBatchSizeAndOrdering() {
        EventId earliest = claimedEventWithExpiredLease(308L, 3);
        EventId middle = claimedEventWithExpiredLease(309L, 2);
        EventId latest = claimedEventWithExpiredLease(310L, 1);

        int firstRecovered = recovery(2, deterministicBackoff()).recoverExpiredLeases();

        assertThat(firstRecovered).isEqualTo(2);
        assertThat(statusOf(earliest)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(statusOf(middle)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(statusOf(latest)).isEqualTo(EventStatus.PUBLISHING.code());

        int secondRecovered = recovery(2, deterministicBackoff()).recoverExpiredLeases();

        assertThat(secondRecovered).isOne();
        assertThat(statusOf(latest)).isEqualTo(EventStatus.RETRY_WAIT.code());
    }

    @Test
    void incompleteLeaseRowsAreNotRecoveredAutomatically() {
        EventId missingOwner = claimedEventWithExpiredLease(311L, 1);
        EventId blankOwner = claimedEventWithExpiredLease(312L, 1);
        EventId missingDeadline = claimedEventWithExpiredLease(313L, 1);
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET lease_owner = NULL WHERE id = ?",
                missingOwner.value()
        );
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET lease_owner = '   ' WHERE id = ?",
                blankOwner.value()
        );
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET lease_until = NULL WHERE id = ?",
                missingDeadline.value()
        );

        assertThat(repository.findExpiredLeaseCandidates(50)).isEmpty();
        assertThat(recovery(50, deterministicBackoff()).recoverExpiredLeases()).isZero();
        assertThat(statusOf(missingOwner)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(statusOf(blankOwner)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(statusOf(missingDeadline)).isEqualTo(EventStatus.PUBLISHING.code());
    }

    @Test
    void twoRecoverersCompetingForTheSameRetryableSnapshotRecoverOnlyOnce()
            throws Exception {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(317L, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        expireLease(eventId, 1);
        List<ExpiredLeaseCandidate> snapshot =
                List.copyOf(repository.findExpiredLeaseCandidates(50));
        assertThat(snapshot).hasSize(1);

        CyclicBarrier transactionStart = new CyclicBarrier(2);
        JdbcExpiredLeaseRecovery firstRecovery = recovery(
                50,
                deterministicBackoff()
        );
        JdbcExpiredLeaseRecovery secondRecovery = recovery(
                50,
                deterministicBackoff()
        );
        ExecutorService executor = Executors.newFixedThreadPool(2);

        int firstRecovered;
        int secondRecovered;
        try {
            Future<Integer> firstResult = executor.submit(
                    () -> {
                        transactionStart.await(5, TimeUnit.SECONDS);
                        return firstRecovery.recoverCandidates(snapshot);
                    }
            );
            Future<Integer> secondResult = executor.submit(
                    () -> {
                        transactionStart.await(5, TimeUnit.SECONDS);
                        return secondRecovery.recoverCandidates(snapshot);
                    }
            );

            firstRecovered = firstResult.get(10, TimeUnit.SECONDS);
            secondRecovered = secondResult.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("recovery executor terminated")
                    .isTrue();
        }

        assertThat(List.of(firstRecovered, secondRecovered))
                .containsExactlyInAnyOrder(0, 1);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
        assertThat(lastErrorOf(eventId))
                .isEqualTo(JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR);
    }

    @Test
    void twoRecoverersCompetingForTheSameExhaustedSnapshotFailOnlyOnce()
            throws Exception {
        ReliableEventPublisher oneAttemptPublisher = publisherWithMaxAttempts(1);
        EventId eventId = inTransaction(
                () -> oneAttemptPublisher.publish(event(318L, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        expireLease(eventId, 1);
        List<ExpiredLeaseCandidate> snapshot =
                List.copyOf(repository.findExpiredLeaseCandidates(50));
        assertThat(snapshot).hasSize(1);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        ExponentialBackoff backoff = org.mockito.Mockito.mock(ExponentialBackoff.class);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        int firstRecovered;
        int secondRecovered;
        try {
            Future<Integer> firstResult = executor.submit(() -> recoverAfterSignal(
                    recovery(50, backoff), snapshot, ready, start
            ));
            Future<Integer> secondResult = executor.submit(() -> recoverAfterSignal(
                    recovery(50, backoff), snapshot, ready, start
            ));

            assertThat(ready.await(5, TimeUnit.SECONDS))
                    .as("both recoverers became ready")
                    .isTrue();
            start.countDown();
            firstRecovered = firstResult.get(10, TimeUnit.SECONDS);
            secondRecovered = secondResult.get(10, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("recovery executor terminated")
                    .isTrue();
        }

        assertThat(List.of(firstRecovered, secondRecovered))
                .containsExactlyInAnyOrder(0, 1);
        org.mockito.Mockito.verifyNoInteractions(backoff);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.FAILED.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(3L);
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
    }

    @Test
    void recoveredLeaseRejectsTheOldWorkersSuccessfulCompletion() throws Exception {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(319L, NOW.minusSeconds(1)))
        );
        BlockingEventTransport oldSender = new BlockingEventTransport();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> oldResult = null;

        try {
            oldResult = executor.submit(
                    () -> publishPending(worker(oldSender, WORKER_ID), CLOCK)
            );
            assertThat(oldSender.awaitSend(5, TimeUnit.SECONDS))
                    .as("old worker entered the sender")
                    .isTrue();
            expireLease(eventId, 1);

            assertThat(recovery(50, deterministicBackoff()).recoverExpiredLeases())
                    .isOne();
            Instant recoveredNextAttemptAt = nextAttemptAtOf(eventId);
            String recoveredError = lastErrorOf(eventId);

            oldSender.release();
            Future<Integer> completedOldResult = oldResult;
            assertThatThrownBy(() -> completedOldResult.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage(
                            "Event claim is no longer current: eventId="
                                    + eventId.value()
                                    + ", leaseOwner="
                                    + WORKER_ID
                    );

            assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
            assertThat(attemptCountOf(eventId)).isOne();
            assertThat(versionOf(eventId)).isEqualTo(2L);
            assertThat(nextAttemptAtOf(eventId)).isEqualTo(recoveredNextAttemptAt);
            assertThat(lastErrorOf(eventId)).isEqualTo(recoveredError);
            assertThat(publishedAtOf(eventId)).isNull();
        } finally {
            oldSender.release();
            if (oldResult != null && !oldResult.isDone()) {
                oldResult.cancel(true);
            }
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("old worker executor terminated")
                    .isTrue();
        }
    }

    @Test
    void newWorkerCompletesAfterRecoveryWithoutBeingOverwrittenByOldWorker()
            throws Exception {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(320L, NOW.minusSeconds(1)))
        );
        BlockingEventTransport oldSender = new BlockingEventTransport();
        BlockingEventTransport newSender = new BlockingEventTransport();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<Integer> oldResult = null;
        Future<Integer> newResult = null;

        try {
            oldResult = executor.submit(
                    () -> publishPending(worker(oldSender, WORKER_ID), CLOCK)
            );
            assertThat(oldSender.awaitSend(5, TimeUnit.SECONDS))
                    .as("old worker entered the sender")
                    .isTrue();
            expireLease(eventId, 1);
            assertThat(recovery(50, deterministicBackoff()).recoverExpiredLeases())
                    .isOne();

            Instant nextAttemptAt = nextAttemptAtOf(eventId);
            JdbcEventPublicationWorker newWorker = worker(
                    newSender,
                    Clock.fixed(nextAttemptAt, ZoneOffset.UTC),
                    deterministicBackoff(),
                    SECOND_WORKER_ID
            );
            Clock newWorkerClock = Clock.fixed(nextAttemptAt, ZoneOffset.UTC);
            newResult = executor.submit(() -> publishPending(newWorker, newWorkerClock, SECOND_WORKER_ID));
            assertThat(newSender.awaitSend(5, TimeUnit.SECONDS))
                    .as("new worker entered the sender")
                    .isTrue();

            Instant newLeaseUntil = leaseUntilOf(eventId);
            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
            assertThat(attemptCountOf(eventId)).isEqualTo(2);
            assertThat(versionOf(eventId)).isEqualTo(3L);
            assertThat(leaseOwnerOf(eventId)).isEqualTo(SECOND_WORKER_ID);

            oldSender.release();
            Future<Integer> completedOldResult = oldResult;
            assertThatThrownBy(() -> completedOldResult.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);

            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
            assertThat(attemptCountOf(eventId)).isEqualTo(2);
            assertThat(versionOf(eventId)).isEqualTo(3L);
            assertThat(leaseOwnerOf(eventId)).isEqualTo(SECOND_WORKER_ID);
            assertThat(leaseUntilOf(eventId)).isEqualTo(newLeaseUntil);

            newSender.release();
            assertThat(newResult.get(10, TimeUnit.SECONDS)).isOne();

            assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
            assertThat(attemptCountOf(eventId)).isEqualTo(2);
            assertThat(versionOf(eventId)).isEqualTo(4L);
            assertThat(leaseOwnerOf(eventId)).isNull();
            assertThat(leaseUntilOf(eventId)).isNull();
            assertThat(publishedAtOf(eventId)).isEqualTo(nextAttemptAt);
            assertThat(oldSender.sendCount()).isOne();
            assertThat(newSender.sendCount()).isOne();
        } finally {
            oldSender.release();
            newSender.release();
            if (oldResult != null && !oldResult.isDone()) {
                oldResult.cancel(true);
            }
            if (newResult != null && !newResult.isDone()) {
                newResult.cancel(true);
            }
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("takeover executor terminated")
                    .isTrue();
        }
    }

    @Test
    void nonRetryableFailureDoesNotStopLaterCandidatesInTheSameBatch() {
        EventId failedEvent = inTransaction(
                () -> publisher.publish(event(209L, NOW.minusSeconds(1)))
        );
        EventId publishedEvent = inTransaction(
                () -> publisher.publish(event(210L, NOW.minusSeconds(1)))
        );
        SelectiveNonRetryableEventTransport sender =
                new SelectiveNonRetryableEventTransport("209");

        int publishedCount = publishPending(worker(sender, CLOCK, deterministicBackoff()), CLOCK);

        assertThat(publishedCount).isOne();
        assertThat(sender.attemptedEventKeys()).containsExactly("209", "210");
        assertThat(statusOf(failedEvent)).isEqualTo(EventStatus.FAILED.code());
        assertThat(statusOf(publishedEvent)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(failedEvent)).isOne();
        assertThat(attemptCountOf(publishedEvent)).isOne();
        assertThat(versionOf(failedEvent)).isEqualTo(3L);
        assertThat(versionOf(publishedEvent)).isEqualTo(2L);
    }

    private JdbcEventPublicationWorker worker(EventTransport sender) {
        return worker(sender, WORKER_ID);
    }

    private JdbcPublicationStore publicationStore(Clock clock, String workerId, PublicationObserver observer) {
        return new JdbcPublicationStore(jdbcTemplate, transactionManager, clock, workerId,
                LEASE_DURATION, observer);
    }

    private int publishPending(JdbcEventPublicationWorker worker, Clock clock) {
        return publishPending(worker, clock, WORKER_ID);
    }

    private int publishPending(JdbcEventPublicationWorker worker, Clock clock, String workerId) {
        JdbcPublicationStore store = publicationStore(clock, workerId, PublicationObserver.NOOP);
        int published = 0;
        for (EventCandidate candidate : store.findDueEventCandidates(50)) {
            var claimed = store.claimCandidate(candidate);
            if (claimed.isPresent() && worker.publishClaimedEvent(claimed.get())) {
                published++;
            }
        }
        return published;
    }

    private boolean publishCandidate(JdbcEventPublicationWorker worker, EventCandidate candidate,
                                     Clock clock, String workerId) {
        return publicationStore(clock, workerId, PublicationObserver.NOOP).claimCandidate(candidate)
                .map(worker::publishClaimedEvent).orElse(false);
    }

    private JdbcEventPublicationWorker worker(EventTransport sender, PublicationObserver observer) {
        return new JdbcEventPublicationWorker(publicationStore(CLOCK, WORKER_ID, observer), sender, CLOCK,
                deterministicBackoff(), observer, PublicationTracer.NOOP);
    }

    private JdbcEventPublicationWorker worker(EventTransport sender, String workerId) {
        return new JdbcEventPublicationWorker(publicationStore(CLOCK, workerId, PublicationObserver.NOOP),
                sender, CLOCK, new ExponentialBackoff(java.time.Duration.ofSeconds(1), java.time.Duration.ofMinutes(5), 0.2), PublicationObserver.NOOP, PublicationTracer.NOOP);
    }

    private JdbcEventPublicationWorker worker(
            EventTransport sender,
            Clock clock,
            ExponentialBackoff backoff
    ) {
        return worker(sender, clock, backoff, WORKER_ID);
    }

    private JdbcEventPublicationWorker worker(
            EventTransport sender,
            Clock clock,
            ExponentialBackoff backoff,
            String workerId
    ) {
        return new JdbcEventPublicationWorker(publicationStore(clock, workerId, PublicationObserver.NOOP),
                sender, clock, backoff, PublicationObserver.NOOP, PublicationTracer.NOOP);
    }

    private JdbcExpiredLeaseRecovery recovery(
            int recoveryBatchSize,
            ExponentialBackoff backoff
    ) {
        return new JdbcExpiredLeaseRecovery(
                jdbcTemplate,
                transactionManager,
                recoveryBatchSize,
                backoff
        );
    }

    private Optional<ClaimedEvent> claim(
            EventCandidate candidate,
            String workerId
    ) {
        return repository.claim(candidate, NOW, workerId, LEASE_DURATION);
    }

    private ClaimedEvent claimedPublishedEvent(long taskId) {
        EventId eventId = inTransaction(() -> publisher.publish(event(taskId, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 100).stream()
                .filter(current -> current.id().equals(eventId)).findFirst().orElseThrow();
        return inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
    }

    private ExpiredLeaseCandidate expiredTerminalCandidate(long taskId) {
        EventId eventId = inTransaction(() -> publisher.publish(event(taskId, NOW)));
        jdbcTemplate.update("UPDATE reliable_event_outbox SET max_attempts=1 WHERE id=?", eventId.value());
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 100).stream()
                .filter(current -> current.id().equals(eventId)).findFirst().orElseThrow();
        inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        expireLease(eventId, 1);
        return repository.findExpiredLeaseCandidates(100).stream()
                .filter(current -> current.id().equals(eventId)).findFirst().orElseThrow();
    }

    private ExpiredLeaseCandidate expiredLeaseCandidate(long taskId) {
        EventId eventId = claimedEventWithExpiredLease(taskId, 1);
        return repository.findExpiredLeaseCandidates(100).stream()
                .filter(candidate -> candidate.id().equals(eventId))
                .findFirst()
                .orElseThrow();
    }

    private EventId claimedEventWithExpiredLease(long taskId, int secondsAgo) {
        EventId eventId = inTransaction(
                () -> publisher.publish(event(taskId, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 100).stream()
                .filter(current -> current.id().equals(eventId))
                .findFirst()
                .orElseThrow();
        inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        expireLease(eventId, secondsAgo);
        return eventId;
    }

    private void expireLease(EventId eventId, int secondsAgo) {
        jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET lease_until = TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(3))
                WHERE id = ?
                """,
                -secondsAgo,
                eventId.value()
        );
    }

    private ExponentialBackoff deterministicBackoff() {
        return new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                0.0);
    }

    private ReliableEventPublisher publisherWithMaxAttempts(int maxAttempts) {
        return new JdbcReliableEventPublisher(jdbcTemplate, objectMapper, CLOCK, maxAttempts);
    }

    private int publishAfterSignal(
            JdbcEventPublicationWorker worker,
            EventCandidate candidate,
            String workerId,
            CountDownLatch ready,
            CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for concurrent start signal");
        }
        return publishCandidate(worker, candidate, CLOCK, workerId) ? 1 : 0;
    }

    private int recoverAfterSignal(
            JdbcExpiredLeaseRecovery recovery,
            List<ExpiredLeaseCandidate> candidates,
            CountDownLatch ready,
            CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for concurrent recovery signal");
        }
        return recovery.recoverCandidates(candidates);
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

    private int attemptCountOf(EventId eventId) {
        Integer attemptCount = jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM reliable_event_outbox WHERE id = ?",
                Integer.class,
                eventId.value()
        );
        if (attemptCount == null) {
            throw new IllegalStateException("Event has no attempt count: " + eventId.value());
        }
        return attemptCount;
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

    private Instant publishedAtOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT published_at FROM reliable_event_outbox WHERE id = ?",
                (resultSet, rowNumber) -> {
                    var utc = resultSet.getObject("published_at", java.time.LocalDateTime.class);
                    return utc == null ? null : utc.toInstant(ZoneOffset.UTC);
                },
                eventId.value()
        );
    }

    private Instant nextAttemptAtOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT next_attempt_at FROM reliable_event_outbox WHERE id = ?",
                (resultSet, rowNumber) -> resultSet.getTimestamp("next_attempt_at").toInstant(),
                eventId.value()
        );
    }

    private String leaseOwnerOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT lease_owner FROM reliable_event_outbox WHERE id = ?",
                String.class,
                eventId.value()
        );
    }

    private Instant leaseUntilOf(EventId eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT lease_until FROM reliable_event_outbox WHERE id = ?",
                (resultSet, rowNumber) -> {
                    Timestamp timestamp = resultSet.getTimestamp("lease_until");
                    return timestamp == null ? null : timestamp.toInstant();
                },
                eventId.value()
        );
    }

    private Instant databaseNow() {
        Timestamp timestamp = jdbcTemplate.queryForObject(
                "SELECT UTC_TIMESTAMP(3)",
                Timestamp.class
        );
        if (timestamp == null) {
            throw new IllegalStateException("Database did not return its current time");
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

    private long requiredLong(String sql) {
        Long value = jdbcTemplate.queryForObject(sql, Long.class);
        if (value == null) {
            throw new IllegalStateException("Query did not return a count");
        }
        return value;
    }

    private record CouponTaskPayload(long taskId) {
    }

    private static final class FakeEventTransport implements EventTransport {

        private final List<OutboundEvent> sentEvents = new ArrayList<>();
        private boolean transactionActiveDuringSend;

        @Override
        public TransportReceipt send(OutboundEvent event) {
            transactionActiveDuringSend =
                    TransactionSynchronizationManager.isActualTransactionActive();
            sentEvents.add(event);
            return new TransportReceipt(Optional.of("fake-" + event.id().value()), Map.of());
        }
    }

    private static final class ConcurrentFakeEventTransport implements EventTransport {

        private final AtomicInteger sendCount = new AtomicInteger();
        private final ConcurrentLinkedQueue<OutboundEvent> sentEvents = new ConcurrentLinkedQueue<>();

        @Override
        public TransportReceipt send(OutboundEvent event) {
            sentEvents.add(event);
            sendCount.incrementAndGet();
            return new TransportReceipt(Optional.of("concurrent-fake-" + event.id().value()), Map.of());
        }

        int sendCount() {
            return sendCount.get();
        }

        List<OutboundEvent> sentEvents() {
            return List.copyOf(sentEvents);
        }
    }

    private static final class BlockingEventTransport implements EventTransport {

        private final CountDownLatch sendStarted = new CountDownLatch(1);
        private final CountDownLatch releaseSend = new CountDownLatch(1);
        private final AtomicInteger sendCount = new AtomicInteger();

        @Override
        public TransportReceipt send(OutboundEvent event) {
            sendCount.incrementAndGet();
            sendStarted.countDown();
            try {
                if (!releaseSend.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to release sender");
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting to release sender", exception);
            }
            return new TransportReceipt(Optional.of("blocking-fake-" + event.id().value()), Map.of());
        }

        boolean awaitSend(long timeout, TimeUnit unit) throws InterruptedException {
            return sendStarted.await(timeout, unit);
        }

        void release() {
            releaseSend.countDown();
        }

        int sendCount() {
            return sendCount.get();
        }
    }

    private static final class FailOnceEventTransport implements EventTransport {

        private int sendCount;

        @Override
        public TransportReceipt send(OutboundEvent event) {
            sendCount++;
            if (sendCount == 1) {
                throw new TransportException(TransportFailureType.RETRYABLE, "broker unavailable");
            }
            return new TransportReceipt(Optional.of("retry-fake-" + event.id().value()), Map.of());
        }

        int sendCount() {
            return sendCount;
        }
    }

    private static final class SelectiveFailureEventTransport implements EventTransport {

        private final String failingEventKey;
        private final List<String> attemptedEventKeys = new ArrayList<>();

        private SelectiveFailureEventTransport(String failingEventKey) {
            this.failingEventKey = failingEventKey;
        }

        @Override
        public TransportReceipt send(OutboundEvent event) {
            attemptedEventKeys.add(event.eventKey());
            if (event.eventKey().equals(failingEventKey)) {
                throw new TransportException(TransportFailureType.RETRYABLE, "broker unavailable for " + event.eventKey());
            }
            return new TransportReceipt(Optional.of("selective-fake-" + event.id().value()), Map.of());
        }

        List<String> attemptedEventKeys() {
            return List.copyOf(attemptedEventKeys);
        }
    }

    private static final class AlwaysFailingEventTransport implements EventTransport {

        private int sendCount;

        @Override
        public TransportReceipt send(OutboundEvent event) {
            sendCount++;
            throw new TransportException(TransportFailureType.RETRYABLE, "temporary failure " + sendCount);
        }

        int sendCount() {
            return sendCount;
        }
    }

    private static final class ResultUnknownEventTransport implements EventTransport {

        private int sendCount;

        @Override
        public TransportReceipt send(OutboundEvent event) {
            sendCount++;
            throw new TransportException(TransportFailureType.RESULT_UNKNOWN, "RocketMQ response lost");
        }

        int sendCount() {
            return sendCount;
        }
    }

    private static final class NonRetryableEventTransport implements EventTransport {

        private int sendCount;

        @Override
        public TransportReceipt send(OutboundEvent event) {
            sendCount++;
            throw new TransportException(TransportFailureType.NON_RETRYABLE, "missing destination");
        }

        int sendCount() {
            return sendCount;
        }
    }

    private static final class SelectiveNonRetryableEventTransport implements EventTransport {

        private final String failingEventKey;
        private final List<String> attemptedEventKeys = new ArrayList<>();

        private SelectiveNonRetryableEventTransport(String failingEventKey) {
            this.failingEventKey = failingEventKey;
        }

        @Override
        public TransportReceipt send(OutboundEvent event) {
            attemptedEventKeys.add(event.eventKey());
            if (event.eventKey().equals(failingEventKey)) {
                throw new TransportException(TransportFailureType.NON_RETRYABLE, "missing destination");
            }
            return new TransportReceipt(Optional.of("terminal-selective-fake-" + event.id().value()), Map.of());
        }

        List<String> attemptedEventKeys() {
            return List.copyOf(attemptedEventKeys);
        }
    }

    private static final class IntentionalRollbackException extends RuntimeException {
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
    }
}
