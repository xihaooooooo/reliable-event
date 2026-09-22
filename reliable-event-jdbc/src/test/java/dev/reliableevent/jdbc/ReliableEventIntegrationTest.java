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
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
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
    void candidateCanBeClaimedOnlyOnceWithItsOriginalVersion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(105L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);

        assertThat(candidate.id()).isEqualTo(eventId);
        assertThat(candidate.version()).isZero();

        ClaimedEvent claimedEvent = inTransaction(
                () -> repository.claim(candidate, NOW).orElseThrow()
        );

        assertThat(claimedEvent.event().id()).isEqualTo(eventId);
        assertThat(claimedEvent.claimVersion()).isOne();
        assertThat(claimedEvent.attemptCount()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHING.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isOne();

        assertThat(inTransaction(() -> repository.claim(candidate, NOW))).isEmpty();
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

        assertThat(inTransaction(() -> repository.claim(candidate, NOW))).isEmpty();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PENDING.code());
        assertThat(attemptCountOf(eventId)).isZero();
        assertThat(versionOf(eventId)).isZero();
    }

    @Test
    void publishedUpdateRejectsAStaleClaimVersion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(107L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> repository.claim(candidate, NOW).orElseThrow()
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
    void retryUpdateRejectsAStaleClaimVersion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(108L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> repository.claim(candidate, NOW).orElseThrow()
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
                    "java.lang.IllegalStateException: broker unavailable"
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
    void deadUpdateRejectsAStaleClaimVersion() {
        EventId eventId = inTransaction(() -> publisher.publish(event(109L, NOW)));
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ClaimedEvent claimedEvent = inTransaction(
                () -> repository.claim(candidate, NOW).orElseThrow()
        );
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET version = version + 1 WHERE id = ?",
                eventId.value()
        );

        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markDead(
                    claimedEvent,
                    NOW,
                    "dev.reliableevent.jdbc.EventSendException: missing destination"
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
        assertThat(inTransaction(() -> repository.claim(exhaustedCandidate, NOW))).isEmpty();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isOne();
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
        assertThat(attemptCountOf(dueEvent)).isOne();
        assertThat(attemptCountOf(futureEvent)).isZero();
        assertThat(versionOf(dueEvent)).isEqualTo(2L);
        assertThat(versionOf(futureEvent)).isZero();
        assertThat(publishedAtOf(dueEvent)).isNotNull();
        assertThat(publishedAtOf(futureEvent)).isNull();
        assertThat(sender.transactionActiveDuringSend).isFalse();
    }

    @Test
    void twoWorkersCompetingForTheSameCandidatePublishOnlyOnce() throws Exception {
        EventId eventId = inTransaction(
                () -> publisher.publish(event(203L, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        ConcurrentFakeEventSender sender = new ConcurrentFakeEventSender();
        JdbcEventPublicationWorker firstWorker = worker(sender);
        JdbcEventPublicationWorker secondWorker = worker(sender);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        int firstPublished;
        int secondPublished;
        try {
            Future<Integer> firstResult = executor.submit(
                    () -> publishAfterSignal(firstWorker, candidate, ready, start)
            );
            Future<Integer> secondResult = executor.submit(
                    () -> publishAfterSignal(secondWorker, candidate, ready, start)
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
                .extracting(StoredEvent::id)
                .containsExactly(eventId);
        assertThat(sender.sentEvents())
                .extracting(StoredEvent::eventKey)
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
        FailOnceEventSender sender = new FailOnceEventSender();
        ExponentialBackoff backoff = deterministicBackoff();

        int firstPublished = worker(sender, CLOCK, backoff).publishDueEvents();

        assertThat(firstPublished).isZero();
        assertThat(sender.sendCount()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(nextAttemptAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));
        assertThat(lastErrorOf(eventId))
                .isEqualTo("java.lang.IllegalStateException: broker unavailable");
        assertThat(publishedAtOf(eventId)).isNull();

        Clock beforeRetry = Clock.fixed(NOW.plusMillis(999), ZoneOffset.UTC);
        int earlyPublished = worker(sender, beforeRetry, backoff).publishDueEvents();

        assertThat(earlyPublished).isZero();
        assertThat(sender.sendCount()).isOne();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);

        Clock atRetry = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);
        int retryPublished = worker(sender, atRetry, backoff).publishDueEvents();

        assertThat(retryPublished).isOne();
        assertThat(sender.sendCount()).isEqualTo(2);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(4L);
        assertThat(publishedAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));
        assertThat(lastErrorOf(eventId))
                .isEqualTo("java.lang.IllegalStateException: broker unavailable");
    }

    @Test
    void failedEventDoesNotStopLaterCandidatesInTheSameBatch() {
        EventId failedEvent = inTransaction(
                () -> publisher.publish(event(205L, NOW.minusSeconds(1)))
        );
        EventId publishedEvent = inTransaction(
                () -> publisher.publish(event(206L, NOW.minusSeconds(1)))
        );
        SelectiveFailureEventSender sender = new SelectiveFailureEventSender("205");

        int publishedCount = worker(sender, CLOCK, deterministicBackoff()).publishDueEvents();

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
    void retryableFailureBecomesDeadAfterLastAllowedAttempt() {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(207L, NOW.minusSeconds(1)))
        );
        AlwaysFailingEventSender sender = new AlwaysFailingEventSender();
        ExponentialBackoff backoff = deterministicBackoff();

        assertThat(worker(sender, CLOCK, backoff).publishDueEvents()).isZero();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(nextAttemptAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));

        Clock atRetry = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);
        assertThat(worker(sender, atRetry, backoff).publishDueEvents()).isZero();

        assertThat(sender.sendCount()).isEqualTo(2);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(4L);
        assertThat(nextAttemptAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));
        assertThat(lastErrorOf(eventId))
                .isEqualTo("java.lang.IllegalStateException: temporary failure 2");
        assertThat(publishedAtOf(eventId)).isNull();

        Clock longAfterRetry = Clock.fixed(NOW.plusSeconds(3_600), ZoneOffset.UTC);
        assertThat(worker(sender, longAfterRetry, backoff).publishDueEvents()).isZero();
        assertThat(sender.sendCount()).isEqualTo(2);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(4L);
    }

    @Test
    void nonRetryableFailureBecomesDeadWithoutComputingBackoff() {
        EventId eventId = inTransaction(
                () -> publisher.publish(event(208L, NOW.minusSeconds(1)))
        );
        NonRetryableEventSender sender = new NonRetryableEventSender();
        AtomicInteger randomCalls = new AtomicInteger();
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                0.2,
                () -> {
                    randomCalls.incrementAndGet();
                    return 0.0;
                }
        );

        assertThat(worker(sender, CLOCK, backoff).publishDueEvents()).isZero();

        assertThat(sender.sendCount()).isOne();
        assertThat(randomCalls).hasValue(0);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(lastErrorOf(eventId))
                .isEqualTo("dev.reliableevent.jdbc.EventSendException: missing destination");
        assertThat(publishedAtOf(eventId)).isNull();
    }

    @Test
    void deadEventDoesNotStopLaterCandidatesInTheSameBatch() {
        EventId deadEvent = inTransaction(
                () -> publisher.publish(event(209L, NOW.minusSeconds(1)))
        );
        EventId publishedEvent = inTransaction(
                () -> publisher.publish(event(210L, NOW.minusSeconds(1)))
        );
        SelectiveNonRetryableEventSender sender =
                new SelectiveNonRetryableEventSender("209");

        int publishedCount = worker(sender, CLOCK, deterministicBackoff()).publishDueEvents();

        assertThat(publishedCount).isOne();
        assertThat(sender.attemptedEventKeys()).containsExactly("209", "210");
        assertThat(statusOf(deadEvent)).isEqualTo(EventStatus.DEAD.code());
        assertThat(statusOf(publishedEvent)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(attemptCountOf(deadEvent)).isOne();
        assertThat(attemptCountOf(publishedEvent)).isOne();
        assertThat(versionOf(deadEvent)).isEqualTo(2L);
        assertThat(versionOf(publishedEvent)).isEqualTo(2L);
    }

    private JdbcEventPublicationWorker worker(EventSender sender) {
        return new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                CLOCK,
                50
        );
    }

    private JdbcEventPublicationWorker worker(
            EventSender sender,
            Clock clock,
            ExponentialBackoff backoff
    ) {
        return new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                clock,
                50,
                backoff
        );
    }

    private ExponentialBackoff deterministicBackoff() {
        return new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                0.2,
                () -> 0.0
        );
    }

    private ReliableEventPublisher publisherWithMaxAttempts(int maxAttempts) {
        return new JdbcReliableEventPublisher(jdbcTemplate, objectMapper, CLOCK, maxAttempts);
    }

    private int publishAfterSignal(
            JdbcEventPublicationWorker worker,
            EventCandidate candidate,
            CountDownLatch ready,
            CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for concurrent start signal");
        }
        return worker.publishCandidates(List.of(candidate));
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
                    var timestamp = resultSet.getTimestamp("published_at");
                    return timestamp == null ? null : timestamp.toInstant();
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

    private static final class FakeEventSender implements EventSender {

        private final List<StoredEvent> sentEvents = new ArrayList<>();
        private boolean transactionActiveDuringSend;

        @Override
        public SendReceipt send(StoredEvent event) {
            transactionActiveDuringSend =
                    TransactionSynchronizationManager.isActualTransactionActive();
            sentEvents.add(event);
            return new SendReceipt("fake-" + event.id().value());
        }
    }

    private static final class ConcurrentFakeEventSender implements EventSender {

        private final AtomicInteger sendCount = new AtomicInteger();
        private final ConcurrentLinkedQueue<StoredEvent> sentEvents = new ConcurrentLinkedQueue<>();

        @Override
        public SendReceipt send(StoredEvent event) {
            sentEvents.add(event);
            sendCount.incrementAndGet();
            return new SendReceipt("concurrent-fake-" + event.id().value());
        }

        int sendCount() {
            return sendCount.get();
        }

        List<StoredEvent> sentEvents() {
            return List.copyOf(sentEvents);
        }
    }

    private static final class FailOnceEventSender implements EventSender {

        private int sendCount;

        @Override
        public SendReceipt send(StoredEvent event) {
            sendCount++;
            if (sendCount == 1) {
                throw new IllegalStateException("broker unavailable");
            }
            return new SendReceipt("retry-fake-" + event.id().value());
        }

        int sendCount() {
            return sendCount;
        }
    }

    private static final class SelectiveFailureEventSender implements EventSender {

        private final String failingEventKey;
        private final List<String> attemptedEventKeys = new ArrayList<>();

        private SelectiveFailureEventSender(String failingEventKey) {
            this.failingEventKey = failingEventKey;
        }

        @Override
        public SendReceipt send(StoredEvent event) {
            attemptedEventKeys.add(event.eventKey());
            if (event.eventKey().equals(failingEventKey)) {
                throw new IllegalStateException("broker unavailable for " + event.eventKey());
            }
            return new SendReceipt("selective-fake-" + event.id().value());
        }

        List<String> attemptedEventKeys() {
            return List.copyOf(attemptedEventKeys);
        }
    }

    private static final class AlwaysFailingEventSender implements EventSender {

        private int sendCount;

        @Override
        public SendReceipt send(StoredEvent event) {
            sendCount++;
            throw new IllegalStateException("temporary failure " + sendCount);
        }

        int sendCount() {
            return sendCount;
        }
    }

    private static final class NonRetryableEventSender implements EventSender {

        private int sendCount;

        @Override
        public SendReceipt send(StoredEvent event) {
            sendCount++;
            throw EventSendException.nonRetryable("missing destination");
        }

        int sendCount() {
            return sendCount;
        }
    }

    private static final class SelectiveNonRetryableEventSender implements EventSender {

        private final String failingEventKey;
        private final List<String> attemptedEventKeys = new ArrayList<>();

        private SelectiveNonRetryableEventSender(String failingEventKey) {
            this.failingEventKey = failingEventKey;
        }

        @Override
        public SendReceipt send(StoredEvent event) {
            attemptedEventKeys.add(event.eventKey());
            if (event.eventKey().equals(failingEventKey)) {
                throw EventSendException.nonRetryable("missing destination");
            }
            return new SendReceipt("terminal-selective-fake-" + event.id().value());
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
