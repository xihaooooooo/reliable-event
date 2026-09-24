package dev.reliableevent.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.MissingActiveTransactionException;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.jdbc.internal.cycle.JdbcEventPublicationCycle;
import dev.reliableevent.jdbc.internal.cycle.PublicationCycleResult;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.internal.publication.SendReceipt;
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

    private static final Instant NOW = Instant.parse("2026-09-21T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final String WORKER_ID = "integration-worker-1";
    private static final String SECOND_WORKER_ID = "integration-worker-2";
    private static final Duration LEASE_DURATION = Duration.ofSeconds(30);

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
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
        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markDead(incorrectOwnerClaim, NOW, "permanent failure");
            return null;
        })).isInstanceOf(IllegalStateException.class);

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
        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markDead(claimedEvent, NOW, "permanent failure");
            return null;
        })).isInstanceOf(IllegalStateException.class);

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
                () -> claim(candidate, WORKER_ID).orElseThrow()
        );
        jdbcTemplate.update(
                "UPDATE reliable_event_outbox SET version = version + 1 WHERE id = ?",
                eventId.value()
        );

        assertThatThrownBy(() -> inTransaction(() -> {
            repository.markDead(
                    claimedEvent,
                    NOW,
                    EventSendException.class.getName() + ": missing destination"
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
        assertThat(inTransaction(() -> claim(exhaustedCandidate, WORKER_ID))).isEmpty();
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
                50,
                WORKER_ID,
                LEASE_DURATION
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
        ConcurrentFakeEventSender sender = new ConcurrentFakeEventSender();
        JdbcEventPublicationWorker firstWorker = worker(sender, WORKER_ID);
        JdbcEventPublicationWorker secondWorker = worker(sender, SECOND_WORKER_ID);
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
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
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
    void unknownSendResultRetriesAndBecomesDeadAfterLastAllowedAttempt() {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(218L, NOW.minusSeconds(1)))
        );
        ResultUnknownEventSender sender = new ResultUnknownEventSender();
        ExponentialBackoff backoff = deterministicBackoff();

        assertThat(worker(sender, CLOCK, backoff).publishDueEvents()).isZero();
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(nextAttemptAtOf(eventId)).isEqualTo(NOW.plusSeconds(1));

        Clock atRetry = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);
        assertThat(worker(sender, atRetry, backoff).publishDueEvents()).isZero();

        assertThat(sender.sendCount()).isEqualTo(2);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isEqualTo(2);
        assertThat(versionOf(eventId)).isEqualTo(4L);
        assertThat(lastErrorOf(eventId))
                .isEqualTo(EventSendException.class.getName() + ": RocketMQ response lost");
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
                .isEqualTo(EventSendException.class.getName() + ": missing destination");
        assertThat(publishedAtOf(eventId)).isNull();
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
    }

    @Test
    void workerRejectsInvalidLeaseConfiguration() {
        FakeEventSender sender = new FakeEventSender();

        assertThatThrownBy(() -> new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                CLOCK,
                50,
                " ",
                LEASE_DURATION
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workerId");
        assertThatThrownBy(() -> new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                CLOCK,
                50,
                "x".repeat(129),
                LEASE_DURATION
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("128");
        assertThatThrownBy(() -> new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                CLOCK,
                50,
                WORKER_ID,
                Duration.ZERO
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseDuration");
        assertThatThrownBy(() -> new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                CLOCK,
                50,
                WORKER_ID,
                Duration.ofMillis(-1)
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseDuration");
        assertThatThrownBy(() -> new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                CLOCK,
                50,
                WORKER_ID,
                null
        )).isInstanceOf(NullPointerException.class)
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
        assertThat(lastErrorOf(eventId))
                .isEqualTo(JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR);
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
    void expiredLeaseBecomesDeadWhenAttemptsAreExhausted() {
        ReliableEventPublisher oneAttemptPublisher = publisherWithMaxAttempts(1);
        EventId eventId = inTransaction(
                () -> oneAttemptPublisher.publish(event(303L, NOW.minusSeconds(1)))
        );
        EventCandidate candidate = repository.findDueEventCandidates(NOW, 50).get(0);
        inTransaction(() -> claim(candidate, WORKER_ID).orElseThrow());
        expireLease(eventId, 1);
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

        int recoveredCount = recovery(50, backoff).recoverExpiredLeases();

        assertThat(recoveredCount).isOne();
        assertThat(randomCalls).hasValue(0);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
        assertThat(lastErrorOf(eventId))
                .isEqualTo(JdbcExpiredLeaseRecovery.LEASE_EXPIRED_ERROR);
        assertThat(publishedAtOf(eventId)).isNull();
        assertThat(repository.findDueEventCandidates(
                databaseNow().plusSeconds(3_600),
                50
        )).isEmpty();
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
    void publicationCycleRecoversExpiredLeasesBeforePublishingDueEvents() {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId expiredEvent = inTransaction(
                () -> twoAttemptPublisher.publish(event(315L, NOW.minusSeconds(1)))
        );
        EventCandidate expiredCandidate = repository.findDueEventCandidates(NOW, 50).get(0);
        inTransaction(() -> claim(expiredCandidate, WORKER_ID).orElseThrow());
        expireLease(expiredEvent, 1);
        EventId dueEvent = inTransaction(
                () -> publisher.publish(event(316L, NOW.minusSeconds(1)))
        );
        FakeEventSender sender = new FakeEventSender();
        JdbcEventPublicationCycle cycle = new JdbcEventPublicationCycle(
                recovery(50, deterministicBackoff()),
                worker(sender, SECOND_WORKER_ID)
        );

        Instant databaseTimeBeforeCycle = databaseNow();
        PublicationCycleResult result = cycle.runOnce();
        Instant databaseTimeAfterCycle = databaseNow();

        assertThat(result).isEqualTo(new PublicationCycleResult(1, 1));
        assertThat(statusOf(expiredEvent)).isEqualTo(EventStatus.RETRY_WAIT.code());
        assertThat(attemptCountOf(expiredEvent)).isOne();
        assertThat(versionOf(expiredEvent)).isEqualTo(2L);
        assertThat(nextAttemptAtOf(expiredEvent)).isBetween(
                databaseTimeBeforeCycle.plusSeconds(1),
                databaseTimeAfterCycle.plusSeconds(1)
        );
        assertThat(statusOf(dueEvent)).isEqualTo(EventStatus.PUBLISHED.code());
        assertThat(sender.sentEvents)
                .extracting(StoredEvent::id)
                .containsExactly(dueEvent);
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
                backoffWaitingAt(transactionStart)
        );
        JdbcExpiredLeaseRecovery secondRecovery = recovery(
                50,
                backoffWaitingAt(transactionStart)
        );
        ExecutorService executor = Executors.newFixedThreadPool(2);

        int firstRecovered;
        int secondRecovered;
        try {
            Future<Integer> firstResult = executor.submit(
                    () -> firstRecovery.recoverCandidates(snapshot)
            );
            Future<Integer> secondResult = executor.submit(
                    () -> secondRecovery.recoverCandidates(snapshot)
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
    void twoRecoverersCompetingForTheSameExhaustedSnapshotDeadLetterOnlyOnce()
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
        assertThat(randomCalls).hasValue(0);
        assertThat(statusOf(eventId)).isEqualTo(EventStatus.DEAD.code());
        assertThat(attemptCountOf(eventId)).isOne();
        assertThat(versionOf(eventId)).isEqualTo(2L);
        assertThat(leaseOwnerOf(eventId)).isNull();
        assertThat(leaseUntilOf(eventId)).isNull();
    }

    @Test
    void recoveredLeaseRejectsTheOldWorkersSuccessfulCompletion() throws Exception {
        ReliableEventPublisher twoAttemptPublisher = publisherWithMaxAttempts(2);
        EventId eventId = inTransaction(
                () -> twoAttemptPublisher.publish(event(319L, NOW.minusSeconds(1)))
        );
        BlockingEventSender oldSender = new BlockingEventSender();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> oldResult = null;

        try {
            oldResult = executor.submit(
                    () -> worker(oldSender, WORKER_ID).publishDueEvents()
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
        BlockingEventSender oldSender = new BlockingEventSender();
        BlockingEventSender newSender = new BlockingEventSender();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<Integer> oldResult = null;
        Future<Integer> newResult = null;

        try {
            oldResult = executor.submit(
                    () -> worker(oldSender, WORKER_ID).publishDueEvents()
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
            newResult = executor.submit(newWorker::publishDueEvents);
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
        return worker(sender, WORKER_ID);
    }

    private JdbcEventPublicationWorker worker(EventSender sender, String workerId) {
        return new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                CLOCK,
                50,
                workerId,
                LEASE_DURATION
        );
    }

    private JdbcEventPublicationWorker worker(
            EventSender sender,
            Clock clock,
            ExponentialBackoff backoff
    ) {
        return worker(sender, clock, backoff, WORKER_ID);
    }

    private JdbcEventPublicationWorker worker(
            EventSender sender,
            Clock clock,
            ExponentialBackoff backoff,
            String workerId
    ) {
        return new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                clock,
                50,
                workerId,
                LEASE_DURATION,
                backoff
        );
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

    private ExponentialBackoff backoffWaitingAt(CyclicBarrier barrier) {
        return new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                0.2,
                () -> {
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                        return 0.0;
                    } catch (Exception exception) {
                        throw new IllegalStateException(
                                "Timed out waiting for concurrent recovery transaction",
                                exception
                        );
                    }
                }
        );
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

    private static final class BlockingEventSender implements EventSender {

        private final CountDownLatch sendStarted = new CountDownLatch(1);
        private final CountDownLatch releaseSend = new CountDownLatch(1);
        private final AtomicInteger sendCount = new AtomicInteger();

        @Override
        public SendReceipt send(StoredEvent event) {
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
            return new SendReceipt("blocking-fake-" + event.id().value());
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

    private static final class ResultUnknownEventSender implements EventSender {

        private int sendCount;

        @Override
        public SendReceipt send(StoredEvent event) {
            sendCount++;
            throw EventSendException.resultUnknown("RocketMQ response lost");
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
