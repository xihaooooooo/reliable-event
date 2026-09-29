package dev.reliableevent.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class PublishedRetentionIntegrationTest {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("reliable_event_retention_test")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager manager;

    private JdbcReliableEventPublisher publisher;
    private JdbcPublishedEventRetention retention;

    @BeforeEach
    void setUp() {
        new ResourceDatabasePopulator(new ClassPathResource("schema/reliable-event-outbox.sql"))
                .execute(jdbc.getDataSource());
        jdbc.update("DELETE FROM reliable_event_outbox");
        jdbc.update("DELETE FROM reliable_event_identity");
        publisher = new JdbcReliableEventPublisher(jdbc, new ObjectMapper());
        retention = new JdbcPublishedEventRetention(jdbc, manager);
    }

    @Test
    void oldRowsAreBackfilledAndDuplicateRegistrationReturnsOriginalId() {
        EventId old = publish("old");
        EventId dead = publish("dead-old");
        jdbc.update("UPDATE reliable_event_outbox SET status = 4 WHERE id = ?", dead.value());
        jdbc.update("DELETE FROM reliable_event_identity WHERE id = ?", old.value());
        jdbc.update("DELETE FROM reliable_event_identity WHERE id = ?", dead.value());
        new ResourceDatabasePopulator(new ClassPathResource("schema/reliable-event-identity-m7-1.sql"))
                .execute(jdbc.getDataSource());

        assertThat(publish("old")).isEqualTo(old);
        EventId next = publish("new");
        assertThat(next.value()).isGreaterThan(dead.value());
        assertThat(count("reliable_event_outbox")).isEqualTo(3);
        assertThat(count("reliable_event_identity")).isEqualTo(3);
    }

    @Test
    void cleanupKeepsIdentityAndDuplicateCannotRequeueOrOverwritePayload() {
        EventId original = publish("stable");
        EventId repeated = new TransactionTemplate(manager).execute(status -> publisher.publish(
                new ReliableEvent<>("test-event", "stable", Map.of("value", "changed"),
                        Instant.now(), Map.of())));
        assertThat(repeated).isEqualTo(original);
        assertThat(jdbc.queryForObject("SELECT payload FROM reliable_event_outbox WHERE id = ?",
                String.class, original.value())).contains("stable").doesNotContain("changed");
        jdbc.update("UPDATE reliable_event_outbox SET status = 2, published_at = UTC_TIMESTAMP(3) - INTERVAL 2 DAY WHERE id = ?",
                original.value());

        PublishedRetentionResult result = retention.runOnce(Duration.ofDays(1), 10);
        assertThat(result.scanned()).isOne();
        assertThat(result.deleted()).isOne();
        assertThat(count("reliable_event_outbox")).isZero();
        assertThat(publish("stable")).isEqualTo(original);
        assertThat(count("reliable_event_outbox")).isZero();
        assertThat(count("reliable_event_identity")).isOne();
        assertThat(retention.runOnce(Duration.ofDays(1), 10).deleted()).isZero();
    }

    @Test
    void batchLimitAndFourOtherStatesProtectRows() {
        for (int index = 0; index < 3; index++) {
            EventId id = publish("published-" + index);
            markOld(id, 2);
        }
        for (int status : new int[] {0, 1, 3, 4}) {
            EventId id = publish("state-" + status);
            markOld(id, status);
        }
        assertThat(retention.runOnce(Duration.ofDays(1), 2).deleted()).isEqualTo(2);
        assertThat(retention.runOnce(Duration.ofDays(1), 2).deleted()).isOne();
        assertThat(retention.runOnce(Duration.ofDays(1), 2).deleted()).isZero();
        assertThat(count("reliable_event_outbox")).isEqualTo(4);
        assertThat(count("reliable_event_identity")).isEqualTo(7);
    }

    @Test
    void cutoffIsExclusiveAndMissingIdentityPreventsDeletion() {
        EventId event = publish("boundary");
        jdbc.update("UPDATE reliable_event_outbox SET status = 2, published_at = UTC_TIMESTAMP(3) WHERE id = ?",
                event.value());
        assertThat(retention.runOnce(Duration.ofDays(1), 10).deleted()).isZero();
        jdbc.update("UPDATE reliable_event_outbox SET published_at = UTC_TIMESTAMP(3) - INTERVAL 2 DAY WHERE id = ?",
                event.value());
        jdbc.update("DELETE FROM reliable_event_identity WHERE id = ?", event.value());
        assertThat(retention.runOnce(Duration.ofDays(1), 10).scanned()).isOne();
        assertThat(count("reliable_event_outbox")).isOne();
    }

    @Test
    void businessRollbackRemovesBothIdentityAndOutbox() {
        assertThatThrownBy(() -> new TransactionTemplate(manager).execute(status -> {
            publisher.publish(event("rollback"));
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(count("reliable_event_outbox")).isZero();
        assertThat(count("reliable_event_identity")).isZero();
    }

    @Test
    void concurrentRegistrationReturnsOneIdentity() throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> competingPublish(ready, start));
            var second = executor.submit(() -> competingPublish(ready, start));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(second.get(10, TimeUnit.SECONDS));
            assertThat(count("reliable_event_identity")).isOne();
            assertThat(count("reliable_event_outbox")).isOne();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentCleanupDeletesAtMostOneCopyOfEachRow() throws Exception {
        for (int index = 0; index < 10; index++) {
            markOld(publish("concurrent-" + index), 2);
        }
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> competingCleanup(ready, start));
            var second = executor.submit(() -> competingCleanup(ready, start));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            int deleted = first.get(10, TimeUnit.SECONDS).deleted()
                    + second.get(10, TimeUnit.SECONDS).deleted();
            assertThat(deleted).isEqualTo(10);
            assertThat(count("reliable_event_outbox")).isZero();
            assertThat(count("reliable_event_identity")).isEqualTo(10);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void failedCleanupRollsBackTheBatch() {
        markOld(publish("failure"), 2);
        JdbcTemplate faulty = new JdbcTemplate(jdbc.getDataSource()) {
            @Override
            public int update(String sql, Object... args) {
                int deleted = super.update(sql, args);
                if (sql.startsWith("DELETE o FROM reliable_event_outbox")) {
                    throw new DataIntegrityViolationException("Injected failure after " + deleted + " deletes");
                }
                return deleted;
            }
        };
        assertThatThrownBy(() -> new JdbcPublishedEventRetention(faulty, manager)
                .runOnce(Duration.ofDays(1), 10)).isInstanceOf(DataAccessException.class);
        assertThat(count("reliable_event_outbox")).isOne();
        assertThat(count("reliable_event_identity")).isOne();
    }

    @Test
    void absentIdentityTableFailsClosed() {
        jdbc.execute("DROP TABLE reliable_event_identity");
        try {
            assertThatThrownBy(() -> publish("no-table"))
                    .isInstanceOf(DataAccessException.class);
            assertThat(count("reliable_event_outbox")).isZero();
        } finally {
            new ResourceDatabasePopulator(new ClassPathResource("schema/reliable-event-outbox.sql"))
                    .execute(jdbc.getDataSource());
        }
    }

    private PublishedRetentionResult competingCleanup(CountDownLatch ready, CountDownLatch start)
            throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("start timeout");
        }
        return new JdbcPublishedEventRetention(jdbc, manager).runOnce(Duration.ofDays(1), 10);
    }

    private EventId competingPublish(CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("start timeout");
        }
        return new TransactionTemplate(manager).execute(status -> publisher.publish(event("same")));
    }

    private EventId publish(String key) {
        return new TransactionTemplate(manager).execute(status -> publisher.publish(event(key)));
    }

    private ReliableEvent<Map<String, String>> event(String key) {
        return new ReliableEvent<>("test-event", key, Map.of("value", key),
                Instant.now(), Map.of());
    }

    private void markOld(EventId id, int status) {
        jdbc.update("UPDATE reliable_event_outbox SET status = ?, published_at = UTC_TIMESTAMP(3) - INTERVAL 2 DAY WHERE id = ?",
                status, id.value());
    }

    private long count(String table) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
    }
}
