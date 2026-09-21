package dev.reliableevent.jdbc;

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

import java.sql.Timestamp;
import java.time.Instant;

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
class OutboxTransactionIntegrationTest {

    private static final String EVENT_TYPE = "coupon-task-execute";
    private static final int PENDING_STATUS = 0;
    private static final int DEFAULT_MAX_ATTEMPTS = 8;

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

    @Test
    void businessAndOutboxRowsRollbackTogether() {
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
            insertBusinessRecord(101L);
            insertOutboxEvent(101L);
            throw new IntentionalRollbackException();
        })).isInstanceOf(IntentionalRollbackException.class);

        assertThat(rowCount("test_business_record")).isZero();
        assertThat(rowCount("reliable_event_outbox")).isZero();
    }

    @Test
    void businessAndOutboxRowsCommitTogether() {
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        transaction.executeWithoutResult(status -> {
            insertBusinessRecord(102L);
            insertOutboxEvent(102L);
        });

        assertThat(rowCount("test_business_record")).isOne();
        assertThat(rowCount("reliable_event_outbox")).isOne();
    }

    private void insertBusinessRecord(long id) {
        jdbcTemplate.update(
                "INSERT INTO test_business_record (id, name) VALUES (?, ?)",
                id,
                "task-" + id
        );
    }

    private void insertOutboxEvent(long taskId) {
        Instant now = Instant.now();
        jdbcTemplate.update("""
                INSERT INTO reliable_event_outbox (
                    event_type,
                    event_key,
                    payload,
                    status,
                    next_attempt_at,
                    max_attempts,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                EVENT_TYPE,
                Long.toString(taskId),
                "{\"taskId\":" + taskId + "}",
                PENDING_STATUS,
                Timestamp.from(now),
                DEFAULT_MAX_ATTEMPTS,
                Timestamp.from(now),
                Timestamp.from(now)
        );
    }

    private long rowCount(String tableName) {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, Long.class);
        return count == null ? 0 : count;
    }

    private static final class IntentionalRollbackException extends RuntimeException {
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
    }
}
