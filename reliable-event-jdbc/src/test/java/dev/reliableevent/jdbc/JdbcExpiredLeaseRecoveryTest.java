package dev.reliableevent.jdbc;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class JdbcExpiredLeaseRecoveryTest {

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final PlatformTransactionManager transactionManager =
            mock(PlatformTransactionManager.class);
    private final ExponentialBackoff backoff = new ExponentialBackoff(
            Duration.ofSeconds(1),
            Duration.ofMinutes(5),
            0.0,
            () -> 0.0
    );

    @Test
    void requiresAPositiveRecoveryBatchSize() {
        assertThatThrownBy(() -> new JdbcExpiredLeaseRecovery(
                jdbcTemplate,
                transactionManager,
                0,
                backoff
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryBatchSize");
    }

    @Test
    void requiresItsDependencies() {
        assertThatThrownBy(() -> new JdbcExpiredLeaseRecovery(
                null,
                transactionManager,
                10,
                backoff
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JdbcExpiredLeaseRecovery(
                jdbcTemplate,
                null,
                10,
                backoff
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JdbcExpiredLeaseRecovery(
                jdbcTemplate,
                transactionManager,
                10,
                null
        )).isInstanceOf(NullPointerException.class);
    }
}
