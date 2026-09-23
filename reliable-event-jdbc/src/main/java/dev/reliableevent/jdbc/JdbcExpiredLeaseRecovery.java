package dev.reliableevent.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

final class JdbcExpiredLeaseRecovery {

    static final String LEASE_EXPIRED_ERROR =
            "Publication lease expired before completion";

    private final JdbcOutboxRepository repository;
    private final TransactionTemplate transaction;
    private final ExponentialBackoff backoff;
    private final int recoveryBatchSize;

    JdbcExpiredLeaseRecovery(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            int recoveryBatchSize
    ) {
        this(
                jdbcTemplate,
                transactionManager,
                recoveryBatchSize,
                ExponentialBackoff.defaults()
        );
    }

    JdbcExpiredLeaseRecovery(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            int recoveryBatchSize,
            ExponentialBackoff backoff
    ) {
        if (recoveryBatchSize <= 0) {
            throw new IllegalArgumentException("recoveryBatchSize must be positive");
        }
        this.repository = new JdbcOutboxRepository(Objects.requireNonNull(jdbcTemplate));
        this.transaction = new TransactionTemplate(Objects.requireNonNull(transactionManager));
        this.recoveryBatchSize = recoveryBatchSize;
        this.backoff = Objects.requireNonNull(backoff);
    }

    int recoverExpiredLeases() {
        List<ExpiredLeaseCandidate> candidates =
                repository.findExpiredLeaseCandidates(recoveryBatchSize);
        int recoveredCount = 0;
        for (ExpiredLeaseCandidate candidate : candidates) {
            if (recover(candidate)) {
                recoveredCount++;
            }
        }
        return recoveredCount;
    }

    private boolean recover(ExpiredLeaseCandidate candidate) {
        if (!candidate.canRetry()) {
            return Boolean.TRUE.equals(transaction.execute(status ->
                    repository.recoverExpiredLeaseToDead(
                            candidate,
                            LEASE_EXPIRED_ERROR
                    )
            ));
        }

        Duration delay = backoff.nextDelay(candidate.attemptCount());
        return Boolean.TRUE.equals(transaction.execute(status ->
                repository.recoverExpiredLeaseToRetryWait(
                        candidate,
                        delay,
                        LEASE_EXPIRED_ERROR
                )
        ));
    }
}
