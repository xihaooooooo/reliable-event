package dev.reliableevent.jdbc.internal.recovery;

import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class JdbcExpiredLeaseRecovery {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcExpiredLeaseRecovery.class);
    public static final String LEASE_EXPIRED_ERROR =
            "Publication lease expired before completion";

    private final JdbcOutboxRepository repository;
    private final TransactionTemplate transaction;
    private final ExponentialBackoff backoff;
    private final int recoveryBatchSize;
    private final PublicationObserver observer;

    public JdbcExpiredLeaseRecovery(
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

    public JdbcExpiredLeaseRecovery(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            int recoveryBatchSize,
            ExponentialBackoff backoff
    ) {
        this(jdbcTemplate, transactionManager, recoveryBatchSize, backoff,
                PublicationObserver.NOOP);
    }

    public JdbcExpiredLeaseRecovery(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            int recoveryBatchSize,
            ExponentialBackoff backoff,
            PublicationObserver observer
    ) {
        if (recoveryBatchSize <= 0) {
            throw new IllegalArgumentException("recoveryBatchSize must be positive");
        }
        this.repository = new JdbcOutboxRepository(Objects.requireNonNull(jdbcTemplate));
        this.transaction = new TransactionTemplate(Objects.requireNonNull(transactionManager));
        this.recoveryBatchSize = recoveryBatchSize;
        this.backoff = Objects.requireNonNull(backoff);
        this.observer = Objects.requireNonNull(observer);
    }

    public int recoverExpiredLeases() {
        List<ExpiredLeaseCandidate> candidates =
                repository.findExpiredLeaseCandidates(recoveryBatchSize);
        return recoverCandidates(candidates);
    }

    public int recoverCandidates(List<ExpiredLeaseCandidate> candidates) {
        List<ExpiredLeaseCandidate> snapshot = List.copyOf(
                Objects.requireNonNull(candidates, "candidates must not be null")
        );
        int recoveredCount = 0;
        for (ExpiredLeaseCandidate candidate : snapshot) {
            if (recover(candidate)) {
                recoveredCount++;
            }
        }
        return recoveredCount;
    }

    private boolean recover(ExpiredLeaseCandidate candidate) {
        boolean dead = !candidate.canRetry();
        boolean recovered;
        if (!candidate.canRetry()) {
            recovered = Boolean.TRUE.equals(transaction.execute(status ->
                    repository.recoverExpiredLeaseToDead(
                            candidate,
                            LEASE_EXPIRED_ERROR
                    )
            ));
        } else {
            Duration delay = backoff.nextDelay(candidate.attemptCount());
            recovered = Boolean.TRUE.equals(transaction.execute(status ->
                    repository.recoverExpiredLeaseToRetryWait(
                            candidate, delay, LEASE_EXPIRED_ERROR
                    )
            ));
        }
        if (recovered) {
            LOG.atInfo().addKeyValue("event", "reliable_event.lease.recovered")
                    .addKeyValue("eventId", candidate.id().value())
                    .addKeyValue("eventType", candidate.eventType())
                    .addKeyValue("eventKey", candidate.eventKey())
                    .addKeyValue("attemptCount", candidate.attemptCount())
                    .addKeyValue("leaseOwner", candidate.leaseOwner())
                    .addKeyValue("targetStatus", dead ? "DEAD" : "RETRY_WAIT")
                    .log("event=reliable_event.lease.recovered eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} targetStatus={}",
                    candidate.id().value(), candidate.eventType(), candidate.eventKey(),
                    candidate.attemptCount(), candidate.leaseOwner(), dead ? "DEAD" : "RETRY_WAIT");
            try {
                observer.leaseRecovered(candidate, dead);
            } catch (RuntimeException failure) {
                LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                        failure.getClass().getName());
            }
        }
        return recovered;
    }

    public void refreshSnapshot() {
        try {
            observer.refreshSnapshot();
        } catch (RuntimeException failure) {
            LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                    failure.getClass().getName());
        }
    }
}
