package dev.reliableevent.jdbc.internal.recovery;

import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.EventFallbackContext;
import dev.reliableevent.EventFallbackHandler;
import dev.reliableevent.FailedEvent;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.internal.model.StoredEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronization;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.Optional;

public final class JdbcExpiredLeaseRecovery {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcExpiredLeaseRecovery.class);
    public static final String LEASE_EXPIRED_ERROR =
            "Publication lease expired before completion";

    private final JdbcOutboxRepository repository;
    private final TransactionTemplate transaction;
    private final ExponentialBackoff backoff;
    private final int recoveryBatchSize;
    private final PublicationObserver observer;
    private final Duration leaseDuration;
    private final EventFallbackHandler fallbackHandler;

    public JdbcExpiredLeaseRecovery(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            int recoveryBatchSize,
            ExponentialBackoff backoff
    ) {
        this(jdbcTemplate, transactionManager, recoveryBatchSize, backoff,
                PublicationObserver.NOOP, Duration.ofSeconds(30), null);
    }

    public JdbcExpiredLeaseRecovery(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            int recoveryBatchSize,
            ExponentialBackoff backoff,
            PublicationObserver observer
    ) {
        this(jdbcTemplate, transactionManager, recoveryBatchSize, backoff, observer,
                Duration.ofSeconds(30), null);
    }

    public JdbcExpiredLeaseRecovery(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager,
                                    int recoveryBatchSize, ExponentialBackoff backoff,
                                    PublicationObserver observer, Duration leaseDuration,
                                    EventFallbackHandler fallbackHandler) {
        if (recoveryBatchSize <= 0) {
            throw new IllegalArgumentException("recoveryBatchSize must be positive");
        }
        this.repository = new JdbcOutboxRepository(Objects.requireNonNull(jdbcTemplate));
        this.transaction = new TransactionTemplate(Objects.requireNonNull(transactionManager));
        this.recoveryBatchSize = recoveryBatchSize;
        this.backoff = Objects.requireNonNull(backoff);
        this.observer = Objects.requireNonNull(observer);
        this.leaseDuration = Objects.requireNonNull(leaseDuration);
        this.fallbackHandler = fallbackHandler;
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
        if (candidate.status() == dev.reliableevent.jdbc.internal.model.EventStatus.FALLBACK_PROCESSING) {
            return recoverFallback(candidate);
        }
        if (!candidate.canRetry()) return recoverTerminalLease(candidate);
        Duration delay = backoff.nextDelay(candidate.attemptCount());
        boolean recovered = Boolean.TRUE.equals(transaction.execute(status -> {
            boolean changed = repository.recoverExpiredLeaseToRetryWait(candidate, delay, LEASE_EXPIRED_ERROR);
            if (changed) afterCommitRecovery(candidate, "RETRY_WAIT");
            return changed;
        }));
        if (recovered) {
            LOG.atInfo().addKeyValue("event", "reliable_event.lease.recovered")
                    .addKeyValue("eventId", candidate.id().value())
                    .addKeyValue("eventType", candidate.eventType())
                    .addKeyValue("eventKey", candidate.eventKey())
                    .addKeyValue("attemptCount", candidate.attemptCount())
                    .addKeyValue("leaseOwner", candidate.leaseOwner())
                    .addKeyValue("targetStatus", "RETRY_WAIT")
                    .log("event=reliable_event.lease.recovered eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} targetStatus=RETRY_WAIT",
                    candidate.id().value(), candidate.eventType(), candidate.eventKey(),
                    candidate.attemptCount(), candidate.leaseOwner());
        }
        return recovered;
    }

    private boolean recoverTerminalLease(ExpiredLeaseCandidate candidate) {
        requireNoActiveTransactionForFallback();
        String owner = "fallback-" + UUID.randomUUID();
        EventFallbackContext.StopReason reason = EventFallbackContext.StopReason.LEASE_EXPIRED;
        EventFallbackContext.FailureClassification classification =
                EventFallbackContext.FailureClassification.RESULT_UNKNOWN;
        Optional<ClaimedEvent> claimed = transaction.execute(status -> repository.recoverExpiredLeaseToFallback(
                candidate, owner, leaseDuration, LEASE_EXPIRED_ERROR, reason.name(), classification.name()));
        if (claimed == null || claimed.isEmpty()) return false;
        return runFallback(candidate, claimed.get(), candidate.attemptCount(), reason, classification,
                LEASE_EXPIRED_ERROR);
    }

    private boolean recoverFallback(ExpiredLeaseCandidate candidate) {
        requireNoActiveTransactionForFallback();
        String owner = "fallback-" + UUID.randomUUID();
        Optional<ClaimedEvent> claimed = transaction.execute(status -> repository.claimExpiredFallbackEvent(
                candidate, owner, leaseDuration));
        if (claimed == null || claimed.isEmpty()) return false;
        EventFallbackContext.StopReason reason = EventFallbackContext.StopReason.valueOf(candidate.fallbackStopReason());
        EventFallbackContext.FailureClassification classification = EventFallbackContext.FailureClassification
                .valueOf(candidate.fallbackFailureClassification());
        return runFallback(candidate, claimed.get(), candidate.attemptCount(), reason, classification,
                candidate.lastError() == null ? LEASE_EXPIRED_ERROR : candidate.lastError());
    }

    private void requireNoActiveTransactionForFallback() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Event fallback lease recovery must run outside an active transaction");
        }
    }

    private boolean runFallback(ExpiredLeaseCandidate candidate, ClaimedEvent claimed, int failureCount,
                                EventFallbackContext.StopReason reason,
                                EventFallbackContext.FailureClassification classification, String summary) {
        boolean completed = false;
        String failureSummary = fallbackHandler == null
                ? bounded("No EventFallbackHandler is configured; original send failure: " + summary) : null;
        if (fallbackHandler != null) {
            StoredEvent event = claimed.event();
            try {
                fallbackHandler.handle(new EventFallbackContext(new FailedEvent(event.id(), event.eventType(),
                        event.eventKey(), event.payloadJson(), event.headersJson()), failureCount,
                        reason, classification, summary));
                completed = true;
            } catch (RuntimeException failure) {
                LOG.warn("event=reliable_event.fallback.failed eventId={} exceptionType={}",
                        event.id().value(), failure.getClass().getName());
                failureSummary = bounded(failure.getClass().getName() + ": " + failure.getMessage()
                        + "; original send failure: " + summary);
            }
        }
        boolean result = completed;
        String persistedFailure = failureSummary;
        String state = completed ? "FALLBACK_COMPLETED" : "FAILED";
        transaction.executeWithoutResult(status -> {
            repository.finishFallback(claimed, result, persistedFailure);
            afterCommit(state);
            afterCommitRecovery(candidate, state);
        });
        return true;
    }

    private String bounded(String summary) {
        String safe = summary == null ? "Fallback failed" : summary;
        int count = safe.codePointCount(0, safe.length());
        return count <= 1024 ? safe : safe.substring(0, safe.offsetByCodePoints(0, 1024));
    }

    public void refreshSnapshot() {
        try {
            observer.refreshSnapshot();
        } catch (RuntimeException failure) {
            LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                    failure.getClass().getName());
        }
    }

    /** Automatic scheduler seam; keeps legacy observer behavior through its default method. */
    public void refreshSnapshotAutomatically() {
        try {
            observer.refreshSnapshotAutomatically();
        } catch (RuntimeException failure) {
            LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                    failure.getClass().getName());
        }
    }

    public void recordCycleFailure(boolean automatic, String stage) {
        try { observer.schedulerCycleFailed(automatic, stage); }
        catch (RuntimeException failure) {
            LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                    failure.getClass().getName());
        }
    }

    private void afterCommit(String status) {
        Runnable increment = () -> {
            try { observer.stateTransitionCommitted(status); }
            catch (RuntimeException failure) {
                LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                        failure.getClass().getName());
            }
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() { increment.run(); }
            });
        }
    }

    private void afterCommitRecovery(ExpiredLeaseCandidate candidate, String targetStatus) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { observer.leaseRecovered(candidate, targetStatus); }
                catch (RuntimeException failure) {
                    LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                            failure.getClass().getName());
                }
            }
        });
    }
}
