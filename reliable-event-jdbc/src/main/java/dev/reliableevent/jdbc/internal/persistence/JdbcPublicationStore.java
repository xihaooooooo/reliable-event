package dev.reliableevent.jdbc.internal.persistence;

import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Coordinates outbox queries and short state-transition transactions. */
public final class JdbcPublicationStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcPublicationStore.class);
    private static final int MAX_WORKER_ID_LENGTH = 128;

    private final JdbcOutboxRepository repository;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final String workerId;
    private final Duration leaseDuration;
    private final PublicationObserver observer;

    public JdbcPublicationStore(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager,
                                Clock clock, String workerId, Duration leaseDuration,
                                PublicationObserver observer) {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        if (workerId.codePointCount(0, workerId.length()) > MAX_WORKER_ID_LENGTH) {
            throw new IllegalArgumentException("workerId must not exceed 128 characters");
        }
        Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
        if (leaseDuration.toMillis() <= 0) {
            throw new IllegalArgumentException("leaseDuration must be at least one millisecond");
        }
        Math.multiplyExact(leaseDuration.toMillis(), 1_000L);
        this.repository = new JdbcOutboxRepository(Objects.requireNonNull(jdbcTemplate));
        this.transaction = new TransactionTemplate(Objects.requireNonNull(transactionManager));
        this.clock = Objects.requireNonNull(clock);
        this.workerId = workerId;
        this.leaseDuration = leaseDuration;
        this.observer = Objects.requireNonNull(observer);
    }

    public List<EventCandidate> findDueEventCandidates(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return repository.findDueEventCandidates(clock.instant(), limit);
    }

    public Optional<ClaimedEvent> claimCandidate(EventCandidate candidate) {
        Objects.requireNonNull(candidate, "candidate must not be null");
        return transaction.execute(status -> repository.claim(
                candidate, clock.instant(), workerId, leaseDuration));
    }

    public void markPublished(ClaimedEvent event) {
        update("PUBLISHED", () -> repository.markPublished(event, clock.instant()));
    }

    public void markRetryWait(ClaimedEvent event, Instant failedAt,
                              Instant nextAttemptAt, String lastError) {
        update("RETRY_WAIT",
                () -> repository.markRetryWait(event, failedAt, nextAttemptAt, lastError));
    }

    public ClaimedEvent beginFallback(ClaimedEvent event, String summary, String stopReason,
                                      String failureClassification) {
        String owner = "fallback-" + UUID.randomUUID();
        long leaseMicros = Math.multiplyExact(leaseDuration.toMillis(), 1_000L);
        Boolean changed = transaction.execute(status -> repository.beginFallback(event, summary,
                stopReason, failureClassification, owner, leaseMicros));
        if (!Boolean.TRUE.equals(changed)) throw new StaleEventClaimException(event.event().id().value(), event.leaseOwner());
        return new ClaimedEvent(event.event(), event.claimVersion() + 1, event.attemptCount(),
                event.maxAttempts(), owner, clock.instant().plus(leaseDuration), event.firstAvailableAt());
    }

    public Optional<ClaimedEvent> claimExpiredFallback(
            dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate candidate) {
        String owner = "fallback-" + UUID.randomUUID();
        return transaction.execute(status -> repository.claimExpiredFallbackEvent(candidate, owner, leaseDuration));
    }

    public void finishFallback(ClaimedEvent event, boolean completed, String failureSummary) {
        update(completed ? "FALLBACK_COMPLETED" : "FAILED",
                () -> repository.finishFallback(event, completed, failureSummary));
    }

    private void update(String state, Runnable operation) {
        boolean outerTransaction = TransactionSynchronizationManager.isActualTransactionActive();
        try {
            transaction.executeWithoutResult(status -> {
                operation.run();
                if (state.equals("PUBLISHED") || state.equals("FAILED") || state.equals("FALLBACK_COMPLETED")) {
                    afterCommit(state);
                }
            });
            observe(() -> observer.stateUpdated(state, outerTransaction));
        } catch (RuntimeException failure) {
            observe(() -> observer.stateUpdateFailed(state,
                    failure instanceof StaleEventClaimException));
            throw failure;
        }
    }

    private void afterCommit(String state) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new org.springframework.transaction.support.TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            observe(() -> observer.stateTransitionCommitted(state));
                        }
                    }
            );
        }
    }

    private void observe(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException failure) {
            LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                    failure.getClass().getName());
        }
    }
}
