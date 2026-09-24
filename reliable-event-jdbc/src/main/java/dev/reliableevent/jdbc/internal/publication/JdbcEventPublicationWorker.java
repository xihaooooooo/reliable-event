package dev.reliableevent.jdbc.internal.publication;

import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.EventSendFailureType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public final class JdbcEventPublicationWorker {

    private static final int MAX_WORKER_ID_LENGTH = 128;

    private final JdbcOutboxRepository repository;
    private final EventSender sender;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final int batchSize;
    private final ExponentialBackoff backoff;
    private final String workerId;
    private final Duration leaseDuration;

    public JdbcEventPublicationWorker(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            EventSender sender,
            Clock clock,
            int batchSize,
            String workerId,
            Duration leaseDuration
    ) {
        this(
                jdbcTemplate,
                transactionManager,
                sender,
                clock,
                batchSize,
                workerId,
                leaseDuration,
                ExponentialBackoff.defaults()
        );
    }

    public JdbcEventPublicationWorker(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            EventSender sender,
            Clock clock,
            int batchSize,
            String workerId,
            Duration leaseDuration,
            ExponentialBackoff backoff
    ) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
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
        this.sender = Objects.requireNonNull(sender);
        this.clock = Objects.requireNonNull(clock);
        this.batchSize = batchSize;
        this.workerId = workerId;
        this.leaseDuration = leaseDuration;
        this.backoff = Objects.requireNonNull(backoff);
    }

    public int publishDueEvents() {
        Instant now = clock.instant();
        return publishCandidates(repository.findDueEventCandidates(now, batchSize));
    }

    public int publishCandidates(List<EventCandidate> candidates) {
        Objects.requireNonNull(candidates, "candidates must not be null");
        Instant now = clock.instant();
        int publishedCount = 0;
        for (EventCandidate candidate : candidates) {
            ClaimedEvent claimedEvent = transaction.execute(
                    status -> repository.claim(
                            candidate,
                            now,
                            workerId,
                            leaseDuration
                    ).orElse(null)
            );
            if (claimedEvent == null) {
                continue;
            }

            if (publish(claimedEvent)) {
                publishedCount++;
            }
        }
        return publishedCount;
    }

    private boolean publish(ClaimedEvent claimedEvent) {
        try {
            sender.send(claimedEvent.event());
        } catch (RuntimeException sendFailure) {
            markFailed(claimedEvent, sendFailure);
            return false;
        }

        transaction.executeWithoutResult(
                status -> repository.markPublished(claimedEvent, clock.instant())
        );
        return true;
    }

    private void markFailed(ClaimedEvent claimedEvent, RuntimeException sendFailure) {
        Instant failedAt = clock.instant();
        String lastError = EventFailureSummary.from(sendFailure);
        EventSendFailureType failureType = EventSendFailureClassifier.classify(sendFailure);

        if (failureType == EventSendFailureType.NON_RETRYABLE || !claimedEvent.canRetry()) {
            transaction.executeWithoutResult(
                    status -> repository.markDead(claimedEvent, failedAt, lastError)
            );
            return;
        }

        Instant nextAttemptAt = failedAt.plus(backoff.nextDelay(claimedEvent.attemptCount()));
        transaction.executeWithoutResult(status -> repository.markRetryWait(
                claimedEvent,
                failedAt,
                nextAttemptAt,
                lastError
        ));
    }
}
