package dev.reliableevent.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

final class JdbcEventPublicationWorker {

    private final JdbcOutboxRepository repository;
    private final EventSender sender;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final int batchSize;
    private final ExponentialBackoff backoff;

    JdbcEventPublicationWorker(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            EventSender sender,
            Clock clock,
            int batchSize
    ) {
        this(
                jdbcTemplate,
                transactionManager,
                sender,
                clock,
                batchSize,
                ExponentialBackoff.defaults()
        );
    }

    JdbcEventPublicationWorker(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            EventSender sender,
            Clock clock,
            int batchSize,
            ExponentialBackoff backoff
    ) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.repository = new JdbcOutboxRepository(Objects.requireNonNull(jdbcTemplate));
        this.transaction = new TransactionTemplate(Objects.requireNonNull(transactionManager));
        this.sender = Objects.requireNonNull(sender);
        this.clock = Objects.requireNonNull(clock);
        this.batchSize = batchSize;
        this.backoff = Objects.requireNonNull(backoff);
    }

    int publishDueEvents() {
        Instant now = clock.instant();
        return publishCandidates(repository.findDueEventCandidates(now, batchSize));
    }

    int publishCandidates(List<EventCandidate> candidates) {
        Objects.requireNonNull(candidates, "candidates must not be null");
        Instant now = clock.instant();
        int publishedCount = 0;
        for (EventCandidate candidate : candidates) {
            ClaimedEvent claimedEvent = transaction.execute(
                    status -> repository.claim(candidate, now).orElse(null)
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
