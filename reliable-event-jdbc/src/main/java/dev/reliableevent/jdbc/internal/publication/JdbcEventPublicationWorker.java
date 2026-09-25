package dev.reliableevent.jdbc.internal.publication;

import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class JdbcEventPublicationWorker {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcEventPublicationWorker.class);
    private static final int MAX_WORKER_ID_LENGTH = 128;

    private final JdbcOutboxRepository repository;
    private final EventSender sender;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final int batchSize;
    private final ExponentialBackoff backoff;
    private final String workerId;
    private final Duration leaseDuration;
    private final PublicationObserver observer;

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
        this(jdbcTemplate, transactionManager, sender, clock, batchSize, workerId,
                leaseDuration, backoff, PublicationObserver.NOOP);
    }

    public JdbcEventPublicationWorker(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            EventSender sender,
            Clock clock,
            int batchSize,
            String workerId,
            Duration leaseDuration,
            ExponentialBackoff backoff,
            PublicationObserver observer
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
        this.observer = Objects.requireNonNull(observer);
    }

    public int publishDueEvents() {
        return publishCandidates(findDueEventCandidates(batchSize));
    }

    public List<EventCandidate> findDueEventCandidates(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return repository.findDueEventCandidates(clock.instant(), limit);
    }

    public int publishCandidates(List<EventCandidate> candidates) {
        Objects.requireNonNull(candidates, "candidates must not be null");
        int publishedCount = 0;
        for (EventCandidate candidate : candidates) {
            if (publishCandidate(candidate)) {
                publishedCount++;
            }
        }
        return publishedCount;
    }

    public boolean publishCandidate(EventCandidate candidate) {
        return claimCandidate(candidate).map(this::publishClaimedEvent).orElse(false);
    }

    public Optional<ClaimedEvent> claimCandidate(EventCandidate candidate) {
        Objects.requireNonNull(candidate, "candidate must not be null");
        Instant now = clock.instant();
        return transaction.execute(
                status -> repository.claim(
                        candidate,
                        now,
                        workerId,
                        leaseDuration
                )
        );
    }

    public boolean publishClaimedEvent(ClaimedEvent claimedEvent) {
        return publish(Objects.requireNonNull(claimedEvent, "claimedEvent must not be null"));
    }

    private boolean publish(ClaimedEvent claimedEvent) {
        long started = System.nanoTime();
        SendReceipt receipt;
        try {
            receipt = sender.send(claimedEvent.event());
            if (receipt == null) {
                throw EventSendException.resultUnknown("Event sender returned without a receipt");
            }
        } catch (RuntimeException sendFailure) {
            EventSendFailureType type = EventSendFailureClassifier.classify(sendFailure);
            long elapsed = System.nanoTime() - started;
            observe(() -> observer.sendFailed(claimedEvent, type, elapsed));
            boolean dead = type == EventSendFailureType.NON_RETRYABLE || !claimedEvent.canRetry();
            eventLog(LOG.atWarn(), claimedEvent)
                    .addKeyValue("event", "reliable_event.publish.failed")
                    .addKeyValue("failureType", type)
                    .addKeyValue("targetStatus", dead ? "DEAD" : "RETRY_WAIT")
                    .addKeyValue("exceptionType", sendFailure.getClass().getName())
                    .log("event=reliable_event.publish.failed eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} failureType={} targetStatus={} exceptionType={}",
                    claimedEvent.event().id().value(), claimedEvent.event().eventType(),
                    claimedEvent.event().eventKey(), claimedEvent.attemptCount(),
                    claimedEvent.leaseOwner(), type, dead ? "DEAD" : "RETRY_WAIT",
                    sendFailure.getClass().getName());
            try {
                markFailed(claimedEvent, sendFailure, type);
            } catch (RuntimeException stateFailure) {
                logStateFailure(claimedEvent, stateFailure);
                throw stateFailure;
            }
            return false;
        }

        long elapsed = System.nanoTime() - started;
        Instant acknowledgedAt = clock.instant();
        SendReceipt acknowledged = receipt;
        observe(() -> observer.sendSucceeded(claimedEvent, acknowledged, elapsed, acknowledgedAt));
        eventLog(LOG.atInfo(), claimedEvent)
                .addKeyValue("event", "reliable_event.send.succeeded")
                .addKeyValue("messageId", receipt.messageId())
                .log("event=reliable_event.send.succeeded eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} messageId={}",
                claimedEvent.event().id().value(), claimedEvent.event().eventType(),
                claimedEvent.event().eventKey(), claimedEvent.attemptCount(),
                claimedEvent.leaseOwner(), receipt.messageId());
        try {
            transaction.executeWithoutResult(
                    status -> repository.markPublished(claimedEvent, clock.instant())
            );
            eventLog(LOG.atInfo(), claimedEvent)
                    .addKeyValue("event", "reliable_event.publish.persisted")
                    .addKeyValue("messageId", receipt.messageId())
                    .addKeyValue("status", "PUBLISHED")
                    .log("event=reliable_event.publish.persisted eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} messageId={} status=PUBLISHED",
                    claimedEvent.event().id().value(), claimedEvent.event().eventType(),
                    claimedEvent.event().eventKey(), claimedEvent.attemptCount(),
                    claimedEvent.leaseOwner(), receipt.messageId());
        } catch (RuntimeException stateFailure) {
            logStateFailure(claimedEvent, stateFailure);
            throw stateFailure;
        }
        return true;
    }

    private void markFailed(ClaimedEvent claimedEvent, RuntimeException sendFailure,
                            EventSendFailureType failureType) {
        Instant failedAt = clock.instant();
        String lastError = EventFailureSummary.from(sendFailure);

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

    private void logStateFailure(ClaimedEvent event, RuntimeException failure) {
        eventLog(LOG.atError(), event)
                .addKeyValue("event", "reliable_event.publication.state_update_failed")
                .addKeyValue("exceptionType", failure.getClass().getName())
                .log("event=reliable_event.publication.state_update_failed eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} exceptionType={}",
                event.event().id().value(), event.event().eventType(), event.event().eventKey(),
                event.attemptCount(), event.leaseOwner(), failure.getClass().getName());
    }

    private LoggingEventBuilder eventLog(LoggingEventBuilder builder, ClaimedEvent event) {
        return builder.addKeyValue("eventId", event.event().id().value())
                .addKeyValue("eventType", event.event().eventType())
                .addKeyValue("eventKey", event.event().eventKey())
                .addKeyValue("attemptCount", event.attemptCount())
                .addKeyValue("leaseOwner", event.leaseOwner());
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
