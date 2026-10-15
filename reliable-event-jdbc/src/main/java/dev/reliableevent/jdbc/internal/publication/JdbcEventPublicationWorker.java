package dev.reliableevent.jdbc.internal.publication;

import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.JdbcPublicationStore;
import dev.reliableevent.jdbc.internal.persistence.StaleEventClaimException;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.EventFallbackContext;
import dev.reliableevent.EventFallbackHandler;
import dev.reliableevent.FailedEvent;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

public final class JdbcEventPublicationWorker {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcEventPublicationWorker.class);
    private final JdbcPublicationStore store;
    private final EventTransport transport;
    private final Clock clock;
    private final ExponentialBackoff backoff;
    private final PublicationObserver observer;
    private final PublicationTracer publicationTracer;
    private final EventFallbackHandler fallbackHandler;

    public JdbcEventPublicationWorker(
            JdbcPublicationStore store,
            EventTransport transport,
            Clock clock,
            ExponentialBackoff backoff,
            PublicationObserver observer,
            PublicationTracer publicationTracer
    ) {
        this(store, transport, clock, backoff, observer, publicationTracer, null);
    }

    public JdbcEventPublicationWorker(
            JdbcPublicationStore store, EventTransport transport, Clock clock,
            ExponentialBackoff backoff, PublicationObserver observer,
            PublicationTracer publicationTracer, EventFallbackHandler fallbackHandler
    ) {
        this.store = Objects.requireNonNull(store);
        this.transport = Objects.requireNonNull(transport);
        this.clock = Objects.requireNonNull(clock);
        this.backoff = Objects.requireNonNull(backoff);
        this.observer = Objects.requireNonNull(observer);
        this.publicationTracer = Objects.requireNonNull(publicationTracer);
        this.fallbackHandler = fallbackHandler;
    }

    public boolean publishClaimedEvent(ClaimedEvent claimedEvent) {
        return publish(Objects.requireNonNull(claimedEvent, "claimedEvent must not be null"));
    }

    private boolean publish(ClaimedEvent claimedEvent) {
        observe(observer::workerStarted);
        PublicationTracer.Attempt tracing = beginAttempt(claimedEvent);
        try {
            return publish(claimedEvent, tracing);
        } finally {
            safelyTrace(tracing::close);
            observe(observer::workerFinished);
        }
    }

    private boolean publish(ClaimedEvent claimedEvent, PublicationTracer.Attempt tracing) {
        StoredEvent sendEvent = eventForSend(tracing, claimedEvent.event());
        long started = System.nanoTime();
        TransportReceipt receipt;
        try {
            receipt = transport.send(OutboundEventMapper.map(sendEvent));
            if (receipt == null) {
                throw new TransportException(TransportFailureType.RESULT_UNKNOWN, "Event transport returned without a receipt");
            }
        } catch (RuntimeException failure) {
            TransportException sendFailure = failure instanceof TransportException classified
                    ? classified
                    : new TransportException(TransportFailureType.RESULT_UNKNOWN,
                            "Transport failed with an unclassified exception", failure);
            long elapsed = System.nanoTime() - started;
            TransportFailureType type = sendFailure.failureType();
            safelyTrace(() -> tracing.sendFailed(type, sendFailure));
            observe(() -> observer.sendFailed(claimedEvent, type, elapsed));
            boolean terminal = type == TransportFailureType.NON_RETRYABLE || !claimedEvent.canRetry();
            eventLog(LOG.atWarn(), claimedEvent)
                    .addKeyValue("event", "reliable_event.publish.failed")
                    .addKeyValue("failureType", type)
                    .addKeyValue("targetStatus", terminal ? "FALLBACK_PROCESSING" : "RETRY_WAIT")
                    .addKeyValue("exceptionType", sendFailure.getClass().getName())
                    .log("event=reliable_event.publish.failed eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} failureType={} targetStatus={} exceptionType={}",
                    claimedEvent.event().id().value(), claimedEvent.event().eventType(),
                    claimedEvent.event().eventKey(), claimedEvent.attemptCount(),
                    claimedEvent.leaseOwner(), type, terminal ? "FALLBACK_PROCESSING" : "RETRY_WAIT",
                    sendFailure.getClass().getName());
            try {
                markFailed(claimedEvent, sendFailure, type, tracing);
            } catch (RuntimeException stateFailure) {
                logStateFailure(claimedEvent, stateFailure);
                throw stateFailure;
            }
            return false;
        }

        long elapsed = System.nanoTime() - started;
        Instant acknowledgedAt = clock.instant();
        TransportReceipt acknowledged = receipt;
        safelyTrace(() -> tracing.sendSucceeded(acknowledged));
        observe(() -> observer.sendSucceeded(claimedEvent, acknowledged, elapsed, acknowledgedAt));
        String messageId = receipt.brokerMessageId().orElse(null);
        var sendSuccessLog = eventLog(LOG.atInfo(), claimedEvent)
                .addKeyValue("event", "reliable_event.send.succeeded");
        if (messageId == null) {
            sendSuccessLog.log("event=reliable_event.send.succeeded eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={}",
                    claimedEvent.event().id().value(), claimedEvent.event().eventType(),
                    claimedEvent.event().eventKey(), claimedEvent.attemptCount(), claimedEvent.leaseOwner());
        } else {
            sendSuccessLog.addKeyValue("messageId", messageId)
                    .log("event=reliable_event.send.succeeded eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} messageId={}",
                            claimedEvent.event().id().value(), claimedEvent.event().eventType(),
                            claimedEvent.event().eventKey(), claimedEvent.attemptCount(),
                            claimedEvent.leaseOwner(), messageId);
        }
        boolean outerTransaction = TransactionSynchronizationManager.isActualTransactionActive();
        try {
            store.markPublished(claimedEvent);
            safelyTrace(() -> tracing.stateUpdated("PUBLISHED", outerTransaction));
            var persistedLog = eventLog(LOG.atInfo(), claimedEvent)
                    .addKeyValue("event", "reliable_event.publish.persisted")
                    .addKeyValue("status", "PUBLISHED");
            if (messageId == null) {
                persistedLog.log("event=reliable_event.publish.persisted eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} status=PUBLISHED",
                        claimedEvent.event().id().value(), claimedEvent.event().eventType(),
                        claimedEvent.event().eventKey(), claimedEvent.attemptCount(), claimedEvent.leaseOwner());
            } else {
                persistedLog.addKeyValue("messageId", messageId)
                        .log("event=reliable_event.publish.persisted eventId={} eventType={} eventKey={} attemptCount={} leaseOwner={} messageId={} status=PUBLISHED",
                                claimedEvent.event().id().value(), claimedEvent.event().eventType(),
                                claimedEvent.event().eventKey(), claimedEvent.attemptCount(),
                                claimedEvent.leaseOwner(), messageId);
            }
        } catch (RuntimeException stateFailure) {
            safelyTrace(() -> tracing.stateUpdateFailed("PUBLISHED", stateFailure,
                    stateFailure instanceof StaleEventClaimException));
            logStateFailure(claimedEvent, stateFailure);
            throw stateFailure;
        }
        return true;
    }

    private void markFailed(ClaimedEvent claimedEvent, RuntimeException sendFailure,
                            TransportFailureType failureType, PublicationTracer.Attempt tracing) {
        Instant failedAt = clock.instant();
        String lastError = EventFailureSummary.from(sendFailure);
        boolean outerTransaction = TransactionSynchronizationManager.isActualTransactionActive();

        if (failureType == TransportFailureType.NON_RETRYABLE || !claimedEvent.canRetry()) {
            requireNoActiveTransactionForFallback();
            try {
                EventFallbackContext.StopReason stopReason = failureType == TransportFailureType.NON_RETRYABLE
                        ? EventFallbackContext.StopReason.NON_RETRYABLE
                        : EventFallbackContext.StopReason.ATTEMPTS_EXHAUSTED;
                EventFallbackContext.FailureClassification classification = fallbackClassification(failureType);
                ClaimedEvent processing = store.beginFallback(claimedEvent, lastError,
                        stopReason.name(), classification.name());
                FallbackOutcome outcome = invokeFallback(processing, stopReason, classification, lastError);
                store.finishFallback(processing, outcome.completed(), outcome.failureSummary());
                String status = outcome.completed() ? "FALLBACK_COMPLETED" : "FAILED";
                safelyTrace(() -> tracing.stateUpdated(status, outerTransaction));
            } catch (RuntimeException stateFailure) {
                safelyTrace(() -> tracing.stateUpdateFailed("FALLBACK_PROCESSING", stateFailure,
                        stateFailure instanceof StaleEventClaimException));
                throw stateFailure;
            }
            return;
        }

        Instant nextAttemptAt = failedAt.plus(backoff.nextDelay(claimedEvent.attemptCount()));
        try {
            store.markRetryWait(claimedEvent, failedAt, nextAttemptAt, lastError);
            safelyTrace(() -> tracing.stateUpdated("RETRY_WAIT", outerTransaction));
        } catch (RuntimeException stateFailure) {
            safelyTrace(() -> tracing.stateUpdateFailed("RETRY_WAIT", stateFailure,
                    stateFailure instanceof StaleEventClaimException));
            throw stateFailure;
        }
    }

    private void requireNoActiveTransactionForFallback() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Event fallback must run outside an active transaction; "
                    + "the claimed event will be handled by lease recovery");
        }
    }

    private FallbackOutcome invokeFallback(ClaimedEvent claimedEvent, EventFallbackContext.StopReason reason,
                                           EventFallbackContext.FailureClassification classification,
                                           String originalSummary) {
        if (fallbackHandler == null) return new FallbackOutcome(false,
                combineErrors("No EventFallbackHandler is configured", originalSummary));
        StoredEvent stored = claimedEvent.event();
        try {
            fallbackHandler.handle(new EventFallbackContext(new FailedEvent(stored.id(), stored.eventType(),
                    stored.eventKey(), stored.payloadJson(), stored.headersJson()), claimedEvent.attemptCount(),
                    reason, classification, originalSummary));
            return new FallbackOutcome(true, null);
        } catch (RuntimeException fallbackFailure) {
            LOG.warn("event=reliable_event.fallback.failed eventId={} exceptionType={}",
                    stored.id().value(), fallbackFailure.getClass().getName());
            return new FallbackOutcome(false,
                    combineErrors(EventFailureSummary.from(fallbackFailure), originalSummary));
        }
    }

    private String combineErrors(String fallback, String original) {
        String combined = fallback + "; original send failure: " + original;
        int count = combined.codePointCount(0, combined.length());
        return count <= 1024 ? combined : combined.substring(0, combined.offsetByCodePoints(0, 1024));
    }

    private record FallbackOutcome(boolean completed, String failureSummary) { }

    private EventFallbackContext.FailureClassification fallbackClassification(TransportFailureType type) {
        return switch (type) {
            case RETRYABLE -> EventFallbackContext.FailureClassification.RETRYABLE;
            case NON_RETRYABLE -> EventFallbackContext.FailureClassification.NON_RETRYABLE;
            case RESULT_UNKNOWN -> EventFallbackContext.FailureClassification.RESULT_UNKNOWN;
        };
    }

    private PublicationTracer.Attempt beginAttempt(ClaimedEvent claimedEvent) {
        try {
            PublicationTracer.Attempt attempt = publicationTracer.begin(claimedEvent);
            return attempt == null ? PublicationTracer.NOOP.begin(claimedEvent) : attempt;
        } catch (RuntimeException tracingFailure) {
            return PublicationTracer.NOOP.begin(claimedEvent);
        }
    }

    private StoredEvent eventForSend(PublicationTracer.Attempt tracing, StoredEvent original) {
        try {
            StoredEvent event = tracing.eventForSend();
            return event == null ? original : event;
        } catch (RuntimeException tracingFailure) {
            return original;
        }
    }

    private void safelyTrace(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException ignored) {
            // Tracing is best-effort and must not alter publication behavior.
        }
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
