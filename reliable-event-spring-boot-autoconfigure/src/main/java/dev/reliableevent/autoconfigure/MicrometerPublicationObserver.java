package dev.reliableevent.autoconfigure;

import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.internal.model.OutboxCounts;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

final class MicrometerPublicationObserver implements PublicationObserver, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MicrometerPublicationObserver.class);

    private final MeterRegistry registry;
    private final JdbcOutboxRepository repository;
    private final List<Meter> ownedMeters = new ArrayList<>();
    private final Counter success;
    private final Counter failure;
    private final Counter recoveredRetry;
    private final Counter recoveredDead;
    private final Timer successDuration;
    private final Timer lag;
    private final EnumMap<EventSendFailureType, Timer> failedDurations =
            new EnumMap<>(EventSendFailureType.class);
    private final AtomicReference<Double> backlog = new AtomicReference<>(Double.NaN);
    private final AtomicReference<Double> dead = new AtomicReference<>(Double.NaN);

    MicrometerPublicationObserver(MeterRegistry registry, JdbcTemplate jdbcTemplate) {
        this(registry, new JdbcOutboxRepository(jdbcTemplate));
    }

    MicrometerPublicationObserver(MeterRegistry registry, JdbcOutboxRepository repository) {
        this.registry = registry;
        this.repository = repository;
        success = own(Counter.builder("reliable_event.publish.success").register(registry));
        failure = own(Counter.builder("reliable_event.publish.failure").register(registry));
        recoveredRetry = own(Counter.builder("reliable_event.lease.expired")
                .tag("result", "retry_wait").register(registry));
        recoveredDead = own(Counter.builder("reliable_event.lease.expired")
                .tag("result", "dead").register(registry));
        successDuration = own(Timer.builder("reliable_event.publish.duration")
                .tag("outcome", "success").register(registry));
        failedDurations.put(EventSendFailureType.RETRYABLE, duration("retryable"));
        failedDurations.put(EventSendFailureType.NON_RETRYABLE, duration("non_retryable"));
        failedDurations.put(EventSendFailureType.RESULT_UNKNOWN, duration("unknown"));
        lag = own(Timer.builder("reliable_event.publish.lag").register(registry));
        own(Gauge.builder("reliable_event.backlog", backlog, AtomicReference::get)
                .strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.dead", dead, AtomicReference::get)
                .strongReference(true).register(registry));
    }

    @Override
    public void sendSucceeded(ClaimedEvent event, SendReceipt receipt, long elapsedNanos, Instant at) {
        success.increment();
        successDuration.record(Duration.ofNanos(Math.max(0, elapsedNanos)));
        if (event.firstAvailableAt() == null) {
            LOG.warn("event=reliable_event.publish.lag_unavailable eventId={}",
                    event.event().id().value());
            return;
        }
        long lagMillis = Duration.between(event.firstAvailableAt(), at).toMillis();
        if (lagMillis < 0) {
            LOG.warn("event=reliable_event.publish.clock_skew eventId={}",
                    event.event().id().value());
        }
        lag.record(Duration.ofMillis(Math.max(0, lagMillis)));
    }

    @Override
    public void sendFailed(ClaimedEvent event, EventSendFailureType type, long elapsedNanos) {
        failure.increment();
        failedDurations.get(type).record(Duration.ofNanos(Math.max(0, elapsedNanos)));
    }

    @Override
    public void leaseRecovered(ExpiredLeaseCandidate candidate, boolean deadResult) {
        (deadResult ? recoveredDead : recoveredRetry).increment();
    }

    @Override
    public void refreshSnapshot() {
        try {
            OutboxCounts counts = repository.countStatuses();
            backlog.set((double) counts.backlog());
            dead.set((double) counts.dead());
        } catch (RuntimeException failure) {
            LOG.warn("event=reliable_event.metrics.snapshot_failed exceptionType={}",
                    failure.getClass().getName());
        }
    }

    @Override
    public void close() {
        ownedMeters.forEach(registry::remove);
    }

    private Timer duration(String outcome) {
        return own(Timer.builder("reliable_event.publish.duration")
                .tag("outcome", outcome).register(registry));
    }

    private <T extends Meter> T own(T meter) {
        ownedMeters.add(meter);
        return meter;
    }
}
