package dev.reliableevent.autoconfigure;

import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.internal.model.OutboxMetricsSnapshot;
import dev.reliableevent.jdbc.PublishedRetentionResult;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.HashMap;

final class MicrometerPublicationObserver implements PublicationObserver, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MicrometerPublicationObserver.class);

    private final MeterRegistry registry;
    private final JdbcOutboxRepository repository;
    private final List<Meter> ownedMeters = new ArrayList<>();
    private final Counter success;
    private final Counter failure;
    private final Counter recoveredRetry;
    private final Counter recoveredFailed;
    private final Counter recoveredFallbackCompleted;
    private final Counter cleanupDeleted;
    private final Counter cleanupFailures;
    private final Timer cleanupDuration;
    private final Timer successDuration;
    private final Timer lag;
    private final EnumMap<TransportFailureType, Timer> failedDurations =
            new EnumMap<>(TransportFailureType.class);
    private final AtomicReference<Double> oldestEligibleAge = new AtomicReference<>(Double.NaN);
    private record PublishedSnapshot(OutboxMetricsSnapshot value, long lastSuccessEpochSeconds) { }
    private final AtomicReference<PublishedSnapshot> snapshot = new AtomicReference<>();
    private final AtomicLong lastAutomaticSnapshotNanos = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean periodicEnabled = new AtomicBoolean();
    private final AtomicBoolean schedulerEnabled = new AtomicBoolean();
    private final AtomicBoolean schedulerRunning = new AtomicBoolean();
    private final AtomicLong schedulerLastSuccess = new AtomicLong();
    private final AtomicLong inflight = new AtomicLong();
    private final AtomicLong queued = new AtomicLong();
    private final Map<String, Counter> schedulerFailures = new HashMap<>();
    private final Counter snapshotFailures;
    private final Counter stateUpdateFailures;
    private final Counter persisted;
    private final Counter failedEntered;
    private final Counter fallbackCompleted;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object snapshotLock = new Object();
    private final AtomicBoolean snapshotInProgress = new AtomicBoolean();
    private boolean snapshotAdmissionOpen = true;
    private long snapshotGeneration;
    private final int snapshotQueryTimeoutSeconds;
    private volatile long automaticSnapshotIntervalNanos = Duration.ofSeconds(15).toNanos();

    MicrometerPublicationObserver(MeterRegistry registry, JdbcTemplate jdbcTemplate) {
        this(registry, new JdbcOutboxRepository(jdbcTemplate), Duration.ofSeconds(2));
    }

    MicrometerPublicationObserver(MeterRegistry registry, JdbcOutboxRepository repository) {
        this(registry, repository, Duration.ofSeconds(2));
    }

    MicrometerPublicationObserver(MeterRegistry registry, JdbcOutboxRepository repository,
                                  Duration snapshotQueryTimeout) {
        this.registry = registry;
        this.repository = repository;
        this.snapshotQueryTimeoutSeconds = Math.max(1, (int) Math.ceil(snapshotQueryTimeout.toMillis() / 1000.0));
        success = own(Counter.builder("reliable_event.publish.success").register(registry));
        failure = own(Counter.builder("reliable_event.publish.failure").register(registry));
        recoveredRetry = own(Counter.builder("reliable_event.lease.expired")
                .tag("result", "retry_wait").register(registry));
        recoveredFailed = own(Counter.builder("reliable_event.lease.expired")
                .tag("result", "failed").register(registry));
        recoveredFallbackCompleted = own(Counter.builder("reliable_event.lease.expired")
                .tag("result", "fallback_completed").register(registry));
        cleanupDeleted = own(Counter.builder("reliable_event.retention.deleted").register(registry));
        cleanupFailures = own(Counter.builder("reliable_event.retention.failed").register(registry));
        cleanupDuration = own(Timer.builder("reliable_event.retention.duration").register(registry));
        successDuration = own(Timer.builder("reliable_event.publish.duration")
                .tag("outcome", "success").register(registry));
        failedDurations.put(TransportFailureType.RETRYABLE, duration("retryable"));
        failedDurations.put(TransportFailureType.NON_RETRYABLE, duration("non_retryable"));
        failedDurations.put(TransportFailureType.RESULT_UNKNOWN, duration("unknown"));
        lag = own(Timer.builder("reliable_event.publish.lag").register(registry));
        own(Gauge.builder("reliable_event.backlog", snapshot, value -> {
            var current = value.get(); return current == null ? Double.NaN : (double) current.value().backlog();
        }).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.failed", snapshot, value -> {
            var current = value.get(); return current == null ? Double.NaN : (double) current.value().failed();
        }).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.retention.oldest_eligible_age", oldestEligibleAge,
                AtomicReference::get).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.ready", snapshot, value -> {
            var current = value.get(); return current == null ? Double.NaN : (double) current.value().ready();
        }).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.ready.oldest_age", snapshot, value -> {
            var current = value.get(); return current == null ? Double.NaN : current.value().readyOldestAgeSeconds();
        }).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.unfinished.overdue", snapshot, value -> {
            var current = value.get(); return current == null ? Double.NaN : (double) current.value().unfinishedOverdue();
        }).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.unfinished.overdue.oldest_age", snapshot, value -> {
            var current = value.get(); return current == null ? Double.NaN : current.value().unfinishedOldestAgeSeconds();
        }).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.unfinished.timestamp_missing", snapshot, value -> {
            var current = value.get(); return current == null ? Double.NaN : (double) current.value().unfinishedTimestampMissing();
        }).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.snapshot.last_success_timestamp", snapshot, value -> {
            var current = value.get(); return current == null ? 0 : current.lastSuccessEpochSeconds();
        }).strongReference(true).register(registry));
        own(Gauge.builder("reliable_event.snapshot.periodic_enabled", periodicEnabled,
                value -> value.get() ? 1 : 0).register(registry));
        own(Gauge.builder("reliable_event.scheduler.enabled", schedulerEnabled,
                value -> value.get() ? 1 : 0).register(registry));
        own(Gauge.builder("reliable_event.scheduler.running", schedulerRunning,
                value -> value.get() ? 1 : 0).register(registry));
        own(Gauge.builder("reliable_event.scheduler.last_success_timestamp", schedulerLastSuccess,
                AtomicLong::doubleValue).register(registry));
        own(Gauge.builder("reliable_event.worker.inflight", inflight,
                AtomicLong::doubleValue).register(registry));
        own(Gauge.builder("reliable_event.worker.queued", queued,
                AtomicLong::doubleValue).register(registry));
        snapshotFailures = own(Counter.builder("reliable_event.snapshot.failure").register(registry));
        stateUpdateFailures = own(Counter.builder("reliable_event.publication.state_update_failure").register(registry));
        persisted = own(Counter.builder("reliable_event.publication.persisted").register(registry));
        failedEntered = own(Counter.builder("reliable_event.failed.entered").register(registry));
        fallbackCompleted = own(Counter.builder("reliable_event.fallback.completed").register(registry));
        for (String mode : List.of("manual", "auto")) {
            for (String stage : List.of("candidate_scan", "lease_recovery", "dispatch", "reschedule", "other")) {
                schedulerFailures.put(mode + ":" + stage, own(Counter.builder("reliable_event.scheduler.failure")
                        .tags("mode", mode, "stage", stage).register(registry)));
            }
        }
    }

    @Override
    public void sendSucceeded(ClaimedEvent event, TransportReceipt receipt, long elapsedNanos, Instant at) {
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
    public void sendFailed(ClaimedEvent event, TransportFailureType type, long elapsedNanos) {
        failure.increment();
        failedDurations.get(type).record(Duration.ofNanos(Math.max(0, elapsedNanos)));
    }

    @Override
    public void leaseRecovered(ExpiredLeaseCandidate candidate, boolean failedResult) {
        (failedResult ? recoveredFailed : recoveredRetry).increment();
    }

    @Override
    public void leaseRecovered(ExpiredLeaseCandidate candidate, String targetStatus) {
        switch (targetStatus) {
            case "RETRY_WAIT" -> recoveredRetry.increment();
            case "FAILED" -> recoveredFailed.increment();
            case "FALLBACK_COMPLETED" -> recoveredFallbackCompleted.increment();
            default -> throw new IllegalArgumentException("Unknown expired-lease target status: " + targetStatus);
        }
    }

    @Override
    public void refreshSnapshot() {
        long generation = beginSnapshot();
        if (generation < 0) return;
        try {
            publishSnapshot(generation, readSnapshot());
        } catch (RuntimeException failure) {
            snapshotFailure(generation);
            LOG.warn("event=reliable_event.metrics.snapshot_failed exceptionType={}",
                    failure.getClass().getName());
        } finally {
            finishSnapshot(generation);
        }
    }

    @Override
    public void refreshSnapshotAutomatically() {
        if (periodicEnabled.get()) return;
        long now = System.nanoTime();
        while (true) {
            long previous = lastAutomaticSnapshotNanos.get();
            if (previous != Long.MIN_VALUE && now - previous < automaticSnapshotIntervalNanos) return;
            if (lastAutomaticSnapshotNanos.compareAndSet(previous, now)) break;
        }
        refreshSnapshot();
    }

    OutboxMetricsSnapshot readSnapshot() {
        return repository.readMetricsSnapshot(snapshotQueryTimeoutSeconds);
    }

    long beginSnapshot() {
        synchronized (snapshotLock) {
            if (closed.get()) {
                LOG.debug("event=reliable_event.metrics.snapshot_skipped reason=closed");
                return -1;
            }
            if (!snapshotAdmissionOpen) {
                LOG.debug("event=reliable_event.metrics.snapshot_skipped reason=admission_closed");
                return -1;
            }
            if (!snapshotInProgress.compareAndSet(false, true)) {
                LOG.debug("event=reliable_event.metrics.snapshot_skipped reason=busy");
                return -1;
            }
            return ++snapshotGeneration;
        }
    }

    void publishSnapshot(long generation, OutboxMetricsSnapshot value) {
        synchronized (snapshotLock) {
            if (closed.get() || generation != snapshotGeneration) return;
            snapshot.set(new PublishedSnapshot(value, Instant.now().getEpochSecond()));
        }
    }

    void snapshotFailure(long generation) {
        synchronized (snapshotLock) {
            if (!closed.get() && generation == snapshotGeneration) snapshotFailures.increment();
        }
    }

    void invalidateSnapshot(long generation) {
        synchronized (snapshotLock) {
            if (generation == snapshotGeneration) snapshotGeneration++;
        }
    }

    void stopSnapshotAdmission() {
        synchronized (snapshotLock) {
            snapshotAdmissionOpen = false;
            snapshotGeneration++;
        }
    }

    void finishSnapshot(long generation) {
        synchronized (snapshotLock) {
            snapshotInProgress.set(false);
        }
    }

    @Override
    public void stateUpdated(String status, boolean commitPending) {
        // The state transition counters required for alerting are the commit-bound counters below.
    }

    @Override
    public void stateUpdateFailed(String status, boolean ownershipRejected) {
        stateUpdateFailures.increment();
    }

    @Override
    public void stateTransitionCommitted(String status) {
        if (closed.get()) return;
        if ("PUBLISHED".equals(status)) persisted.increment();
        if ("FAILED".equals(status)) failedEntered.increment();
        if ("FALLBACK_COMPLETED".equals(status)) fallbackCompleted.increment();
    }

    @Override
    public void schedulerState(boolean enabled, boolean running) {
        schedulerEnabled.set(enabled);
        schedulerRunning.set(running);
    }

    @Override
    public void schedulerCycleCompleted(boolean automatic, long completedAtEpochSeconds) {
        if (automatic && !closed.get()) schedulerLastSuccess.set(completedAtEpochSeconds);
    }

    @Override
    public void schedulerCycleFailed(boolean automatic, String stage) {
        String mode = automatic ? "auto" : "manual";
        Counter counter = schedulerFailures.get(mode + ":" + stage);
        if (counter == null) counter = schedulerFailures.get(mode + ":other");
        if (counter != null) counter.increment();
    }

    @Override public void workerStarted() { inflight.incrementAndGet(); }
    @Override public void workerFinished() { decrement(inflight, "worker.inflight"); }
    @Override public void candidateQueued() { queued.incrementAndGet(); }
    @Override public void candidateDequeued() { decrement(queued, "worker.queued"); }

    private void decrement(AtomicLong value, String name) {
        while (true) {
            long current = value.get();
            if (current <= 0) {
                LOG.error("event=reliable_event.metrics.counter_underflow name={}", name);
                return;
            }
            if (value.compareAndSet(current, current - 1)) return;
        }
    }

    void periodicEnabled(boolean enabled) { periodicEnabled.set(enabled); }
    void automaticSnapshotInterval(Duration interval) { automaticSnapshotIntervalNanos = interval.toNanos(); }

    @Override
    public void cleanupCompleted(PublishedRetentionResult result, long elapsedNanos) {
        cleanupDeleted.increment(result.deleted());
        cleanupDuration.record(Duration.ofNanos(Math.max(0, elapsedNanos)));
        oldestEligibleAge.set((double) result.oldestEligibleAgeSeconds());
    }

    @Override
    public void cleanupFailed() {
        cleanupFailures.increment();
    }

    @Override
    public void close() {
        synchronized (snapshotLock) {
            if (closed.compareAndSet(false, true)) {
                snapshotAdmissionOpen = false;
                snapshotGeneration++;
                ownedMeters.forEach(registry::remove);
            }
        }
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
