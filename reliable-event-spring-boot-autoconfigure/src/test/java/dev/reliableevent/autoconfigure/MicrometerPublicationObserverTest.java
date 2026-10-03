package dev.reliableevent.autoconfigure;

import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.internal.model.OutboxMetricsSnapshot;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.PublishedRetentionResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.TimeUnit;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MicrometerPublicationObserverTest {

    private final Instant availableAt = Instant.parse("2026-09-25T00:00:00Z");

    @Test
    void recordsAttemptsLagRecoveryAndDatabaseSnapshotWithoutHighCardinalityTags() {
        JdbcOutboxRepository repository = mock(JdbcOutboxRepository.class);
        when(repository.readMetricsSnapshot(2)).thenReturn(
                new OutboxMetricsSnapshot(3, 2, 4, Double.NaN, 6, 17.5, 2));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPublicationObserver observer = new MicrometerPublicationObserver(registry, repository);
        ClaimedEvent event = event(availableAt);

        assertThat(registry.get("reliable_event.backlog").gauge().value()).isNaN();
        observer.sendFailed(event, EventSendFailureType.RESULT_UNKNOWN, 10_000_000);
        observer.sendSucceeded(event, new SendReceipt("message-1"), 20_000_000,
                availableAt.plusSeconds(4));
        observer.leaseRecovered(new ExpiredLeaseCandidate(new EventId(1), 1, "worker-1",
                availableAt, 1, 2), false);
        observer.leaseRecovered(new ExpiredLeaseCandidate(new EventId(2), 1, "worker-1",
                availableAt, 2, 2), true);
        observer.refreshSnapshot();
        observer.cleanupCompleted(new PublishedRetentionResult(3, 2, 60), 1_000_000);
        observer.cleanupFailed();

        assertThat(registry.get("reliable_event.publish.success").counter().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.publish.failure").counter().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.publish.duration").tag("outcome", "success")
                .timer().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.publish.duration").tag("outcome", "unknown")
                .timer().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.publish.lag").timer().totalTime(TimeUnit.SECONDS))
                .isEqualTo(4);
        assertThat(registry.get("reliable_event.lease.expired").tag("result", "retry_wait")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.lease.expired").tag("result", "dead")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.backlog").gauge().value()).isEqualTo(3);
        assertThat(registry.get("reliable_event.dead").gauge().value()).isEqualTo(2);
        assertThat(registry.get("reliable_event.ready").gauge().value()).isEqualTo(4);
        assertThat(registry.get("reliable_event.ready.oldest_age").gauge().value()).isNaN();
        assertThat(registry.get("reliable_event.unfinished.overdue").gauge().value()).isEqualTo(6);
        assertThat(registry.get("reliable_event.unfinished.overdue.oldest_age").gauge().value())
                .isEqualTo(17.5);
        assertThat(registry.get("reliable_event.unfinished.timestamp_missing").gauge().value()).isEqualTo(2);
        assertThat(registry.get("reliable_event.snapshot.last_success_timestamp").gauge().value()).isPositive();
        assertThat(registry.get("reliable_event.snapshot.failure").counter().count()).isZero();
        assertThat(registry.get("reliable_event.retention.deleted").counter().count()).isEqualTo(2);
        assertThat(registry.get("reliable_event.retention.failed").counter().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.retention.duration").timer().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.retention.oldest_eligible_age").gauge().value())
                .isEqualTo(60);
        assertThat(registry.getMeters()).allSatisfy(meter ->
                assertThat(meter.getId().getTags()).allSatisfy(tag ->
                        assertThat(tag.getKey()).isIn("outcome", "result", "mode", "stage")));

        observer.close();
        assertThat(registry.find("reliable_event.backlog").gauge()).isNull();
    }

    @Test
    void failedRefreshRetainsTheWholePreviousSnapshotAndCountsFailure() {
        JdbcOutboxRepository repository = mock(JdbcOutboxRepository.class);
        when(repository.readMetricsSnapshot(2)).thenReturn(
                new OutboxMetricsSnapshot(7, 1, 2, 3.0, 4, 5.0, 6))
                .thenThrow(new IllegalStateException("database unavailable"));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPublicationObserver observer = new MicrometerPublicationObserver(registry, repository);

        observer.refreshSnapshot();
        double lastSuccess = registry.get("reliable_event.snapshot.last_success_timestamp").gauge().value();
        observer.refreshSnapshot();

        assertThat(registry.get("reliable_event.ready").gauge().value()).isEqualTo(2);
        assertThat(registry.get("reliable_event.unfinished.overdue.oldest_age").gauge().value()).isEqualTo(5);
        assertThat(registry.get("reliable_event.snapshot.last_success_timestamp").gauge().value())
                .isEqualTo(lastSuccess);
        assertThat(registry.get("reliable_event.snapshot.failure").counter().count()).isEqualTo(1);
        observer.close();
    }

    @Test
    void concurrentRefreshReturnsImmediatelyAndKeepsThePublishedSnapshot() {
        JdbcOutboxRepository repository = mock(JdbcOutboxRepository.class);
        when(repository.readMetricsSnapshot(2)).thenReturn(
                new OutboxMetricsSnapshot(1, 0, 2, 12.0, 2, 12.0, 0));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPublicationObserver observer = new MicrometerPublicationObserver(registry, repository);
        observer.refreshSnapshot();
        long activeGeneration = observer.beginSnapshot();

        observer.refreshSnapshot();

        assertThat(activeGeneration).isPositive();
        assertThat(registry.get("reliable_event.ready").gauge().value()).isEqualTo(2);
        assertThat(registry.get("reliable_event.snapshot.failure").counter().count()).isZero();
        org.mockito.Mockito.verify(repository, org.mockito.Mockito.times(1)).readMetricsSnapshot(2);
        observer.finishSnapshot(activeGeneration);
        observer.close();
    }

    @Test
    void prometheusScrapeContainsRequiredMetricsWithOnlyBoundedLabels() {
        JdbcOutboxRepository repository = mock(JdbcOutboxRepository.class);
        when(repository.readMetricsSnapshot(2)).thenReturn(
                new OutboxMetricsSnapshot(1, 2, 3, 4.0, 5, 6.0, 7));
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MicrometerPublicationObserver observer = new MicrometerPublicationObserver(registry, repository);
        observer.refreshSnapshot();
        observer.stateTransitionCommitted("PUBLISHED");
        observer.stateTransitionCommitted("DEAD");
        observer.stateUpdateFailed("PUBLISHED", true);
        observer.schedulerCycleFailed(true, "candidate_scan");

        String scrape = registry.scrape();

        assertThat(scrape).contains(
                "reliable_event_ready ", "reliable_event_ready_oldest_age ",
                "reliable_event_unfinished_overdue ", "reliable_event_unfinished_overdue_oldest_age ",
                "reliable_event_unfinished_timestamp_missing ",
                "reliable_event_snapshot_last_success_timestamp ",
                "reliable_event_publication_persisted_total 1.0",
                "reliable_event_dead_entered_total 1.0",
                "reliable_event_publication_state_update_failure_total 1.0",
                "reliable_event_scheduler_failure_total{mode=\"auto\",stage=\"candidate_scan\"} 1.0");
        assertThat(scrape).doesNotContain("eventId", "event_id", "messageId", "message_id", "eventKey");
        observer.close();
        registry.close();
    }

    @Test
    void unavailableLegacyTimestampDoesNotProduceFalseLag() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPublicationObserver observer = new MicrometerPublicationObserver(
                registry, mock(JdbcOutboxRepository.class));

        observer.sendSucceeded(event(null), new SendReceipt("message-2"), 1,
                availableAt.plusSeconds(10));

        assertThat(registry.get("reliable_event.publish.success").counter().count()).isEqualTo(1);
        assertThat(registry.get("reliable_event.publish.lag").timer().count()).isZero();
        observer.close();
    }

    private ClaimedEvent event(Instant firstAvailableAt) {
        return new ClaimedEvent(new StoredEvent(new EventId(1), "type-1", "key-1", "{}", "{}"),
                1, 1, 2, "worker-1", availableAt.plusSeconds(30), firstAvailableAt);
    }
}
