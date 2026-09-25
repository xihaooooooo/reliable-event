package dev.reliableevent.autoconfigure;

import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.internal.model.OutboxCounts;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
        when(repository.countStatuses()).thenReturn(new OutboxCounts(3, 2));
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
        assertThat(registry.getMeters()).allSatisfy(meter ->
                assertThat(meter.getId().getTags()).allSatisfy(tag ->
                        assertThat(tag.getKey()).isIn("outcome", "result")));

        observer.close();
        assertThat(registry.find("reliable_event.backlog").gauge()).isNull();
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
