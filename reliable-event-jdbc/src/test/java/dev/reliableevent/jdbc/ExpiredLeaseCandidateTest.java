package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExpiredLeaseCandidateTest {

    private static final EventId EVENT_ID = new EventId(1L);
    private static final Instant LEASE_UNTIL = Instant.parse("2026-09-23T00:00:00Z");

    @Test
    void reportsWhetherAnotherAttemptIsAvailable() {
        ExpiredLeaseCandidate retryable = candidate(1, 2);
        ExpiredLeaseCandidate exhausted = candidate(2, 2);

        assertThat(retryable.canRetry()).isTrue();
        assertThat(exhausted.canRetry()).isFalse();
    }

    @Test
    void requiresAPositiveVersionAndAttemptCounts() {
        assertThatThrownBy(() -> new ExpiredLeaseCandidate(
                EVENT_ID,
                0L,
                "worker-1",
                LEASE_UNTIL,
                1,
                2
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");
        assertThatThrownBy(() -> new ExpiredLeaseCandidate(
                EVENT_ID,
                1L,
                "worker-1",
                LEASE_UNTIL,
                0,
                2
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attemptCount");
        assertThatThrownBy(() -> new ExpiredLeaseCandidate(
                EVENT_ID,
                1L,
                "worker-1",
                LEASE_UNTIL,
                1,
                0
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts");
    }

    @Test
    void requiresLeaseIdentityAndDeadline() {
        assertThatThrownBy(() -> new ExpiredLeaseCandidate(
                EVENT_ID,
                1L,
                " ",
                LEASE_UNTIL,
                1,
                2
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseOwner");
        assertThatThrownBy(() -> new ExpiredLeaseCandidate(
                EVENT_ID,
                1L,
                "worker-1",
                null,
                1,
                2
        )).isInstanceOf(NullPointerException.class)
                .hasMessageContaining("leaseUntil");
    }

    @Test
    void rejectsAttemptCountsAboveTheMaximum() {
        assertThatThrownBy(() -> candidate(3, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not exceed");
    }

    private ExpiredLeaseCandidate candidate(int attemptCount, int maxAttempts) {
        return new ExpiredLeaseCandidate(
                EVENT_ID,
                1L,
                "worker-1",
                LEASE_UNTIL,
                attemptCount,
                maxAttempts
        );
    }
}
