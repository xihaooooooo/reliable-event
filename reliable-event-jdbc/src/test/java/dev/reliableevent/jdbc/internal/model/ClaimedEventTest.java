package dev.reliableevent.jdbc.internal.model;

import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClaimedEventTest {

    private static final StoredEvent EVENT = new StoredEvent(
            new EventId(1L),
            "coupon-task-execute",
            "101",
            "{\"taskId\":101}",
            "{}"
    );
    private static final Instant LEASE_UNTIL = Instant.parse("2026-09-23T00:00:30Z");

    @Test
    void acceptsAValidLeaseToken() {
        ClaimedEvent claimedEvent = new ClaimedEvent(
                EVENT,
                1L,
                1,
                8,
                "worker-1",
                LEASE_UNTIL
        );

        assertThat(claimedEvent.leaseOwner()).isEqualTo("worker-1");
        assertThat(claimedEvent.leaseUntil()).isEqualTo(LEASE_UNTIL);
        assertThat(claimedEvent.canRetry()).isTrue();
    }

    @Test
    void rejectsABlankLeaseOwner() {
        assertThatThrownBy(() -> new ClaimedEvent(
                EVENT,
                1L,
                1,
                8,
                " ",
                LEASE_UNTIL
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseOwner");
    }

    @Test
    void rejectsAMissingLeaseDeadline() {
        assertThatThrownBy(() -> new ClaimedEvent(
                EVENT,
                1L,
                1,
                8,
                "worker-1",
                null
        )).isInstanceOf(NullPointerException.class)
                .hasMessageContaining("leaseUntil");
    }

    @Test
    void keepsAttemptCountInvariants() {
        assertThatThrownBy(() -> new ClaimedEvent(
                EVENT,
                1L,
                0,
                8,
                "worker-1",
                LEASE_UNTIL
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attemptCount");
        assertThatThrownBy(() -> new ClaimedEvent(
                EVENT,
                1L,
                2,
                1,
                "worker-1",
                LEASE_UNTIL
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not exceed");
    }
}
