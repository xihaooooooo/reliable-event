package dev.reliableevent.jdbc.internal.model;

import dev.reliableevent.internal.model.StoredEvent;

import java.time.Instant;
import java.util.Objects;

public record ClaimedEvent(
        StoredEvent event,
        long claimVersion,
        int attemptCount,
        int maxAttempts,
        String leaseOwner,
        Instant leaseUntil
) {

    public ClaimedEvent {
        Objects.requireNonNull(event, "event must not be null");
        if (claimVersion <= 0) {
            throw new IllegalArgumentException("claimVersion must be positive");
        }
        if (attemptCount <= 0) {
            throw new IllegalArgumentException("attemptCount must be positive");
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        if (attemptCount > maxAttempts) {
            throw new IllegalArgumentException("attemptCount must not exceed maxAttempts");
        }
        if (leaseOwner == null || leaseOwner.isBlank()) {
            throw new IllegalArgumentException("leaseOwner must not be blank");
        }
        Objects.requireNonNull(leaseUntil, "leaseUntil must not be null");
    }

    public boolean canRetry() {
        return attemptCount < maxAttempts;
    }
}
