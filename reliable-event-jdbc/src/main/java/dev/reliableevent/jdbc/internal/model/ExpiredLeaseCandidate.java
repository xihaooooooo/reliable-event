package dev.reliableevent.jdbc.internal.model;

import dev.reliableevent.EventId;

import java.time.Instant;
import java.util.Objects;

public record ExpiredLeaseCandidate(
        EventId id,
        long version,
        String leaseOwner,
        Instant leaseUntil,
        int attemptCount,
        int maxAttempts
) {

    public ExpiredLeaseCandidate {
        Objects.requireNonNull(id, "id must not be null");
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        if (leaseOwner == null || leaseOwner.isBlank()) {
            throw new IllegalArgumentException("leaseOwner must not be blank");
        }
        Objects.requireNonNull(leaseUntil, "leaseUntil must not be null");
        if (attemptCount <= 0) {
            throw new IllegalArgumentException("attemptCount must be positive");
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        if (attemptCount > maxAttempts) {
            throw new IllegalArgumentException("attemptCount must not exceed maxAttempts");
        }
    }

    public boolean canRetry() {
        return attemptCount < maxAttempts;
    }
}
