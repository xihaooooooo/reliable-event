package dev.reliableevent.jdbc;

import java.util.Objects;

record ClaimedEvent(
        StoredEvent event,
        long claimVersion,
        int attemptCount,
        int maxAttempts
) {

    ClaimedEvent {
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
    }

    boolean canRetry() {
        return attemptCount < maxAttempts;
    }
}
