package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

import java.time.Instant;

/**
 * Details for restricted operational access. The event key and error summary may
 * contain sensitive business data; callers must control access and avoid logging them.
 */
public record DeadEventDetails(
        EventId id,
        String eventType,
        String eventKey,
        int attemptCount,
        int maxAttempts,
        Instant createdAt,
        Instant deadAt,
        long version,
        String lastError
) {
    @Override
    public String toString() {
        return "DeadEventDetails[id=" + id + ", eventType=" + eventType
                + ", attemptCount=" + attemptCount + ", maxAttempts=" + maxAttempts
                + ", createdAt=" + createdAt + ", deadAt=" + deadAt
                + ", version=" + version + "]";
    }
}
