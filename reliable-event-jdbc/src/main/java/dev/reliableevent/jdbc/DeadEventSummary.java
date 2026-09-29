package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

import java.time.Instant;

/** A dead event's list fields, excluding its business key and failure text. */
public record DeadEventSummary(
        EventId id,
        String eventType,
        int attemptCount,
        int maxAttempts,
        Instant deadAt,
        long version
) { }
