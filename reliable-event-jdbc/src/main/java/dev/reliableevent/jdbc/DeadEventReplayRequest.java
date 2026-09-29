package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

import java.util.Objects;

/** One explicit, version-guarded request to replay a dead event. */
public record DeadEventReplayRequest(
        EventId id,
        long expectedVersion,
        String operator,
        String reason
) {
    public DeadEventReplayRequest {
        Objects.requireNonNull(id, "id must not be null");
        if (expectedVersion < 0 || expectedVersion == Long.MAX_VALUE) {
            throw new IllegalArgumentException("expectedVersion must be between 0 and Long.MAX_VALUE - 1");
        }
        operator = requiredText(operator, "operator", 128);
        reason = requiredText(reason, "reason", 1024);
    }

    private static String requiredText(String value, String name, int maxLength) {
        Objects.requireNonNull(value, name + " must not be null");
        String normalized = value.strip();
        if (normalized.isEmpty() || normalized.codePointCount(0, normalized.length()) > maxLength) {
            throw new IllegalArgumentException(name + " must contain 1 to " + maxLength + " characters");
        }
        return normalized;
    }

    @Override
    public String toString() {
        return "DeadEventReplayRequest[id=" + id + ", expectedVersion=" + expectedVersion + "]";
    }
}
