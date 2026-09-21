package dev.reliableevent;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record ReliableEvent<T>(
        String eventType,
        String eventKey,
        T payload,
        Instant availableAt,
        Map<String, String> headers
) {

    public ReliableEvent {
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("Event type must not be blank");
        }
        if (eventKey == null || eventKey.isBlank()) {
            throw new IllegalArgumentException("Event key must not be blank");
        }
        Objects.requireNonNull(payload, "Event payload must not be null");
        Objects.requireNonNull(availableAt, "Event availableAt must not be null");
        headers = Map.copyOf(Objects.requireNonNull(headers, "Event headers must not be null"));
    }
}
