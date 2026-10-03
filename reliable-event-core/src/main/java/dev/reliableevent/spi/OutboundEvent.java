package dev.reliableevent.spi;

import dev.reliableevent.EventId;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable view of one persisted event supplied to a transport for one send attempt.
 * {@code payloadJson} is the original JSON text and must be sent as UTF-8 without
 * deserializing and reserializing the business payload. Headers are an immutable copy;
 * keys and values must satisfy the ReliableEvent header limits and reserved-name rules.
 */
public record OutboundEvent(EventId id, String eventType, String eventKey, String payloadJson,
                            Map<String, String> headers) {
    public OutboundEvent {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(eventKey, "eventKey must not be null");
        Objects.requireNonNull(payloadJson, "payloadJson must not be null");
        headers = Map.copyOf(Objects.requireNonNull(headers, "headers must not be null"));
    }
}
