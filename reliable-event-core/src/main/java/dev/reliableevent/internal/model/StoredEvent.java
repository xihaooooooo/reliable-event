package dev.reliableevent.internal.model;

import dev.reliableevent.EventId;

public record StoredEvent(
        EventId id,
        String eventType,
        String eventKey,
        String payloadJson,
        String headersJson
) {
}
