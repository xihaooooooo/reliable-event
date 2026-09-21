package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

record StoredEvent(
        EventId id,
        String eventType,
        String eventKey,
        String payloadJson,
        String headersJson
) {
}
