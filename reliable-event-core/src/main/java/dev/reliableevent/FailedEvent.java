package dev.reliableevent;

import java.util.Objects;

/** Persisted event data supplied to an application fallback, including unvalidated headers. */
public record FailedEvent(EventId id, String eventType, String eventKey, String payloadJson,
                          String headersJson) {
    public FailedEvent {
        Objects.requireNonNull(id, "id must not be null");
    }

    @Override public String toString() { return "FailedEvent[id=" + id + ", eventType=" + eventType + "]"; }
}
