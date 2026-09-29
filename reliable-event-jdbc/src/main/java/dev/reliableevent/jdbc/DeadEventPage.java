package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A page ordered by descending event ID. The cursor is empty on the final page. */
public record DeadEventPage(List<DeadEventSummary> events, Optional<EventId> nextCursor) {

    public DeadEventPage {
        events = List.copyOf(Objects.requireNonNull(events, "events must not be null"));
        Objects.requireNonNull(nextCursor, "nextCursor must not be null");
    }
}
