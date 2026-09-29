package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

import java.util.Objects;

/** The three possible outcomes of looking up one event as a dead event. */
public sealed interface DeadEventLookup {

    record Dead(DeadEventDetails event) implements DeadEventLookup {
        public Dead { Objects.requireNonNull(event, "event must not be null"); }
    }

    record NotDead(EventId id) implements DeadEventLookup {
        public NotDead { Objects.requireNonNull(id, "id must not be null"); }
    }

    record NotFound(EventId id) implements DeadEventLookup {
        public NotFound { Objects.requireNonNull(id, "id must not be null"); }
    }
}
