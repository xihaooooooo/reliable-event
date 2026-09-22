package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

import java.util.Objects;

record EventCandidate(EventId id, long version) {

    EventCandidate {
        Objects.requireNonNull(id, "id must not be null");
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
    }
}
