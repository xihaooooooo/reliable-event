package dev.reliableevent.jdbc.internal.model;

import dev.reliableevent.EventId;

import java.util.Objects;

public record EventCandidate(EventId id, long version) {

    public EventCandidate {
        Objects.requireNonNull(id, "id must not be null");
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
    }
}
