package dev.reliableevent;

public record EventId(long value) {

    public EventId {
        if (value <= 0) {
            throw new IllegalArgumentException("Event id must be positive");
        }
    }
}
