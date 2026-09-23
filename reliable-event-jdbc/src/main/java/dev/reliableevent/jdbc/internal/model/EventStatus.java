package dev.reliableevent.jdbc.internal.model;

public enum EventStatus {
    PENDING(0),
    PUBLISHING(1),
    PUBLISHED(2),
    RETRY_WAIT(3),
    DEAD(4);

    private final int code;

    EventStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
