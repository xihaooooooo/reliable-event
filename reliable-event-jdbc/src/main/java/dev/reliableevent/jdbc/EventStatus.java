package dev.reliableevent.jdbc;

enum EventStatus {
    PENDING(0),
    PUBLISHING(1),
    PUBLISHED(2),
    RETRY_WAIT(3),
    DEAD(4);

    private final int code;

    EventStatus(int code) {
        this.code = code;
    }

    int code() {
        return code;
    }
}
