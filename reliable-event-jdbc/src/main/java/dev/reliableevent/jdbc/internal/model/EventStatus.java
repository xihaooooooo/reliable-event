package dev.reliableevent.jdbc.internal.model;

public enum EventStatus {
    PENDING(0),
    PUBLISHING(1),
    PUBLISHED(2),
    RETRY_WAIT(3),
    FAILED(4),
    FALLBACK_COMPLETED(5),
    FALLBACK_PROCESSING(6);

    private final int code;


    EventStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
