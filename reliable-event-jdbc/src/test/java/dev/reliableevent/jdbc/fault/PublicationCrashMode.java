package dev.reliableevent.jdbc.fault;

enum PublicationCrashMode {
    AFTER_CLAIM_BEFORE_SEND,
    AFTER_DELIVERY_BEFORE_STATE_UPDATE;

    static PublicationCrashMode parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("crash mode must not be blank");
        }
        try {
            return valueOf(value);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("unknown crash mode: " + value, exception);
        }
    }
}
