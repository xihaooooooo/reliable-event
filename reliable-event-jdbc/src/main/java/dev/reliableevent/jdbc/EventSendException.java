package dev.reliableevent.jdbc;

import java.util.Objects;

final class EventSendException extends RuntimeException {

    private final EventSendFailureType failureType;

    static EventSendException retryable(String message, Throwable cause) {
        return new EventSendException(EventSendFailureType.RETRYABLE, message, cause);
    }

    static EventSendException nonRetryable(String message, Throwable cause) {
        return new EventSendException(EventSendFailureType.NON_RETRYABLE, message, cause);
    }

    static EventSendException retryable(String message) {
        return retryable(message, null);
    }

    static EventSendException nonRetryable(String message) {
        return nonRetryable(message, null);
    }

    private EventSendException(
            EventSendFailureType failureType,
            String message,
            Throwable cause
    ) {
        super(message, cause);
        this.failureType = Objects.requireNonNull(failureType, "failureType must not be null");
    }

    EventSendFailureType failureType() {
        return failureType;
    }
}
