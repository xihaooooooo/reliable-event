package dev.reliableevent.internal.publication;

import java.util.Objects;

public final class EventSendException extends RuntimeException {

    private final EventSendFailureType failureType;

    public static EventSendException retryable(String message, Throwable cause) {
        return new EventSendException(EventSendFailureType.RETRYABLE, message, cause);
    }

    public static EventSendException nonRetryable(String message, Throwable cause) {
        return new EventSendException(EventSendFailureType.NON_RETRYABLE, message, cause);
    }

    public static EventSendException resultUnknown(String message, Throwable cause) {
        return new EventSendException(EventSendFailureType.RESULT_UNKNOWN, message, cause);
    }

    public static EventSendException retryable(String message) {
        return retryable(message, null);
    }

    public static EventSendException nonRetryable(String message) {
        return nonRetryable(message, null);
    }

    public static EventSendException resultUnknown(String message) {
        return resultUnknown(message, null);
    }

    private EventSendException(
            EventSendFailureType failureType,
            String message,
            Throwable cause
    ) {
        super(message, cause);
        this.failureType = Objects.requireNonNull(failureType, "failureType must not be null");
    }

    public EventSendFailureType failureType() {
        return failureType;
    }
}
