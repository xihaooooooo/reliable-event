package dev.reliableevent.jdbc.internal.publication;

import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSendFailureType;

import java.util.Objects;

final class EventSendFailureClassifier {

    private EventSendFailureClassifier() {
    }

    static EventSendFailureType classify(RuntimeException exception) {
        Objects.requireNonNull(exception, "exception must not be null");
        if (exception instanceof EventSendException sendException) {
            return sendException.failureType();
        }
        return EventSendFailureType.RETRYABLE;
    }
}
