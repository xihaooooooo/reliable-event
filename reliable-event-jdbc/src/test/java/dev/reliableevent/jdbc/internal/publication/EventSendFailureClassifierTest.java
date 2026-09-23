package dev.reliableevent.jdbc.internal.publication;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventSendFailureClassifierTest {

    @Test
    void classifiesExplicitSendFailures() {
        assertThat(EventSendFailureClassifier.classify(
                EventSendException.retryable("broker unavailable")
        )).isEqualTo(EventSendFailureType.RETRYABLE);

        assertThat(EventSendFailureClassifier.classify(
                EventSendException.nonRetryable("missing destination")
        )).isEqualTo(EventSendFailureType.NON_RETRYABLE);
    }

    @Test
    void treatsUnknownRuntimeExceptionsAsRetryableRegardlessOfMessage() {
        RuntimeException exception = new IllegalStateException("non-retryable permanent error");

        assertThat(EventSendFailureClassifier.classify(exception))
                .isEqualTo(EventSendFailureType.RETRYABLE);
    }

    @Test
    void typedSendFailurePreservesItsCause() {
        RuntimeException cause = new IllegalArgumentException("invalid topic");
        EventSendException exception = EventSendException.nonRetryable(
                "invalid destination",
                cause
        );

        assertThat(exception.getCause()).isSameAs(cause);
        assertThat(exception.failureType()).isEqualTo(EventSendFailureType.NON_RETRYABLE);
    }
}
