package dev.reliableevent.internal.publication;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventSendExceptionTest {

    @Test
    void exposesEveryFailureTypeAndPreservesTheCause() {
        RuntimeException cause = new RuntimeException("transport failed");

        assertThat(EventSendException.retryable("retry", cause).failureType())
                .isEqualTo(EventSendFailureType.RETRYABLE);
        assertThat(EventSendException.nonRetryable("dead", cause).failureType())
                .isEqualTo(EventSendFailureType.NON_RETRYABLE);
        EventSendException unknown = EventSendException.resultUnknown("unknown", cause);

        assertThat(unknown.failureType()).isEqualTo(EventSendFailureType.RESULT_UNKNOWN);
        assertThat(unknown).hasCause(cause);
    }
}
