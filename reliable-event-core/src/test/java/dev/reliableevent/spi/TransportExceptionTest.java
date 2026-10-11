package dev.reliableevent.spi;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransportExceptionTest {
    @Test
    void preservesEveryClassificationAndCauseAndRejectsMissingClassification() {
        RuntimeException cause = new RuntimeException("transport failed");
        for (TransportFailureType type : TransportFailureType.values()) {
            var failure = new TransportException(type, "classified", cause);
            assertThat(failure.failureType()).isEqualTo(type);
            assertThat(failure).hasCause(cause);
        }
        assertThatThrownBy(() -> new TransportException(null, "invalid"))
                .isInstanceOf(NullPointerException.class);
    }
}
