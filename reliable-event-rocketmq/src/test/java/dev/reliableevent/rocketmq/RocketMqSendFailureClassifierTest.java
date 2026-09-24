package dev.reliableevent.rocketmq;

import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSendFailureType;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.ProxyTimeoutException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RocketMqSendFailureClassifierTest {

    private final RocketMqSendFailureClassifier classifier = new RocketMqSendFailureClassifier();

    @Test
    void classifiesDeterministicRejectionsAsNonRetryable() {
        ClientException cause = new BadRequestException(400, "request-1", "invalid message secret");

        EventSendException result = classifier.classify(cause);

        assertThat(result.failureType()).isEqualTo(EventSendFailureType.NON_RETRYABLE);
        assertThat(result).hasCause(cause)
                .hasMessageContaining("BadRequestException")
                .hasMessageNotContaining("secret");
    }

    @Test
    void classifiesThrottlingAsRetryable() {
        EventSendException result = classifier.classify(
                new TooManyRequestsException(429, "request-2", "slow down")
        );

        assertThat(result.failureType()).isEqualTo(EventSendFailureType.RETRYABLE);
    }

    @Test
    void classifiesTimeoutsAndUnknownClientErrorsAsResultUnknown() {
        EventSendException timeout = classifier.classify(
                new ProxyTimeoutException(504, "request-3", "deadline")
        );
        ClientException unknownCause = new ClientException("unexpected secret");
        EventSendException unknown = classifier.classify(unknownCause);

        assertThat(timeout.failureType()).isEqualTo(EventSendFailureType.RESULT_UNKNOWN);
        assertThat(unknown.failureType()).isEqualTo(EventSendFailureType.RESULT_UNKNOWN);
        assertThat(unknown).hasCause(unknownCause)
                .hasMessageNotContaining("secret");
    }
}
