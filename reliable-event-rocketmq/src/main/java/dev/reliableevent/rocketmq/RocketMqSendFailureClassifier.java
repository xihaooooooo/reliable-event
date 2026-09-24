package dev.reliableevent.rocketmq;

import dev.reliableevent.internal.publication.EventSendException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.ForbiddenException;
import org.apache.rocketmq.client.java.exception.LiteSubscriptionQuotaExceededException;
import org.apache.rocketmq.client.java.exception.LiteTopicQuotaExceededException;
import org.apache.rocketmq.client.java.exception.NotFoundException;
import org.apache.rocketmq.client.java.exception.PayloadEmptyException;
import org.apache.rocketmq.client.java.exception.PayloadTooLargeException;
import org.apache.rocketmq.client.java.exception.PaymentRequiredException;
import org.apache.rocketmq.client.java.exception.ProxyTimeoutException;
import org.apache.rocketmq.client.java.exception.RequestHeaderFieldsTooLargeException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.exception.UnauthorizedException;
import org.apache.rocketmq.client.java.exception.UnsupportedException;

import java.util.Objects;

final class RocketMqSendFailureClassifier {

    EventSendException classify(ClientException exception) {
        Objects.requireNonNull(exception, "exception must not be null");
        String type = exception.getClass().getSimpleName();

        if (exception instanceof BadRequestException
                || exception instanceof ForbiddenException
                || exception instanceof LiteSubscriptionQuotaExceededException
                || exception instanceof LiteTopicQuotaExceededException
                || exception instanceof NotFoundException
                || exception instanceof PayloadEmptyException
                || exception instanceof PayloadTooLargeException
                || exception instanceof PaymentRequiredException
                || exception instanceof RequestHeaderFieldsTooLargeException
                || exception instanceof UnauthorizedException
                || exception instanceof UnsupportedException) {
            return EventSendException.nonRetryable(
                    "RocketMQ rejected the message with " + type,
                    exception
            );
        }
        if (exception instanceof TooManyRequestsException) {
            return EventSendException.retryable(
                    "RocketMQ temporarily rejected the message with " + type,
                    exception
            );
        }
        if (exception instanceof ProxyTimeoutException) {
            return EventSendException.resultUnknown(
                    "RocketMQ send result is unknown after " + type,
                    exception
            );
        }
        return EventSendException.resultUnknown(
                "RocketMQ send result is unknown after " + type,
                exception
        );
    }
}
