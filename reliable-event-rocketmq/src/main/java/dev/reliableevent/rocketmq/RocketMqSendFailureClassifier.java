package dev.reliableevent.rocketmq;

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
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;

import java.util.Objects;

final class RocketMqSendFailureClassifier {

    TransportException classify(ClientException exception) {
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
            return new TransportException(TransportFailureType.NON_RETRYABLE,
                    "RocketMQ rejected the message with " + type,
                    exception
            );
        }
        if (exception instanceof TooManyRequestsException) {
            return new TransportException(TransportFailureType.RETRYABLE,
                    "RocketMQ temporarily rejected the message with " + type,
                    exception
            );
        }
        if (exception instanceof ProxyTimeoutException) {
            return new TransportException(TransportFailureType.RESULT_UNKNOWN,
                    "RocketMQ send result is unknown after " + type,
                    exception
            );
        }
        return new TransportException(TransportFailureType.RESULT_UNKNOWN,
                "RocketMQ send result is unknown after " + type,
                exception
        );
    }
}
