package dev.reliableevent.rocketmq;

import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportReceipt;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.Producer;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** RocketMQ implementation of the framework's public synchronous transport contract. */
public final class RocketMqEventTransport implements EventTransport {

    private final Producer producer;
    private final EventDestinationResolver destinationResolver;
    private final RocketMqMessageFactory messageFactory;
    private final RocketMqSendFailureClassifier failureClassifier;

    public RocketMqEventTransport(
            ClientServiceProvider provider,
            Producer producer,
            EventDestinationResolver destinationResolver) {
        this(provider, producer, destinationResolver, RocketMqMessageFactory.DEFAULT_MAX_BODY_BYTES);
    }

    public RocketMqEventTransport(
            ClientServiceProvider provider,
            Producer producer,
            EventDestinationResolver destinationResolver,
            int maxBodyBytes) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
        this.destinationResolver = Objects.requireNonNull(
                destinationResolver, "destinationResolver must not be null");
        this.messageFactory = new RocketMqMessageFactory(provider, maxBodyBytes);
        this.failureClassifier = new RocketMqSendFailureClassifier();
    }

    @Override
    public TransportReceipt send(OutboundEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        RocketMqDestination destination = destinationResolver.resolve(event.eventType());
        Message message = messageFactory.create(event, destination);
        try {
            org.apache.rocketmq.client.apis.producer.SendReceipt brokerReceipt = producer.send(message);
            return toTransportReceipt(brokerReceipt);
        } catch (TransportException failure) {
            throw failure;
        } catch (ClientException failure) {
            throw failureClassifier.classify(failure);
        } catch (RuntimeException failure) {
            throw new TransportException(
                    TransportFailureType.RESULT_UNKNOWN,
                    "RocketMQ send result is unknown after an unexpected client failure",
                    failure);
        }
    }

    private static TransportReceipt toTransportReceipt(
            org.apache.rocketmq.client.apis.producer.SendReceipt brokerReceipt) {
        if (brokerReceipt == null) {
            throw new TransportException(
                    TransportFailureType.RESULT_UNKNOWN,
                    "RocketMQ send returned without a receipt");
        }
        MessageId messageId = brokerReceipt.getMessageId();
        if (messageId == null || messageId.toString().isBlank()) {
            return TransportReceipt.confirmed();
        }
        return new TransportReceipt(Optional.of(messageId.toString()), Map.of());
    }
}
