package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.SendReceipt;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.Producer;

import java.util.Objects;

public final class RocketMqEventSender implements EventSender {

    private final Producer producer;
    private final EventDestinationResolver destinationResolver;
    private final RocketMqMessageFactory messageFactory;
    private final RocketMqSendFailureClassifier failureClassifier;

    public RocketMqEventSender(
            ClientServiceProvider provider,
            Producer producer,
            EventDestinationResolver destinationResolver,
            ObjectMapper objectMapper
    ) {
        this(
                provider,
                producer,
                destinationResolver,
                objectMapper,
                RocketMqMessageFactory.DEFAULT_MAX_BODY_BYTES
        );
    }

    public RocketMqEventSender(
            ClientServiceProvider provider,
            Producer producer,
            EventDestinationResolver destinationResolver,
            ObjectMapper objectMapper,
            int maxBodyBytes
    ) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
        this.destinationResolver = Objects.requireNonNull(
                destinationResolver,
                "destinationResolver must not be null"
        );
        this.messageFactory = new RocketMqMessageFactory(provider, objectMapper, maxBodyBytes);
        this.failureClassifier = new RocketMqSendFailureClassifier();
    }

    @Override
    public SendReceipt send(StoredEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        RocketMqDestination destination = destinationResolver.resolve(event.eventType());
        Message message = messageFactory.create(event, destination);

        try {
            org.apache.rocketmq.client.apis.producer.SendReceipt brokerReceipt =
                    producer.send(message);
            return toInternalReceipt(brokerReceipt);
        } catch (EventSendException exception) {
            throw exception;
        } catch (ClientException exception) {
            throw failureClassifier.classify(exception);
        } catch (RuntimeException exception) {
            throw EventSendException.resultUnknown(
                    "RocketMQ send result is unknown after an unexpected client failure",
                    exception
            );
        }
    }

    private SendReceipt toInternalReceipt(
            org.apache.rocketmq.client.apis.producer.SendReceipt brokerReceipt
    ) {
        if (brokerReceipt == null) {
            throw EventSendException.resultUnknown(
                    "RocketMQ send returned without a receipt"
            );
        }
        MessageId messageId = brokerReceipt.getMessageId();
        if (messageId == null || messageId.toString().isBlank()) {
            throw EventSendException.resultUnknown(
                    "RocketMQ send returned without a message id"
            );
        }
        return new SendReceipt(messageId.toString());
    }
}
