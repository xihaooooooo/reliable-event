package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportReceipt;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.producer.Producer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Compatibility wrapper for applications that directly use the legacy EventSender API. */
public final class RocketMqEventSender implements EventSender {

    private final ObjectMapper objectMapper;
    private final RocketMqEventTransport transport;

    public RocketMqEventSender(
            ClientServiceProvider provider,
            Producer producer,
            EventDestinationResolver destinationResolver,
            ObjectMapper objectMapper) {
        this(provider, producer, destinationResolver, objectMapper, RocketMqMessageFactory.DEFAULT_MAX_BODY_BYTES);
    }

    public RocketMqEventSender(
            ClientServiceProvider provider,
            Producer producer,
            EventDestinationResolver destinationResolver,
            ObjectMapper objectMapper,
            int maxBodyBytes) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.transport = new RocketMqEventTransport(provider, producer, destinationResolver, maxBodyBytes);
    }

    @Override
    public SendReceipt send(StoredEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        validateLegacyEvent(event);
        TransportReceipt receipt;
        try {
            receipt = transport.send(new OutboundEvent(
                    event.id(),
                    event.eventType(),
                    event.eventKey(),
                    event.payloadJson(),
                    readHeaders(event.headersJson())));
        } catch (TransportException failure) {
            throw toLegacyFailure(failure);
        }
        return new SendReceipt(receipt.brokerMessageId().orElse(null), receipt.metadata());
    }

    private Map<String, String> readHeaders(String json) {
        if (json == null || json.isBlank()) {
            throw EventSendException.nonRetryable("headersJson must not be blank");
        }
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root == null || !root.isObject()) {
                throw EventSendException.nonRetryable(
                        "Reliable event headers must be a JSON object containing only string values");
            }
            Map<String, String> headers = new LinkedHashMap<>();
            root.fields().forEachRemaining(entry -> {
                if (!entry.getValue().isTextual()) {
                    throw EventSendException.nonRetryable(
                            "Reliable event header values must contain only strings");
                }
                headers.put(entry.getKey(), entry.getValue().textValue());
            });
            return Map.copyOf(headers);
        } catch (JsonProcessingException failure) {
            throw EventSendException.nonRetryable(
                    "Reliable event headers must be valid JSON", failure);
        }
    }

    private static void validateLegacyEvent(StoredEvent event) {
        if (event.id() == null) {
            throw EventSendException.nonRetryable("eventId must not be null");
        }
        if (event.eventType() == null || event.eventType().isBlank()) {
            throw EventSendException.nonRetryable("eventType must not be blank");
        }
        if (event.eventKey() == null || event.eventKey().isBlank()) {
            throw EventSendException.nonRetryable("eventKey must not be blank");
        }
        if (event.payloadJson() == null || event.payloadJson().isBlank()) {
            throw EventSendException.nonRetryable("payloadJson must not be blank");
        }
        if (event.headersJson() == null || event.headersJson().isBlank()) {
            throw EventSendException.nonRetryable("headersJson must not be blank");
        }
    }

    private static EventSendException toLegacyFailure(TransportException failure) {
        Throwable cause = failure.getCause();
        return switch (failure.failureType()) {
            case RETRYABLE -> EventSendException.retryable(failure.getMessage(), cause);
            case NON_RETRYABLE -> EventSendException.nonRetryable(failure.getMessage(), cause);
            case RESULT_UNKNOWN -> EventSendException.resultUnknown(failure.getMessage(), cause);
        };
    }
}
