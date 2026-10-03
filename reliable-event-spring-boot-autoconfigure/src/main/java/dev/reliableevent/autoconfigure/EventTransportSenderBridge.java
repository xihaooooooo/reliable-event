package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.headers.EventHeaderConstraints;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportReceipt;

import java.util.LinkedHashMap;
import java.util.Map;

/** Adapts the supported transport SPI to the existing publication worker seam. */
final class EventTransportSenderBridge implements EventSender {
    private final EventTransport transport;
    private final ObjectMapper objectMapper;

    EventTransportSenderBridge(EventTransport transport, ObjectMapper objectMapper) {
        this.transport = transport;
        this.objectMapper = objectMapper;
    }

    @Override
    public SendReceipt send(StoredEvent event) {
        Map<String, String> headers = readHeaders(event.headersJson());
        String validationError = EventHeaderConstraints.validationError(headers);
        if (validationError != null) {
            throw EventSendException.nonRetryable(validationError);
        }
        TransportReceipt receipt;
        try {
            receipt = transport.send(new OutboundEvent(event.id(), event.eventType(), event.eventKey(),
                    event.payloadJson(), headers));
        } catch (TransportException failure) {
            throw mapFailure(failure);
        } catch (RuntimeException unexpected) {
            throw EventSendException.resultUnknown("Transport failed with an unclassified exception", unexpected);
        }
        if (receipt == null) {
            throw EventSendException.resultUnknown("Transport returned without a receipt");
        }
        return new SendReceipt(receipt.brokerMessageId().orElse(null), receipt.metadata());
    }

    private Map<String, String> readHeaders(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root == null || !root.isObject()) throw new IllegalArgumentException("headers are not an object");
            Map<String, String> headers = new LinkedHashMap<>();
            root.fields().forEachRemaining(entry -> {
                if (!entry.getValue().isTextual()) {
                    throw new IllegalArgumentException("header values must be strings");
                }
                headers.put(entry.getKey(), entry.getValue().textValue());
            });
            return Map.copyOf(headers);
        } catch (JsonProcessingException | IllegalArgumentException malformed) {
            throw EventSendException.nonRetryable("Stored event headers are invalid", malformed);
        }
    }

    private static EventSendException mapFailure(TransportException failure) {
        return switch (failure.failureType()) {
            case RETRYABLE -> EventSendException.retryable("Transport reported a retryable failure", failure);
            case NON_RETRYABLE -> EventSendException.nonRetryable("Transport reported a non-retryable failure", failure);
            case RESULT_UNKNOWN -> EventSendException.resultUnknown("Transport reported an unknown send result", failure);
        };
    }
}
