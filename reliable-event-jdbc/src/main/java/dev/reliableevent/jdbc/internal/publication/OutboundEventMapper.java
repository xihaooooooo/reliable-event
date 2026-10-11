package dev.reliableevent.jdbc.internal.publication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.headers.EventHeaderConstraints;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;

import java.util.LinkedHashMap;
import java.util.Map;

/** Reads persisted headers before handing an immutable event to the transport. */
final class OutboundEventMapper {
    private static final ObjectMapper JSON = new ObjectMapper();

    private OutboundEventMapper() { }

    static OutboundEvent map(StoredEvent event) {
        if (event.id() == null || event.eventType() == null || event.eventType().isBlank()
                || event.eventKey() == null || event.eventKey().isBlank()
                || event.payloadJson() == null || event.payloadJson().isBlank()) {
            throw new TransportException(TransportFailureType.NON_RETRYABLE, "Stored event fields are invalid");
        }
        Map<String, String> headers = readHeaders(event.headersJson());
        String validationError = EventHeaderConstraints.validationError(headers);
        if (validationError != null) {
            throw new TransportException(TransportFailureType.NON_RETRYABLE, validationError);
        }
        return new OutboundEvent(event.id(), event.eventType(), event.eventKey(), event.payloadJson(), headers);
    }

    private static Map<String, String> readHeaders(String json) {
        try {
            JsonNode root = JSON.readTree(json);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("headers are not an object");
            }
            Map<String, String> headers = new LinkedHashMap<>();
            root.fields().forEachRemaining(entry -> {
                if (!entry.getValue().isTextual()) {
                    throw new IllegalArgumentException("header values must be strings");
                }
                headers.put(entry.getKey(), entry.getValue().textValue());
            });
            return Map.copyOf(headers);
        } catch (JsonProcessingException | IllegalArgumentException malformed) {
            // Parser exceptions can contain the raw header JSON. Keep it out of the failure.
            throw new TransportException(TransportFailureType.NON_RETRYABLE, "Stored event headers are invalid");
        }
    }
}
