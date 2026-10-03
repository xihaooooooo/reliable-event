package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.headers.EventHeaderConstraints;
import dev.reliableevent.internal.publication.EventSendException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

final class RocketMqMessageFactory {

    static final String EVENT_ID_PROPERTY = "reliable_event_id";
    static final String EVENT_TYPE_PROPERTY = "reliable_event_type";
    static final String EVENT_KEY_PROPERTY = "reliable_event_key";
    static final String RESERVED_PROPERTY_PREFIX = EventHeaderConstraints.RESERVED_PROPERTY_PREFIX;
    static final int DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024;

    private final ClientServiceProvider provider;
    private final ObjectMapper objectMapper;
    private final int maxBodyBytes;

    RocketMqMessageFactory(
            ClientServiceProvider provider,
            ObjectMapper objectMapper,
            int maxBodyBytes
    ) {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        if (maxBodyBytes <= 0) {
            throw new IllegalArgumentException("maxBodyBytes must be positive");
        }
        this.maxBodyBytes = maxBodyBytes;
    }

    Message create(StoredEvent event, RocketMqDestination destination) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(destination, "destination must not be null");
        requireNonBlank(event.eventType(), "eventType");
        requireNonBlank(event.eventKey(), "eventKey");
        requireNonBlank(event.payloadJson(), "payloadJson");

        byte[] body = event.payloadJson().getBytes(StandardCharsets.UTF_8);
        if (body.length > maxBodyBytes) {
            throw EventSendException.nonRetryable(
                    "RocketMQ message body has " + body.length
                            + " bytes and exceeds the configured limit of " + maxBodyBytes + " bytes"
            );
        }

        MessageBuilder builder;
        try {
            builder = provider.newMessageBuilder()
                    .setTopic(destination.topic())
                    .setKeys(event.eventKey())
                    .setBody(body);
            if (destination.tag() != null) {
                builder.setTag(destination.tag());
            }
            parseHeaders(event.headersJson()).forEach(builder::addProperty);
            builder.addProperty(EVENT_ID_PROPERTY, Long.toString(event.id().value()));
            builder.addProperty(EVENT_TYPE_PROPERTY, event.eventType());
            builder.addProperty(EVENT_KEY_PROPERTY, event.eventKey());
            return builder.build();
        } catch (EventSendException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw EventSendException.nonRetryable(
                    "Unable to build a valid RocketMQ message for event type "
                            + safeEventType(event.eventType()),
                    exception
            );
        }
    }

    private Map<String, String> parseHeaders(String headersJson) {
        requireNonBlank(headersJson, "headersJson");
        final JsonNode root;
        try {
            root = objectMapper.readTree(headersJson);
        } catch (JsonProcessingException exception) {
            throw EventSendException.nonRetryable(
                    "Reliable event headers must be a JSON object containing only string values",
                    exception
            );
        }
        if (root == null || !root.isObject()) {
            throw EventSendException.nonRetryable(
                    "Reliable event headers must be a JSON object containing only string values"
            );
        }
        Map<String, String> headers = new LinkedHashMap<>();
        root.fields().forEachRemaining(entry -> {
            if (!entry.getValue().isTextual()) {
                throw EventSendException.nonRetryable(
                        "Reliable event header " + safeHeaderName(entry.getKey())
                                + " must contain a string value"
                );
            }
            headers.put(entry.getKey(), entry.getValue().textValue());
        });
        String validationError = EventHeaderConstraints.validationError(headers);
        if (validationError != null) {
            throw EventSendException.nonRetryable(validationError);
        }
        return Map.copyOf(headers);
    }

    private void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw EventSendException.nonRetryable(name + " must not be blank");
        }
    }

    private String safeHeaderName(String key) {
        if (key == null) {
            return "<null>";
        }
        String truncated = key.length() <= EventHeaderConstraints.MAX_HEADER_KEY_LENGTH
                ? key
                : key.substring(0, EventHeaderConstraints.MAX_HEADER_KEY_LENGTH);
        StringBuilder safe = new StringBuilder(truncated.length());
        for (int index = 0; index < truncated.length(); index++) {
            char character = truncated.charAt(index);
            boolean allowed = character >= 'A' && character <= 'Z'
                    || character >= 'a' && character <= 'z'
                    || character >= '0' && character <= '9'
                    || character == '_'
                    || character == '.'
                    || character == '-';
            safe.append(allowed ? character : '?');
        }
        return safe.toString();
    }

    private String safeEventType(String eventType) {
        StringBuilder safe = new StringBuilder();
        eventType.codePoints().limit(128).forEach(codePoint -> {
            if (Character.isISOControl(codePoint)) {
                safe.append('?');
            } else {
                safe.appendCodePoint(codePoint);
            }
        });
        return safe.toString();
    }
}
