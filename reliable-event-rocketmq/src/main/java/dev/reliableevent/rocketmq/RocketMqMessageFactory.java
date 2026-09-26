package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

final class RocketMqMessageFactory {

    static final String EVENT_ID_PROPERTY = "reliable_event_id";
    static final String EVENT_TYPE_PROPERTY = "reliable_event_type";
    static final String EVENT_KEY_PROPERTY = "reliable_event_key";
    static final String RESERVED_PROPERTY_PREFIX = "reliable_event_";
    static final int DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024;

    private static final int MAX_HEADER_COUNT = 64;
    private static final int MAX_HEADER_KEY_LENGTH = 128;
    private static final int MAX_HEADER_VALUE_BYTES = 4 * 1024;
    private static final int MAX_TOTAL_HEADER_BYTES = 16 * 1024;
    private static final Pattern HEADER_KEY_PATTERN = Pattern.compile("[A-Za-z0-9_.-]+");
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
        if (headers.size() > MAX_HEADER_COUNT) {
            throw EventSendException.nonRetryable(
                    "Reliable event has " + headers.size()
                            + " headers and exceeds the limit of " + MAX_HEADER_COUNT
            );
        }

        int totalBytes = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            validateHeaderKey(key);
            if (value == null || value.isBlank()) {
                throw EventSendException.nonRetryable(
                        "Reliable event header " + safeHeaderName(key) + " must have a non-blank value"
                );
            }
            int valueBytes = value.getBytes(StandardCharsets.UTF_8).length;
            if (valueBytes > MAX_HEADER_VALUE_BYTES) {
                throw EventSendException.nonRetryable(
                        "Reliable event header " + safeHeaderName(key)
                                + " exceeds the value limit of " + MAX_HEADER_VALUE_BYTES + " bytes"
                );
            }
            totalBytes = Math.addExact(
                    totalBytes,
                    key.getBytes(StandardCharsets.UTF_8).length + valueBytes
            );
            if (totalBytes > MAX_TOTAL_HEADER_BYTES) {
                throw EventSendException.nonRetryable(
                        "Reliable event headers exceed the total limit of "
                                + MAX_TOTAL_HEADER_BYTES + " bytes"
                );
            }
        }
        return Map.copyOf(headers);
    }

    private void validateHeaderKey(String key) {
        if (key == null
                || key.isBlank()
                || key.length() > MAX_HEADER_KEY_LENGTH
                || !HEADER_KEY_PATTERN.matcher(key).matches()) {
            throw EventSendException.nonRetryable(
                    "Reliable event contains an invalid header name " + safeHeaderName(key)
            );
        }
        if (key.startsWith(RESERVED_PROPERTY_PREFIX)) {
            throw EventSendException.nonRetryable(
                    "Reliable event header " + safeHeaderName(key) + " uses a reserved prefix"
            );
        }
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
        String truncated = key.length() <= MAX_HEADER_KEY_LENGTH
                ? key
                : key.substring(0, MAX_HEADER_KEY_LENGTH);
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
