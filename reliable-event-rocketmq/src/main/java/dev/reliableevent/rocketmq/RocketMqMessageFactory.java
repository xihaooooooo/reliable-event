package dev.reliableevent.rocketmq;

import dev.reliableevent.internal.headers.EventHeaderConstraints;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class RocketMqMessageFactory {

    static final String EVENT_ID_PROPERTY = "reliable_event_id";
    static final String EVENT_TYPE_PROPERTY = "reliable_event_type";
    static final String EVENT_KEY_PROPERTY = "reliable_event_key";
    static final String RESERVED_PROPERTY_PREFIX = EventHeaderConstraints.RESERVED_PROPERTY_PREFIX;
    static final int DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024;

    private final ClientServiceProvider provider;
    private final int maxBodyBytes;

    RocketMqMessageFactory(ClientServiceProvider provider, int maxBodyBytes) {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
        if (maxBodyBytes <= 0) {
            throw new IllegalArgumentException("maxBodyBytes must be positive");
        }
        this.maxBodyBytes = maxBodyBytes;
    }

    Message create(OutboundEvent event, RocketMqDestination destination) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(destination, "destination must not be null");
        requireNonBlank(event.eventType(), "eventType");
        requireNonBlank(event.eventKey(), "eventKey");
        requireNonBlank(event.payloadJson(), "payloadJson");

        byte[] body = event.payloadJson().getBytes(StandardCharsets.UTF_8);
        if (body.length > maxBodyBytes) {
            throw nonRetryable("RocketMQ message body has " + body.length
                    + " bytes and exceeds the configured limit of " + maxBodyBytes + " bytes");
        }
        String headerError;
        try {
            headerError = EventHeaderConstraints.validationError(event.headers());
        } catch (RuntimeException invalidHeaders) {
            throw nonRetryable("Reliable event headers must contain only string values", invalidHeaders);
        }
        if (headerError != null) {
            throw nonRetryable(headerError);
        }

        try {
            MessageBuilder builder = provider.newMessageBuilder()
                    .setTopic(destination.topic())
                    .setKeys(event.eventKey())
                    .setBody(body);
            if (destination.tag() != null) {
                builder.setTag(destination.tag());
            }
            event.headers().forEach(builder::addProperty);
            builder.addProperty(EVENT_ID_PROPERTY, Long.toString(event.id().value()));
            builder.addProperty(EVENT_TYPE_PROPERTY, event.eventType());
            builder.addProperty(EVENT_KEY_PROPERTY, event.eventKey());
            return builder.build();
        } catch (TransportException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw nonRetryable("Unable to build a valid RocketMQ message for event type "
                    + safeEventType(event.eventType()), exception);
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw nonRetryable(name + " must not be blank");
        }
    }

    private static TransportException nonRetryable(String message) {
        return nonRetryable(message, null);
    }

    private static TransportException nonRetryable(String message, Throwable cause) {
        return new TransportException(TransportFailureType.NON_RETRYABLE, message, cause);
    }

    private static String safeEventType(String eventType) {
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
