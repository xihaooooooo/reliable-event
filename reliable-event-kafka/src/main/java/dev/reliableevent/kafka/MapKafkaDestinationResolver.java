package dev.reliableevent.kafka;

import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** Resolves event types to explicitly provisioned Kafka topics. */
public final class MapKafkaDestinationResolver implements KafkaDestinationResolver {
    private static final Pattern TOPIC = Pattern.compile("[a-zA-Z0-9._-]{1,249}");
    private final Map<String, String> mappings;

    public MapKafkaDestinationResolver(Map<String, String> mappings) {
        this.mappings = Map.copyOf(Objects.requireNonNull(mappings, "mappings must not be null"));
    }

    @Override
    public String resolve(String eventType) {
        String topic = mappings.get(eventType);
        if (topic == null || topic.isBlank()) {
            throw new TransportException(TransportFailureType.NON_RETRYABLE,
                    "No Kafka topic mapping is configured for the event type");
        }
        if (!isValidTopic(topic)) {
            throw new TransportException(TransportFailureType.NON_RETRYABLE,
                    "Kafka topic mapping is invalid");
        }
        return topic;
    }

    static boolean isValidTopic(String topic) {
        return topic != null && TOPIC.matcher(topic).matches() && !topic.equals(".") && !topic.equals("..");
    }
}
