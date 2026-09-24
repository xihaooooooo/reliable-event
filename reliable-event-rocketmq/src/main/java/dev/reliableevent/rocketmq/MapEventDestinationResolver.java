package dev.reliableevent.rocketmq;

import dev.reliableevent.internal.publication.EventSendException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class MapEventDestinationResolver implements EventDestinationResolver {

    private final Map<String, RocketMqDestination> destinations;

    public MapEventDestinationResolver(Map<String, RocketMqDestination> destinations) {
        Objects.requireNonNull(destinations, "destinations must not be null");
        Map<String, RocketMqDestination> validated = new LinkedHashMap<>();
        destinations.forEach((eventType, destination) -> {
            if (eventType == null || eventType.isBlank()) {
                throw new IllegalArgumentException("event type mapping key must not be blank");
            }
            validated.put(eventType, Objects.requireNonNull(
                    destination,
                    "destination for event type " + eventType + " must not be null"
            ));
        });
        this.destinations = Map.copyOf(validated);
    }

    @Override
    public RocketMqDestination resolve(String eventType) {
        if (eventType == null || eventType.isBlank()) {
            throw EventSendException.nonRetryable("Reliable event type must not be blank");
        }
        RocketMqDestination destination = destinations.get(eventType);
        if (destination == null) {
            throw EventSendException.nonRetryable(
                    "No RocketMQ destination is configured for event type "
                            + safeEventType(eventType)
            );
        }
        return destination;
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
