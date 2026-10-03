package dev.reliableevent.kafka;

@FunctionalInterface
public interface KafkaDestinationResolver {
    String resolve(String eventType);
}
