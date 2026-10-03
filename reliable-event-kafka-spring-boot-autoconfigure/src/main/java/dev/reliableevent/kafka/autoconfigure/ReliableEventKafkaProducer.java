package dev.reliableevent.kafka.autoconfigure;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;

/** Marker type proving this client was created and is owned by ReliableEvent. */
final class ReliableEventKafkaProducer extends KafkaProducer<String, byte[]> {
    ReliableEventKafkaProducer(Map<String, Object> configuration) {
        super(configuration);
    }
}
