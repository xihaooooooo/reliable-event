package dev.reliableevent.spi;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * A confirmed send. {@code brokerMessageId} is empty when the Broker supplies no ID;
 * an Outbox event ID, producer-assigned ID, delivery tag, or offset must not be substituted.
 * Metadata is diagnostic only and limited to topic, partition, offset, exchange,
 * routing_key, and delivery_tag keys, with at most six entries and 256 UTF-8 bytes per value.
 * Metadata is never a metric label and must not contain credentials or payload data.
 */
public record TransportReceipt(Optional<String> brokerMessageId, Map<String, String> metadata) {
    private static final Set<String> ALLOWED_METADATA_KEYS = Set.of(
            "topic", "partition", "offset", "exchange", "routing_key", "delivery_tag");
    private static final int MAX_METADATA_VALUE_BYTES = 256;

    public TransportReceipt {
        brokerMessageId = Objects.requireNonNull(brokerMessageId, "brokerMessageId must not be null");
        brokerMessageId = brokerMessageId.map(value -> {
            if (value.isBlank()) throw new IllegalArgumentException("brokerMessageId must not be blank");
            return value;
        });
        metadata = Map.copyOf(Objects.requireNonNull(metadata, "metadata must not be null"));
        if (metadata.size() > ALLOWED_METADATA_KEYS.size()) {
            throw new IllegalArgumentException("metadata contains too many entries");
        }
        metadata.forEach((key, value) -> {
            if (!ALLOWED_METADATA_KEYS.contains(key)) {
                throw new IllegalArgumentException("unsupported transport metadata key");
            }
            if (value == null || value.getBytes(StandardCharsets.UTF_8).length > MAX_METADATA_VALUE_BYTES) {
                throw new IllegalArgumentException("transport metadata value exceeds its limit");
            }
        });
    }

    public static TransportReceipt confirmed() { return new TransportReceipt(Optional.empty(), Map.of()); }
}
