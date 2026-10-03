package dev.reliableevent.internal.publication;

import java.util.Map;
import java.util.Objects;

public record SendReceipt(String messageId, Map<String, String> metadata) {

    public SendReceipt(String messageId) {
        this(messageId, Map.of());
    }

    public SendReceipt {
        if (messageId != null && messageId.isBlank()) {
            throw new IllegalArgumentException("messageId must be null or non-blank");
        }
        metadata = Map.copyOf(Objects.requireNonNull(metadata, "metadata must not be null"));
    }
}
