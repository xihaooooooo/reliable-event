package dev.reliableevent.internal.publication;

public record SendReceipt(String messageId) {

    public SendReceipt {
        if (messageId == null || messageId.isBlank()) {
            throw new IllegalArgumentException("messageId must not be blank");
        }
    }
}
