package dev.reliableevent.kafka.example;

public record OrderCreatedPayload(long orderId, String itemCode, int quantity) { }
