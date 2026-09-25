package dev.reliableevent.example;

public record OrderCreatedPayload(long orderId, String itemCode, int quantity) { }
