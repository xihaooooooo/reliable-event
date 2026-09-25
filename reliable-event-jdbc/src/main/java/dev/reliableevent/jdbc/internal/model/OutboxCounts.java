package dev.reliableevent.jdbc.internal.model;

public record OutboxCounts(long backlog, long dead) { }
