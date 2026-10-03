package dev.reliableevent.jdbc.internal.model;

/** Immutable metrics view read from one database statement and one database timestamp. */
public record OutboxMetricsSnapshot(
        long backlog,
        long dead,
        long ready,
        double readyOldestAgeSeconds,
        long unfinishedOverdue,
        double unfinishedOldestAgeSeconds,
        long unfinishedTimestampMissing
) { }
