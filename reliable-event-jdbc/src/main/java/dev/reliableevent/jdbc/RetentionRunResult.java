package dev.reliableevent.jdbc;

/** Results for one Outbox and identity retention pass. */
public record RetentionRunResult(PublishedRetentionResult published, int identitiesDeleted) { }
