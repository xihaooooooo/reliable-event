package dev.reliableevent.jdbc;

/** One bounded cleanup pass. Age is for the oldest still eligible row after deletion. */
public record PublishedRetentionResult(int scanned, int deleted, long oldestEligibleAgeSeconds) { }
