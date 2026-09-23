package dev.reliableevent.jdbc.internal.cycle;

public record PublicationCycleResult(int recoveredCount, int publishedCount) {

    public PublicationCycleResult {
        if (recoveredCount < 0) {
            throw new IllegalArgumentException("recoveredCount must not be negative");
        }
        if (publishedCount < 0) {
            throw new IllegalArgumentException("publishedCount must not be negative");
        }
    }
}
