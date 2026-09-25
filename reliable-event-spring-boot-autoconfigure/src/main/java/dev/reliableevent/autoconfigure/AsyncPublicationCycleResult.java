package dev.reliableevent.autoconfigure;

public record AsyncPublicationCycleResult(int recoveredCount, int submittedCount) {

    public AsyncPublicationCycleResult {
        if (recoveredCount < 0 || submittedCount < 0) {
            throw new IllegalArgumentException("Cycle counts must not be negative");
        }
    }
}
