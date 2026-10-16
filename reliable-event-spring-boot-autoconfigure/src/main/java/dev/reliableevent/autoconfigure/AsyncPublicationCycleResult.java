package dev.reliableevent.autoconfigure;

/**
 * Dispatch counts. Scheduler scans do not execute lease recovery and return zero for
 * {@code recoveredCount}; recovery is observed independently through lease recovery metrics.
 */
public record AsyncPublicationCycleResult(int recoveredCount, int submittedCount) {

    public AsyncPublicationCycleResult {
        if (recoveredCount < 0 || submittedCount < 0) {
            throw new IllegalArgumentException("Cycle counts must not be negative");
        }
    }
}
