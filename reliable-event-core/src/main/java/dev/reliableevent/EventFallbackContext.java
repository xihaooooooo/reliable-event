package dev.reliableevent;

import java.util.Objects;

/** Immutable, public information about a terminal publication failure. */
public record EventFallbackContext(FailedEvent event, int failureCount,
                                   StopReason stopReason, FailureClassification failureClassification,
                                   String errorSummary) {
    public EventFallbackContext {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(stopReason, "stopReason must not be null");
        Objects.requireNonNull(failureClassification, "failureClassification must not be null");
        Objects.requireNonNull(errorSummary, "errorSummary must not be null");
        if (failureCount < 0) throw new IllegalArgumentException("failureCount must not be negative");
    }
    public enum StopReason { NON_RETRYABLE, ATTEMPTS_EXHAUSTED, LEASE_EXPIRED }
    public enum FailureClassification { RETRYABLE, NON_RETRYABLE, RESULT_UNKNOWN }

    @Override public String toString() {
        return "EventFallbackContext[event=" + event + ", failureCount=" + failureCount
                + ", stopReason=" + stopReason + ", failureClassification=" + failureClassification + "]";
    }
}
