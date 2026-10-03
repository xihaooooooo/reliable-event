package dev.reliableevent.spi;

import java.util.Objects;

/**
 * A classified transport failure. Use {@link TransportFailureType#RETRYABLE} only when
 * the failure is known to be recoverable, {@code NON_RETRYABLE} for a definite permanent
 * rejection, and {@code RESULT_UNKNOWN} when acceptance may have occurred without a
 * reliable confirmation. Do not include credentials, payloads, or sensitive metadata in
 * the message or cause.
 */
public final class TransportException extends RuntimeException {
    private final TransportFailureType failureType;

    public TransportException(TransportFailureType failureType, String message) { this(failureType, message, null); }
    public TransportException(TransportFailureType failureType, String message, Throwable cause) {
        super(message, cause);
        this.failureType = Objects.requireNonNull(failureType, "failureType must not be null");
    }
    public TransportFailureType failureType() { return failureType; }
}
