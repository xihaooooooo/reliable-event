package dev.reliableevent.jdbc.internal.persistence;

/** Typed ownership rejection while preserving the repository's IllegalStateException contract. */
public final class StaleEventClaimException extends IllegalStateException {
    public StaleEventClaimException(long eventId, String leaseOwner) {
        super("Event claim is no longer current: eventId=" + eventId + ", leaseOwner=" + leaseOwner);
    }
}
