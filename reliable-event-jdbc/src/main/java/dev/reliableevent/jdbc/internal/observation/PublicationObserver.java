package dev.reliableevent.jdbc.internal.observation;

import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;

import java.time.Instant;

/** Optional, best-effort observation boundary for the JDBC publication protocol. */
public interface PublicationObserver {

    PublicationObserver NOOP = new PublicationObserver() { };

    default void sendSucceeded(ClaimedEvent event, SendReceipt receipt, long elapsedNanos, Instant at) { }

    default void sendFailed(ClaimedEvent event, EventSendFailureType type, long elapsedNanos) { }

    default void leaseRecovered(ExpiredLeaseCandidate candidate, boolean dead) { }

    default void refreshSnapshot() { }
}
