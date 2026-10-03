package dev.reliableevent.jdbc.internal.tracing;

import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.internal.publication.SendReceipt;

/** Best-effort tracing seam for one successfully claimed publication attempt. */
public interface PublicationTracer {
    PublicationTracer NOOP = claimedEvent -> new Attempt() {
        @Override public StoredEvent eventForSend() { return claimedEvent.event(); }
        @Override public void sendSucceeded(SendReceipt receipt) { }
        @Override public void sendFailed(EventSendFailureType type, Throwable failure) { }
        @Override public void stateUpdated(String targetStatus, boolean commitPending) { }
        @Override public void stateUpdateFailed(String targetStatus, Throwable failure, boolean ownershipRejected) { }
        @Override public void close() { }
    };

    Attempt begin(ClaimedEvent claimedEvent);

    interface Attempt extends AutoCloseable {
        StoredEvent eventForSend();
        void sendSucceeded(SendReceipt receipt);
        void sendFailed(EventSendFailureType type, Throwable failure);
        void stateUpdated(String targetStatus, boolean commitPending);
        void stateUpdateFailed(String targetStatus, Throwable failure, boolean ownershipRejected);
        @Override void close();
    }
}
