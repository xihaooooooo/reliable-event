package dev.reliableevent.jdbc.internal.tracing;

import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportReceipt;

/** Best-effort tracing seam for one successfully claimed publication attempt. */
public interface PublicationTracer {
    PublicationTracer NOOP = claimedEvent -> new Attempt() {
        @Override public StoredEvent eventForSend() { return claimedEvent.event(); }
        @Override public void sendSucceeded(TransportReceipt receipt) { }
        @Override public void sendFailed(TransportFailureType type, Throwable failure) { }
        @Override public void stateUpdated(String targetStatus, boolean commitPending) { }
        @Override public void stateUpdateFailed(String targetStatus, Throwable failure, boolean ownershipRejected) { }
        @Override public void close() { }
    };

    Attempt begin(ClaimedEvent claimedEvent);

    interface Attempt extends AutoCloseable {
        StoredEvent eventForSend();
        void sendSucceeded(TransportReceipt receipt);
        void sendFailed(TransportFailureType type, Throwable failure);
        void stateUpdated(String targetStatus, boolean commitPending);
        void stateUpdateFailed(String targetStatus, Throwable failure, boolean ownershipRejected);
        @Override void close();
    }
}
