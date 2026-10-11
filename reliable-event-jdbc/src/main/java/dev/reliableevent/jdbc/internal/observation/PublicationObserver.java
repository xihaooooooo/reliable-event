package dev.reliableevent.jdbc.internal.observation;

import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.PublishedRetentionResult;

import java.time.Instant;

/** Optional, best-effort observation boundary for the JDBC publication protocol. */
public interface PublicationObserver {

    PublicationObserver NOOP = new PublicationObserver() { };

    default void sendSucceeded(ClaimedEvent event, TransportReceipt receipt, long elapsedNanos, Instant at) { }

    default void sendFailed(ClaimedEvent event, TransportFailureType type, long elapsedNanos) { }

    default void leaseRecovered(ExpiredLeaseCandidate candidate, boolean dead) { }

    default void refreshSnapshot() { }

    /** Automatic publication-cycle seam; default preserves existing observer callbacks. */
    default void refreshSnapshotAutomatically() { refreshSnapshot(); }

    default void schedulerState(boolean enabled, boolean running) { }

    default void schedulerCycleCompleted(boolean automatic, long completedAtEpochSeconds) { }

    default void schedulerCycleFailed(boolean automatic, String stage) { }

    default void workerCapacity(int inflight, int queued) { }

    default void workerStarted() { }

    default void workerFinished() { }

    default void candidateQueued() { }

    default void candidateDequeued() { }

    default void stateUpdated(String status, boolean commitPending) { }

    default void stateUpdateFailed(String status, boolean ownershipRejected) { }

    /** Register a metric increment to happen only after the state transition commits. */
    default void stateTransitionCommitted(String status) { }

    default void cleanupCompleted(PublishedRetentionResult result, long elapsedNanos) { }

    default void cleanupFailed() { }
}
