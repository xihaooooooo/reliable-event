package dev.reliableevent.example;

import dev.reliableevent.EventId;
import dev.reliableevent.jdbc.DeadEventDetails;
import dev.reliableevent.jdbc.DeadEventLookup;
import dev.reliableevent.jdbc.DeadEventOperations;
import dev.reliableevent.jdbc.DeadEventPage;
import dev.reliableevent.jdbc.DeadEventReplayRequest;
import dev.reliableevent.jdbc.DeadEventReplayResult;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Java-only integration example. The caller must authorize the operator, inspect
 * Broker and consumer facts, and fix the failure before requesting a replay.
 */
@Component
@ConditionalOnProperty(prefix = "reliable-event", name = "dead-operations-enabled", havingValue = "true")
public final class DeadEventManualProcedure {

    private final DeadEventOperations operations;

    public DeadEventManualProcedure(DeadEventOperations operations) {
        this.operations = operations;
    }

    public DeadEventPage list(int pageSize) {
        return operations.firstPage(pageSize);
    }

    public DeadEventLookup inspect(EventId id) {
        return operations.lookup(id);
    }

    /** Call only after the external checks described in the operations guide. */
    public DeadEventReplayResult replayAfterVerification(DeadEventDetails inspected,
                                                          String authenticatedOperator, String reason) {
        return operations.replay(new DeadEventReplayRequest(
                inspected.id(), inspected.version(), authenticatedOperator, reason));
    }
}
