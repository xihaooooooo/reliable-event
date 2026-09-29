package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

/**
 * Application-facing operations for inspecting and explicitly requeuing DEAD events.
 * The calling application must authorize access and verify external delivery facts.
 */
public interface DeadEventOperations {

    DeadEventPage firstPage(int pageSize);

    DeadEventPage nextPage(EventId beforeExclusive, int pageSize);

    DeadEventLookup lookup(EventId id);

    DeadEventReplayResult replay(DeadEventReplayRequest request);
}
