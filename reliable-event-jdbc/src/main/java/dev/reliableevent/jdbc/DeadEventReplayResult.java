package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;

import java.time.Instant;

/** A rejected request never changes the event or creates an audit row. */
public sealed interface DeadEventReplayResult {

    record Replayed(EventId id, long previousVersion, long newVersion,
                    long auditId, Instant replayedAt) implements DeadEventReplayResult { }

    record NotFound(EventId id) implements DeadEventReplayResult { }

    record NotDead(EventId id) implements DeadEventReplayResult { }

    record VersionMismatch(EventId id, long currentVersion) implements DeadEventReplayResult { }
}
