package dev.reliableevent.jdbc;

import dev.reliableevent.jdbc.internal.model.EventStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/** Requeues one DEAD event and writes its success audit in the same database transaction. */
public final class JdbcDeadEventReplay {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    public JdbcDeadEventReplay(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc must not be null");
        this.transaction = new TransactionTemplate(Objects.requireNonNull(
                transactionManager, "transactionManager must not be null"));
    }

    public DeadEventReplayResult replay(DeadEventReplayRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        return Objects.requireNonNull(transaction.execute(status -> replayInTransaction(request)));
    }

    private DeadEventReplayResult replayInTransaction(DeadEventReplayRequest request) {
        var rows = jdbc.query("""
                SELECT status, version, attempt_count, max_attempts, last_error
                FROM reliable_event_outbox
                WHERE id = ?
                FOR UPDATE
                """, (resultSet, rowNumber) -> new PreviousState(
                resultSet.getInt("status"),
                resultSet.getLong("version"),
                resultSet.getInt("attempt_count"),
                resultSet.getInt("max_attempts"),
                resultSet.getString("last_error")
        ), request.id().value());
        if (rows.isEmpty()) {
            return new DeadEventReplayResult.NotFound(request.id());
        }
        PreviousState previous = rows.get(0);
        if (previous.status() != EventStatus.DEAD.code()) {
            return new DeadEventReplayResult.NotDead(request.id());
        }
        if (previous.version() != request.expectedVersion()) {
            return new DeadEventReplayResult.VersionMismatch(request.id(), previous.version());
        }
        long newVersion = Math.addExact(previous.version(), 1);
        LocalDateTime replayedAt = Objects.requireNonNull(jdbc.queryForObject(
                "SELECT UTC_TIMESTAMP(3)", LocalDateTime.class));

        int updated = jdbc.update("""
                UPDATE reliable_event_outbox
                SET status = ?, attempt_count = 0, next_attempt_at = ?,
                    lease_owner = NULL, lease_until = NULL, last_error = NULL,
                    published_at = NULL, updated_at = ?, version = version + 1
                WHERE id = ? AND status = ? AND version = ?
                """, EventStatus.PENDING.code(), replayedAt, replayedAt,
                request.id().value(), EventStatus.DEAD.code(), previous.version());
        if (updated != 1) {
            throw new IllegalStateException("Dead event changed while replaying");
        }

        KeyHolder key = new GeneratedKeyHolder();
        jdbc.update(connection -> {
            var statement = connection.prepareStatement("""
                    INSERT INTO reliable_event_replay_audit (
                        event_id, previous_version, new_version,
                        previous_attempt_count, previous_max_attempts, previous_last_error,
                        operator_id, reason, replayed_at, result
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, request.id().value());
            statement.setLong(2, previous.version());
            statement.setLong(3, newVersion);
            statement.setInt(4, previous.attemptCount());
            statement.setInt(5, previous.maxAttempts());
            statement.setString(6, previous.lastError());
            statement.setString(7, request.operator());
            statement.setString(8, request.reason());
            statement.setObject(9, replayedAt);
            statement.setString(10, "REQUEUED");
            return statement;
        }, key);
        Number auditId = key.getKey();
        if (auditId == null) {
            throw new IllegalStateException("Replay audit insert returned no id");
        }
        return new DeadEventReplayResult.Replayed(
                request.id(), previous.version(), newVersion,
                auditId.longValue(), replayedAt.toInstant(ZoneOffset.UTC));
    }

    private record PreviousState(int status, long version, int attemptCount,
                                 int maxAttempts, String lastError) { }
}
