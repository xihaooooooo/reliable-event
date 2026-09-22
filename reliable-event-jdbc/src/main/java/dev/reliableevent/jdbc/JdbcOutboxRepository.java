package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

final class JdbcOutboxRepository {

    private static final String INSERT_EVENT = """
            INSERT INTO reliable_event_outbox (
                event_type,
                event_key,
                payload,
                headers,
                status,
                next_attempt_at,
                max_attempts,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)
            """;

    private final JdbcTemplate jdbcTemplate;

    JdbcOutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    EventId insert(
            String eventType,
            String eventKey,
            String payloadJson,
            String headersJson,
            Instant availableAt,
            Instant createdAt,
            int maxAttempts
    ) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            var statement = connection.prepareStatement(INSERT_EVENT, Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, eventType);
            statement.setString(2, eventKey);
            statement.setString(3, payloadJson);
            statement.setString(4, headersJson);
            statement.setInt(5, EventStatus.PENDING.code());
            statement.setTimestamp(6, Timestamp.from(availableAt));
            statement.setInt(7, maxAttempts);
            statement.setTimestamp(8, Timestamp.from(createdAt));
            statement.setTimestamp(9, Timestamp.from(createdAt));
            return statement;
        }, keyHolder);

        Number generatedKey = keyHolder.getKey();
        if (generatedKey != null) {
            return new EventId(generatedKey.longValue());
        }

        Long existingId = jdbcTemplate.queryForObject(
                "SELECT id FROM reliable_event_outbox WHERE event_type = ? AND event_key = ?",
                Long.class,
                eventType,
                eventKey
        );
        if (existingId == null) {
            throw new IllegalStateException("Outbox insert completed without returning an event id");
        }
        return new EventId(existingId);
    }

    List<EventCandidate> findDueEventCandidates(Instant now, int limit) {
        return jdbcTemplate.query(
                """
                SELECT id, version
                FROM reliable_event_outbox
                WHERE status IN (?, ?) AND next_attempt_at <= ?
                  AND attempt_count < max_attempts
                ORDER BY next_attempt_at, id
                LIMIT ?
                """,
                (resultSet, rowNumber) -> new EventCandidate(
                        new EventId(resultSet.getLong("id")),
                        resultSet.getLong("version")
                ),
                EventStatus.PENDING.code(),
                EventStatus.RETRY_WAIT.code(),
                Timestamp.from(now),
                limit
        );
    }

    Optional<ClaimedEvent> claim(EventCandidate candidate, Instant now) {
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?,
                    attempt_count = attempt_count + 1,
                    version = version + 1,
                    updated_at = ?
                WHERE id = ?
                  AND status IN (?, ?)
                  AND next_attempt_at <= ?
                  AND attempt_count < max_attempts
                  AND version = ?
                """,
                EventStatus.PUBLISHING.code(),
                Timestamp.from(now),
                candidate.id().value(),
                EventStatus.PENDING.code(),
                EventStatus.RETRY_WAIT.code(),
                Timestamp.from(now),
                candidate.version()
        );
        if (updated != 1) {
            return Optional.empty();
        }
        return Optional.of(findClaimedEvent(candidate.id().value()));
    }

    private ClaimedEvent findClaimedEvent(long eventId) {
        return jdbcTemplate.queryForObject(
                """
                SELECT id,
                       event_type,
                       event_key,
                       payload,
                       headers,
                       version,
                       attempt_count,
                       max_attempts
                FROM reliable_event_outbox
                WHERE id = ?
                """,
                (resultSet, rowNumber) -> new ClaimedEvent(
                        new StoredEvent(
                                new EventId(resultSet.getLong("id")),
                                resultSet.getString("event_type"),
                                resultSet.getString("event_key"),
                                resultSet.getString("payload"),
                                resultSet.getString("headers")
                        ),
                        resultSet.getLong("version"),
                        resultSet.getInt("attempt_count"),
                        resultSet.getInt("max_attempts")
                ),
                eventId
        );
    }

    void markPublished(ClaimedEvent claimedEvent, Instant publishedAt) {
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?,
                    published_at = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                """,
                EventStatus.PUBLISHED.code(),
                Timestamp.from(publishedAt),
                Timestamp.from(publishedAt),
                claimedEvent.event().id().value(),
                EventStatus.PUBLISHING.code(),
                claimedEvent.claimVersion()
        );
        if (updated != 1) {
            throw new IllegalStateException(
                    "Event claim is no longer current: " + claimedEvent.event().id().value()
            );
        }
    }

    void markRetryWait(
            ClaimedEvent claimedEvent,
            Instant failedAt,
            Instant nextAttemptAt,
            String lastError
    ) {
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?,
                    next_attempt_at = ?,
                    last_error = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                """,
                EventStatus.RETRY_WAIT.code(),
                Timestamp.from(nextAttemptAt),
                lastError,
                Timestamp.from(failedAt),
                claimedEvent.event().id().value(),
                EventStatus.PUBLISHING.code(),
                claimedEvent.claimVersion()
        );
        if (updated != 1) {
            throw new IllegalStateException(
                    "Event claim is no longer current: " + claimedEvent.event().id().value()
            );
        }
    }

    void markDead(ClaimedEvent claimedEvent, Instant failedAt, String lastError) {
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?,
                    last_error = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                """,
                EventStatus.DEAD.code(),
                lastError,
                Timestamp.from(failedAt),
                claimedEvent.event().id().value(),
                EventStatus.PUBLISHING.code(),
                claimedEvent.claimVersion()
        );
        if (updated != 1) {
            throw new IllegalStateException(
                    "Event claim is no longer current: " + claimedEvent.event().id().value()
            );
        }
    }
}
