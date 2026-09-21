package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

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

    List<Long> findDueEventIds(Instant now, int limit) {
        return jdbcTemplate.query(
                """
                SELECT id
                FROM reliable_event_outbox
                WHERE status = ? AND next_attempt_at <= ?
                ORDER BY next_attempt_at, id
                LIMIT ?
                """,
                (resultSet, rowNumber) -> resultSet.getLong("id"),
                EventStatus.PENDING.code(),
                Timestamp.from(now),
                limit
        );
    }

    boolean markPublishing(long eventId, Instant now) {
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                EventStatus.PUBLISHING.code(),
                Timestamp.from(now),
                eventId,
                EventStatus.PENDING.code()
        );
        return updated == 1;
    }

    StoredEvent findById(long eventId) {
        return jdbcTemplate.queryForObject(
                """
                SELECT id, event_type, event_key, payload, headers
                FROM reliable_event_outbox
                WHERE id = ?
                """,
                (resultSet, rowNumber) -> new StoredEvent(
                        new EventId(resultSet.getLong("id")),
                        resultSet.getString("event_type"),
                        resultSet.getString("event_key"),
                        resultSet.getString("payload"),
                        resultSet.getString("headers")
                ),
                eventId
        );
    }

    void markPublished(long eventId, Instant publishedAt) {
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?, published_at = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                EventStatus.PUBLISHED.code(),
                Timestamp.from(publishedAt),
                Timestamp.from(publishedAt),
                eventId,
                EventStatus.PUBLISHING.code()
        );
        if (updated != 1) {
            throw new IllegalStateException("Event was not in PUBLISHING state: " + eventId);
        }
    }
}
