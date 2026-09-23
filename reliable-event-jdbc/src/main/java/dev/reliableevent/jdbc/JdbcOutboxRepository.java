package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
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

    List<ExpiredLeaseCandidate> findExpiredLeaseCandidates(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return jdbcTemplate.query(
                """
                SELECT id,
                       version,
                       lease_owner,
                       lease_until,
                       attempt_count,
                       max_attempts
                FROM reliable_event_outbox
                WHERE status = ?
                  AND lease_owner IS NOT NULL
                  AND TRIM(lease_owner) <> ''
                  AND lease_until <= UTC_TIMESTAMP(3)
                ORDER BY lease_until, id
                LIMIT ?
                """,
                (resultSet, rowNumber) -> new ExpiredLeaseCandidate(
                        new EventId(resultSet.getLong("id")),
                        resultSet.getLong("version"),
                        resultSet.getString("lease_owner"),
                        resultSet.getTimestamp("lease_until").toInstant(),
                        resultSet.getInt("attempt_count"),
                        resultSet.getInt("max_attempts")
                ),
                EventStatus.PUBLISHING.code(),
                limit
        );
    }

    Optional<ClaimedEvent> claim(
            EventCandidate candidate,
            Instant now,
            String leaseOwner,
            Duration leaseDuration
    ) {
        long leaseDurationMicros = Math.multiplyExact(leaseDuration.toMillis(), 1_000L);
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?,
                    attempt_count = attempt_count + 1,
                    lease_owner = ?,
                    lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)),
                    version = version + 1,
                    updated_at = ?
                WHERE id = ?
                  AND status IN (?, ?)
                  AND next_attempt_at <= ?
                  AND attempt_count < max_attempts
                  AND version = ?
                """,
                EventStatus.PUBLISHING.code(),
                leaseOwner,
                leaseDurationMicros,
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
                       max_attempts,
                       lease_owner,
                       lease_until
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
                        resultSet.getInt("max_attempts"),
                        resultSet.getString("lease_owner"),
                        resultSet.getTimestamp("lease_until").toInstant()
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
                    lease_owner = NULL,
                    lease_until = NULL,
                    updated_at = ?,
                    version = version + 1
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                  AND lease_owner = ?
                  AND lease_until > UTC_TIMESTAMP(3)
                """,
                EventStatus.PUBLISHED.code(),
                Timestamp.from(publishedAt),
                Timestamp.from(publishedAt),
                claimedEvent.event().id().value(),
                EventStatus.PUBLISHING.code(),
                claimedEvent.claimVersion(),
                claimedEvent.leaseOwner()
        );
        if (updated != 1) {
            throw staleClaim(claimedEvent);
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
                    lease_owner = NULL,
                    lease_until = NULL,
                    updated_at = ?,
                    version = version + 1
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                  AND lease_owner = ?
                  AND lease_until > UTC_TIMESTAMP(3)
                """,
                EventStatus.RETRY_WAIT.code(),
                Timestamp.from(nextAttemptAt),
                lastError,
                Timestamp.from(failedAt),
                claimedEvent.event().id().value(),
                EventStatus.PUBLISHING.code(),
                claimedEvent.claimVersion(),
                claimedEvent.leaseOwner()
        );
        if (updated != 1) {
            throw staleClaim(claimedEvent);
        }
    }

    void markDead(ClaimedEvent claimedEvent, Instant failedAt, String lastError) {
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?,
                    last_error = ?,
                    lease_owner = NULL,
                    lease_until = NULL,
                    updated_at = ?,
                    version = version + 1
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                  AND lease_owner = ?
                  AND lease_until > UTC_TIMESTAMP(3)
                """,
                EventStatus.DEAD.code(),
                lastError,
                Timestamp.from(failedAt),
                claimedEvent.event().id().value(),
                EventStatus.PUBLISHING.code(),
                claimedEvent.claimVersion(),
                claimedEvent.leaseOwner()
        );
        if (updated != 1) {
            throw staleClaim(claimedEvent);
        }
    }

    boolean recoverExpiredLeaseToRetryWait(
            ExpiredLeaseCandidate candidate,
            Duration delay,
            String lastError
    ) {
        long delayMicros = positiveDurationMicros(delay, "delay");
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?,
                    next_attempt_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)),
                    last_error = ?,
                    lease_owner = NULL,
                    lease_until = NULL,
                    updated_at = UTC_TIMESTAMP(3),
                    version = version + 1
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                  AND lease_owner = ?
                  AND lease_until = ?
                  AND lease_until <= UTC_TIMESTAMP(3)
                """,
                EventStatus.RETRY_WAIT.code(),
                delayMicros,
                Objects.requireNonNull(lastError, "lastError must not be null"),
                candidate.id().value(),
                EventStatus.PUBLISHING.code(),
                candidate.version(),
                candidate.leaseOwner(),
                Timestamp.from(candidate.leaseUntil())
        );
        return updated == 1;
    }

    boolean recoverExpiredLeaseToDead(
            ExpiredLeaseCandidate candidate,
            String lastError
    ) {
        int updated = jdbcTemplate.update(
                """
                UPDATE reliable_event_outbox
                SET status = ?,
                    last_error = ?,
                    lease_owner = NULL,
                    lease_until = NULL,
                    updated_at = UTC_TIMESTAMP(3),
                    version = version + 1
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                  AND lease_owner = ?
                  AND lease_until = ?
                  AND lease_until <= UTC_TIMESTAMP(3)
                """,
                EventStatus.DEAD.code(),
                Objects.requireNonNull(lastError, "lastError must not be null"),
                candidate.id().value(),
                EventStatus.PUBLISHING.code(),
                candidate.version(),
                candidate.leaseOwner(),
                Timestamp.from(candidate.leaseUntil())
        );
        return updated == 1;
    }

    private IllegalStateException staleClaim(ClaimedEvent claimedEvent) {
        return new IllegalStateException(
                "Event claim is no longer current: eventId="
                        + claimedEvent.event().id().value()
                        + ", leaseOwner="
                        + claimedEvent.leaseOwner()
        );
    }

    private long positiveDurationMicros(Duration duration, String name) {
        Objects.requireNonNull(duration, name + " must not be null");
        long millis = duration.toMillis();
        if (millis <= 0) {
            throw new IllegalArgumentException(name + " must be at least one millisecond");
        }
        return Math.multiplyExact(millis, 1_000L);
    }
}
