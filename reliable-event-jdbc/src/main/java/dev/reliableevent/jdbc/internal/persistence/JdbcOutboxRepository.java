package dev.reliableevent.jdbc.internal.persistence;

import dev.reliableevent.EventId;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import dev.reliableevent.jdbc.internal.model.ExpiredLeaseCandidate;
import dev.reliableevent.jdbc.internal.model.OutboxCounts;
import dev.reliableevent.jdbc.internal.model.OutboxMetricsSnapshot;
import dev.reliableevent.internal.model.StoredEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class JdbcOutboxRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcOutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public EventId insert(
            String eventType,
            String eventKey,
            String payloadJson,
            String headersJson,
            Instant availableAt,
            Instant createdAt,
            int maxAttempts
    ) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        try {
            jdbcTemplate.update(connection -> {
                var statement = connection.prepareStatement(
                        JdbcOutboxSql.INSERT_IDENTITY, Statement.RETURN_GENERATED_KEYS);
                statement.setString(1, eventType);
                statement.setString(2, eventKey);
                statement.setTimestamp(3, Timestamp.from(createdAt));
                return statement;
            }, keyHolder);
        } catch (DuplicateKeyException duplicate) {
            // The generated primary key is never supplied; the only possible duplicate
            // for this insert is the unique business identity. A locking read sees a
            // concurrent insert after its owner commits, even under REPEATABLE READ.
            Long existingId = jdbcTemplate.queryForObject(
                    JdbcOutboxSql.FIND_IDENTITY_FOR_UPDATE, Long.class, eventType, eventKey);
            if (existingId == null) {
                throw new IllegalStateException("Duplicate identity has no existing id", duplicate);
            }
            return new EventId(existingId);
        }
        Number generatedKey = keyHolder.getKey();
        if (generatedKey == null) {
            throw new IllegalStateException("Identity insert completed without returning an event id");
        }
        EventId id = new EventId(generatedKey.longValue());
        jdbcTemplate.update(JdbcOutboxSql.INSERT_EVENT,
                id.value(), eventType, eventKey, payloadJson, headersJson,
                EventStatus.PENDING.code(), Timestamp.from(availableAt),
                Timestamp.from(availableAt), maxAttempts,
                Timestamp.from(createdAt), Timestamp.from(createdAt));
        return id;
    }

    public List<EventCandidate> findDueEventCandidates(Instant now, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return jdbcTemplate.query(
                JdbcOutboxSql.FIND_DUE_EVENT_CANDIDATES,
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

    public OutboxCounts countStatuses() {
        return jdbcTemplate.query(
                JdbcOutboxSql.COUNT_STATUSES,
                resultSet -> {
                    long backlog = 0;
                    long dead = 0;
                    while (resultSet.next()) {
                        if (resultSet.getInt("status") == EventStatus.DEAD.code()) {
                            dead += resultSet.getLong("total");
                        } else {
                            backlog += resultSet.getLong("total");
                        }
                    }
                    return new OutboxCounts(backlog, dead);
                },
                EventStatus.PENDING.code(), EventStatus.RETRY_WAIT.code(), EventStatus.DEAD.code()
        );
    }

    /** Reads all publication-age fields using one MySQL UTC timestamp and one statement. */
    public OutboxMetricsSnapshot readMetricsSnapshot(int queryTimeoutSeconds) {
        if (queryTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("queryTimeoutSeconds must be positive");
        }
        try (Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource(), "DataSource unavailable")
                .getConnection();
             PreparedStatement statement = connection.prepareStatement(JdbcOutboxSql.READ_METRICS_SNAPSHOT)) {
            statement.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Metrics snapshot query returned no row");
                }
                long ready = result.getLong("ready");
                long unfinished = result.getLong("unfinished_overdue");
                return new OutboxMetricsSnapshot(
                        result.getLong("backlog"), result.getLong("dead"), ready,
                        age(result, "ready_oldest_age", ready), unfinished,
                        age(result, "unfinished_oldest_age", unfinished),
                        result.getLong("unfinished_timestamp_missing"));
            }
        } catch (SQLException exception) {
            throw new org.springframework.jdbc.CannotGetJdbcConnectionException(
                    "Could not read reliable-event metrics snapshot", exception);
        }
    }

    private static double age(ResultSet result, String column, long count) throws SQLException {
        if (count == 0) {
            return 0.0;
        }
        double age = result.getDouble(column);
        return result.wasNull() ? Double.NaN : Math.max(0.0, age);
    }

    public List<ExpiredLeaseCandidate> findExpiredLeaseCandidates(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return jdbcTemplate.query(
                JdbcOutboxSql.FIND_EXPIRED_LEASE_CANDIDATES,
                (resultSet, rowNumber) -> new ExpiredLeaseCandidate(
                        new EventId(resultSet.getLong("id")),
                        resultSet.getLong("version"),
                        resultSet.getString("lease_owner"),
                        resultSet.getTimestamp("lease_until").toInstant(),
                        resultSet.getInt("attempt_count"),
                        resultSet.getInt("max_attempts"),
                        resultSet.getString("event_type"),
                        resultSet.getString("event_key")
                ),
                EventStatus.PUBLISHING.code(),
                limit
        );
    }

    public Optional<ClaimedEvent> claim(
            EventCandidate candidate,
            Instant now,
            String leaseOwner,
            Duration leaseDuration
    ) {
        long leaseDurationMicros = Math.multiplyExact(leaseDuration.toMillis(), 1_000L);
        int updated = jdbcTemplate.update(
                JdbcOutboxSql.CLAIM_EVENT,
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
                JdbcOutboxSql.FIND_CLAIMED_EVENT,
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
                        resultSet.getTimestamp("lease_until").toInstant(),
                        resultSet.getTimestamp("first_available_at") == null
                                ? null : resultSet.getTimestamp("first_available_at").toInstant()
                ),
                eventId
        );
    }

    public void markPublished(ClaimedEvent claimedEvent, Instant publishedAt) {
        // MySQL DATETIME has no zone. Bind a UTC wall time regardless of the JVM zone.
        int updated = jdbcTemplate.update(
                JdbcOutboxSql.MARK_PUBLISHED,
                EventStatus.PUBLISHED.code(),
                LocalDateTime.ofInstant(publishedAt, ZoneOffset.UTC),
                LocalDateTime.ofInstant(publishedAt, ZoneOffset.UTC),
                claimedEvent.event().id().value(),
                EventStatus.PUBLISHING.code(),
                claimedEvent.claimVersion(),
                claimedEvent.leaseOwner()
        );
        if (updated != 1) {
            throw staleClaim(claimedEvent);
        }
    }

    public void markRetryWait(
            ClaimedEvent claimedEvent,
            Instant failedAt,
            Instant nextAttemptAt,
            String lastError
    ) {
        int updated = jdbcTemplate.update(
                JdbcOutboxSql.MARK_RETRY_WAIT,
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

    public void markDead(ClaimedEvent claimedEvent, Instant failedAt, String lastError) {
        int updated = jdbcTemplate.update(
                JdbcOutboxSql.MARK_DEAD,
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

    public boolean recoverExpiredLeaseToRetryWait(
            ExpiredLeaseCandidate candidate,
            Duration delay,
            String lastError
    ) {
        long delayMicros = positiveDurationMicros(delay, "delay");
        int updated = jdbcTemplate.update(
                JdbcOutboxSql.RECOVER_EXPIRED_LEASE_TO_RETRY_WAIT,
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

    public boolean recoverExpiredLeaseToDead(
            ExpiredLeaseCandidate candidate,
            String lastError
    ) {
        int updated = jdbcTemplate.update(
                JdbcOutboxSql.RECOVER_EXPIRED_LEASE_TO_DEAD,
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
        return new StaleEventClaimException(
                claimedEvent.event().id().value(), claimedEvent.leaseOwner());
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
