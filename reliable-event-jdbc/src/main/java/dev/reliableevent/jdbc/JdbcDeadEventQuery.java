package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Read-only dead-event queries. Lists omit event keys and errors; details include
 * those restricted fields. Authorization remains the caller's responsibility.
 */
public final class JdbcDeadEventQuery {

    public static final int MAX_PAGE_SIZE = 100;

    private static final String LIST_COLUMNS =
            "id, event_type, attempt_count, max_attempts, updated_at, version";

    private final JdbcTemplate jdbcTemplate;

    public JdbcDeadEventQuery(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    /** Distinguishes a missing event from one that exists but is not DEAD. */
    public DeadEventLookup lookup(EventId id) {
        Objects.requireNonNull(id, "id must not be null");
        return jdbcTemplate.query("""
                SELECT id, event_type, event_key, status, attempt_count,
                       max_attempts, created_at, updated_at, version, last_error
                FROM reliable_event_outbox
                WHERE id = ?
                """, resultSet -> {
            if (!resultSet.next()) {
                return new DeadEventLookup.NotFound(id);
            }
            if (resultSet.getInt("status") != EventStatus.DEAD.code()) {
                return new DeadEventLookup.NotDead(id);
            }
            return new DeadEventLookup.Dead(new DeadEventDetails(
                    id,
                    resultSet.getString("event_type"),
                    resultSet.getString("event_key"),
                    resultSet.getInt("attempt_count"),
                    resultSet.getInt("max_attempts"),
                    timestamp(resultSet, "created_at"),
                    timestamp(resultSet, "updated_at"),
                    resultSet.getLong("version"),
                    resultSet.getString("last_error")
            ));
        }, id.value());
    }

    /** Returns the newest event IDs first, without taking a database snapshot. */
    public DeadEventPage firstPage(int pageSize) {
        return page(null, pageSize);
    }

    /** Continues strictly below the cursor returned by the previous page. */
    public DeadEventPage nextPage(EventId beforeExclusive, int pageSize) {
        return page(Objects.requireNonNull(beforeExclusive, "beforeExclusive must not be null"), pageSize);
    }

    private DeadEventPage page(EventId beforeExclusive, int pageSize) {
        if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize must be between 1 and " + MAX_PAGE_SIZE);
        }
        String sql = "SELECT " + LIST_COLUMNS + " FROM reliable_event_outbox "
                + "WHERE status = ? "
                + (beforeExclusive == null ? "" : "AND id < ? ")
                + "ORDER BY id DESC LIMIT ?";
        List<DeadEventSummary> fetched = beforeExclusive == null
                ? jdbcTemplate.query(sql, JdbcDeadEventQuery::summary,
                        EventStatus.DEAD.code(), pageSize + 1)
                : jdbcTemplate.query(sql, JdbcDeadEventQuery::summary,
                        EventStatus.DEAD.code(), beforeExclusive.value(), pageSize + 1);
        boolean hasMore = fetched.size() > pageSize;
        List<DeadEventSummary> events = hasMore ? fetched.subList(0, pageSize) : fetched;
        Optional<EventId> nextCursor = hasMore
                ? Optional.of(events.get(events.size() - 1).id()) : Optional.empty();
        return new DeadEventPage(events, nextCursor);
    }

    private static DeadEventSummary summary(ResultSet resultSet, int rowNumber) throws SQLException {
        return new DeadEventSummary(
                new EventId(resultSet.getLong("id")),
                resultSet.getString("event_type"),
                resultSet.getInt("attempt_count"),
                resultSet.getInt("max_attempts"),
                timestamp(resultSet, "updated_at"),
                resultSet.getLong("version")
        );
    }

    private static Instant timestamp(ResultSet resultSet, String column) throws SQLException {
        return resultSet.getTimestamp(column).toInstant();
    }
}
