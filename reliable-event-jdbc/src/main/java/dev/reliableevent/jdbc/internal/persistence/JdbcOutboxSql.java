package dev.reliableevent.jdbc.internal.persistence;

/** SQL statements used by {@link JdbcOutboxRepository}. */
final class JdbcOutboxSql {

    static final String INSERT_IDENTITY = """
            INSERT INTO reliable_event_identity (event_type, event_key, registered_at)
            VALUES (?, ?, ?)
            """;

    static final String INSERT_EVENT = """
            INSERT INTO reliable_event_outbox (
                id,
                event_type,
                event_key,
                payload,
                headers,
                status,
                next_attempt_at,
                first_available_at,
                max_attempts,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    static final String FIND_IDENTITY_FOR_UPDATE = """
            SELECT id FROM reliable_event_identity
            WHERE event_type = ? AND event_key = ? FOR UPDATE
            """;

    static final String FIND_DUE_EVENT_CANDIDATES = """
            SELECT id, version
            FROM reliable_event_outbox
            WHERE status IN (?, ?) AND next_attempt_at <= ?
              AND attempt_count < max_attempts
            ORDER BY next_attempt_at, id
            LIMIT ?
            """;

    static final String COUNT_STATUSES = """
            SELECT status, COUNT(*) AS total
            FROM reliable_event_outbox
            WHERE status IN (?, ?, ?)
            GROUP BY status
            """;

    static final String READ_METRICS_SNAPSHOT = """
            SELECT
              COALESCE(SUM(status IN (0, 3)), 0) AS backlog,
              COALESCE(SUM(status = 4), 0) AS failed,
              COALESCE(SUM(status IN (0, 3) AND next_attempt_at <= UTC_TIMESTAMP(3)), 0) AS ready,
              CASE
                WHEN SUM(status IN (0, 3) AND next_attempt_at <= UTC_TIMESTAMP(3)) = 0 THEN 0
                ELSE TIMESTAMPDIFF(MICROSECOND,
                  MIN(CASE WHEN status IN (0, 3) AND next_attempt_at <= UTC_TIMESTAMP(3)
                           THEN first_available_at END), UTC_TIMESTAMP(3)) / 1000000.0
              END AS ready_oldest_age,
              COALESCE(SUM(status IN (0, 1, 3, 6) AND first_available_at <= UTC_TIMESTAMP(3)), 0)
                AS unfinished_overdue,
              CASE
                WHEN SUM(status IN (0, 1, 3, 6) AND first_available_at <= UTC_TIMESTAMP(3)) = 0 THEN 0
                ELSE TIMESTAMPDIFF(MICROSECOND,
                  MIN(CASE WHEN status IN (0, 1, 3, 6) AND first_available_at <= UTC_TIMESTAMP(3)
                           THEN first_available_at END), UTC_TIMESTAMP(3)) / 1000000.0
              END AS unfinished_oldest_age,
              COALESCE(SUM(status IN (0, 1, 3, 6) AND first_available_at IS NULL), 0)
                AS unfinished_timestamp_missing
            FROM reliable_event_outbox
            """;

    static final String FIND_EXPIRED_LEASE_CANDIDATES = """
            SELECT id,
                   version,
                   event_type,
                   event_key,
                   payload,
                   headers,
                   status,
                   last_error,
                   fallback_stop_reason,
                   fallback_failure_classification,
                   lease_owner,
                   lease_until,
                   attempt_count,
                   max_attempts
            FROM reliable_event_outbox
            WHERE status IN (?, ?)
              AND lease_owner IS NOT NULL
              AND TRIM(lease_owner) <> ''
              AND lease_until <= UTC_TIMESTAMP(3)
            ORDER BY lease_until, id
            LIMIT ?
            """;

    static final String CLAIM_EVENT = """
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
            """;

    static final String FIND_CLAIMED_EVENT = """
            SELECT id,
                   event_type,
                   event_key,
                   payload,
                   headers,
                   version,
                   attempt_count,
                   max_attempts,
                   lease_owner,
                   lease_until,
                   first_available_at
            FROM reliable_event_outbox
            WHERE id = ?
            """;

    static final String MARK_PUBLISHED = """
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
            """;

    static final String MARK_RETRY_WAIT = """
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
            """;

    static final String BEGIN_FALLBACK = """
            UPDATE reliable_event_outbox
            SET status = 6, last_error = ?, fallback_stop_reason = ?,
                fallback_failure_classification = ?, lease_owner = ?,
                lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)),
                updated_at = UTC_TIMESTAMP(3), version = version + 1
            WHERE id = ? AND status = 1 AND version = ? AND lease_owner = ?
              AND lease_until > UTC_TIMESTAMP(3)
            """;

    static final String FINISH_FALLBACK = """
            UPDATE reliable_event_outbox
            SET status = ?, last_error = COALESCE(?, last_error),
                lease_owner = NULL, lease_until = NULL,
                updated_at = UTC_TIMESTAMP(3), version = version + 1
            WHERE id = ? AND status = 6 AND version = ? AND lease_owner = ?
              AND lease_until > UTC_TIMESTAMP(3)
            """;

    static final String CLAIM_EXPIRED_FALLBACK = """
            UPDATE reliable_event_outbox
            SET lease_owner = ?, lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)),
                updated_at = UTC_TIMESTAMP(3), version = version + 1
            WHERE id = ? AND status = 6 AND version = ? AND lease_owner = ?
              AND lease_until = ? AND lease_until <= UTC_TIMESTAMP(3)
            """;

    static final String RECOVER_EXPIRED_LEASE_TO_FALLBACK = """
            UPDATE reliable_event_outbox
            SET status = 6, last_error = ?, fallback_stop_reason = ?,
                fallback_failure_classification = ?, lease_owner = ?,
                lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)),
                updated_at = UTC_TIMESTAMP(3), version = version + 1
            WHERE id = ? AND status = 1 AND version = ? AND lease_owner = ?
              AND lease_until = ? AND lease_until <= UTC_TIMESTAMP(3)
            """;

    static final String RECOVER_EXPIRED_LEASE_TO_RETRY_WAIT = """
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
            """;

    private JdbcOutboxSql() {
    }
}
