package dev.reliableevent.jdbc;

import dev.reliableevent.jdbc.internal.model.EventStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Deletes bounded batches of old PUBLISHED rows and, optionally, expired identities. */
public final class JdbcPublishedEventRetention {

    public static final int MAX_BATCH_SIZE = 1_000;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    public JdbcPublishedEventRetention(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc must not be null");
        this.transaction = new TransactionTemplate(Objects.requireNonNull(manager, "manager must not be null"));
    }

    public PublishedRetentionResult runOnce(Duration retention, int batchSize) {
        return runOnceInternal(retention, batchSize, null).published();
    }

    /** Cleans Outbox rows and identities in one transaction, counting them separately. */
    public RetentionRunResult runOnceWithIdentityRetention(
            Duration outboxRetention,
            Duration identityRetention,
            int batchSize) {
        Objects.requireNonNull(identityRetention, "identityRetention must not be null");
        validateRetention(identityRetention, "identityRetention");
        return runOnceInternal(outboxRetention, batchSize, identityRetention);
    }

    private RetentionRunResult runOnceInternal(Duration retention, int batchSize,
                                               Duration identityRetention) {
        Objects.requireNonNull(retention, "retention must not be null");
        validateRetention(retention, "retention");
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("batchSize must be between 1 and " + MAX_BATCH_SIZE);
        }
        return Objects.requireNonNull(transaction.execute(status -> {
            PublishedRetentionResult published = clean(retention, batchSize, identityRetention != null);
            int identitiesDeleted = identityRetention == null ? 0 : cleanIdentities(identityRetention, batchSize);
            return new RetentionRunResult(published, identitiesDeleted);
        }));
    }

    private static void validateRetention(Duration retention, String name) {
        if (retention.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be at least one millisecond");
        }
    }

    private PublishedRetentionResult clean(Duration retention, int batchSize,
                                           boolean captureIdentityPublicationTime) {
        LocalDateTime now = Objects.requireNonNull(
                jdbc.queryForObject("SELECT UTC_TIMESTAMP(3)", LocalDateTime.class));
        LocalDateTime cutoff = now.minus(retention);
        List<Long> ids = jdbc.queryForList("""
                SELECT id FROM reliable_event_outbox
                WHERE status = ? AND published_at < ?
                ORDER BY published_at, id LIMIT ?
                """, Long.class, EventStatus.PUBLISHED.code(), cutoff, batchSize);

        int deleted = 0;
        if (!ids.isEmpty()) {
            String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
            if (captureIdentityPublicationTime) {
                List<Object> captureArgs = new ArrayList<>(ids.size() + 2);
                captureArgs.add(EventStatus.PUBLISHED.code());
                captureArgs.add(cutoff);
                captureArgs.addAll(ids);
                jdbc.update("""
                        UPDATE reliable_event_identity i
                        INNER JOIN reliable_event_outbox o
                            ON i.id = o.id AND i.event_type = o.event_type AND i.event_key = o.event_key
                        SET i.published_at = o.published_at
                        WHERE o.status = ? AND o.published_at < ? AND o.id IN (
                        """ + placeholders + ")", captureArgs.toArray());
            }
            String sql = """
                    DELETE o FROM reliable_event_outbox o
                    INNER JOIN reliable_event_identity i
                        ON i.id = o.id AND i.event_type = o.event_type AND i.event_key = o.event_key
                    WHERE o.status = ? AND o.published_at < ? AND o.id IN (
                    """ + placeholders + ")";
            List<Object> args = new ArrayList<>(ids.size() + 2);
            args.add(EventStatus.PUBLISHED.code());
            args.add(cutoff);
            args.addAll(ids);
            deleted = jdbc.update(sql, args.toArray());
        }
        Long oldestAge = jdbc.queryForObject("""
                SELECT TIMESTAMPDIFF(SECOND, MIN(published_at), UTC_TIMESTAMP(3))
                FROM reliable_event_outbox
                WHERE status = ? AND published_at < ?
                """, Long.class, EventStatus.PUBLISHED.code(), cutoff);
        return new PublishedRetentionResult(ids.size(), deleted,
                oldestAge == null ? 0 : Math.max(0, oldestAge));
    }

    private int cleanIdentities(Duration retention, int batchSize) {
        LocalDateTime now = Objects.requireNonNull(
                jdbc.queryForObject("SELECT UTC_TIMESTAMP(3)", LocalDateTime.class));
        LocalDateTime cutoff = now.minus(retention);
        List<Long> ids = jdbc.queryForList("""
                SELECT i.id FROM reliable_event_identity i
                WHERE i.published_at < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM reliable_event_outbox o
                      WHERE o.id = i.id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM reliable_event_outbox o
                      WHERE o.event_type = i.event_type AND o.event_key = i.event_key
                  )
                ORDER BY i.published_at, i.id LIMIT ?
                """, Long.class, cutoff, batchSize);
        if (ids.isEmpty()) {
            return 0;
        }
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        List<Object> args = new ArrayList<>(ids.size() + 1);
        args.add(cutoff);
        args.addAll(ids);
        return jdbc.update("""
                DELETE i FROM reliable_event_identity i
                WHERE i.published_at < ? AND i.id IN (
                %s) AND NOT EXISTS (
                    SELECT 1 FROM reliable_event_outbox o
                    WHERE o.id = i.id
                ) AND NOT EXISTS (
                    SELECT 1 FROM reliable_event_outbox o
                    WHERE o.event_type = i.event_type AND o.event_key = i.event_key
                )
                """.formatted(placeholders), args.toArray());
    }
}
