package dev.reliableevent.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

final class JdbcEventPublicationWorker {

    private final JdbcOutboxRepository repository;
    private final EventSender sender;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final int batchSize;

    JdbcEventPublicationWorker(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            EventSender sender,
            Clock clock,
            int batchSize
    ) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.repository = new JdbcOutboxRepository(Objects.requireNonNull(jdbcTemplate));
        this.transaction = new TransactionTemplate(Objects.requireNonNull(transactionManager));
        this.sender = Objects.requireNonNull(sender);
        this.clock = Objects.requireNonNull(clock);
        this.batchSize = batchSize;
    }

    int publishDueEvents() {
        Instant now = clock.instant();
        int publishedCount = 0;
        for (long eventId : repository.findDueEventIds(now, batchSize)) {
            Boolean published = transaction.execute(status -> publishOne(eventId, now));
            if (Boolean.TRUE.equals(published)) {
                publishedCount++;
            }
        }
        return publishedCount;
    }

    private boolean publishOne(long eventId, Instant now) {
        if (!repository.markPublishing(eventId, now)) {
            return false;
        }
        StoredEvent event = repository.findById(eventId);
        sender.send(event);
        repository.markPublished(eventId, now);
        return true;
    }
}
