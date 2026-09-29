package dev.reliableevent.jdbc;

import dev.reliableevent.EventId;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/** JDBC implementation of the application-facing dead-event operations. */
public final class JdbcDeadEventOperations implements DeadEventOperations {

    private final JdbcDeadEventQuery query;
    private final JdbcDeadEventReplay replay;

    public JdbcDeadEventOperations(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager) {
        this.query = new JdbcDeadEventQuery(jdbcTemplate);
        this.replay = new JdbcDeadEventReplay(jdbcTemplate, transactionManager);
    }

    @Override
    public DeadEventPage firstPage(int pageSize) {
        return query.firstPage(pageSize);
    }

    @Override
    public DeadEventPage nextPage(EventId beforeExclusive, int pageSize) {
        return query.nextPage(beforeExclusive, pageSize);
    }

    @Override
    public DeadEventLookup lookup(EventId id) {
        return query.lookup(id);
    }

    @Override
    public DeadEventReplayResult replay(DeadEventReplayRequest request) {
        return replay.replay(request);
    }
}
