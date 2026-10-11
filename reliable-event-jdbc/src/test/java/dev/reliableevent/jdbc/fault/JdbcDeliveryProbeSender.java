package dev.reliableevent.jdbc.fault;

import java.util.Map;
import java.util.Optional;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.TransportReceipt;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.UUID;

final class JdbcDeliveryProbeSender implements EventTransport {

    private final JdbcTemplate jdbcTemplate;
    private final String workerId;

    JdbcDeliveryProbeSender(DataSource dataSource, String workerId) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource));
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        this.workerId = workerId;
    }

    @Override
    public TransportReceipt send(OutboundEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("delivery probe must run outside an active transaction");
        }

        String messageId = workerId + "-" + UUID.randomUUID();
        int inserted = jdbcTemplate.update(
                """
                INSERT INTO test_message_delivery (
                    event_id,
                    event_key,
                    worker_id,
                    message_id,
                    delivered_at
                ) VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))
                """,
                event.id().value(),
                event.eventKey(),
                workerId,
                messageId
        );
        if (inserted != 1) {
            throw new IllegalStateException("delivery probe did not insert exactly one row");
        }
        return new TransportReceipt(Optional.ofNullable(messageId), Map.of());
    }
}
