package dev.reliableevent.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.MissingActiveTransactionException;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.ReliableEventSerializationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.util.Objects;

public final class JdbcReliableEventPublisher implements ReliableEventPublisher {

    private static final int DEFAULT_MAX_ATTEMPTS = 8;

    private final JdbcOutboxRepository repository;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final int maxAttempts;

    public JdbcReliableEventPublisher(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this(jdbcTemplate, objectMapper, Clock.systemUTC(), DEFAULT_MAX_ATTEMPTS);
    }

    JdbcReliableEventPublisher(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            Clock clock,
            int maxAttempts
    ) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.repository = new JdbcOutboxRepository(Objects.requireNonNull(jdbcTemplate));
        this.objectMapper = Objects.requireNonNull(objectMapper);
        this.clock = Objects.requireNonNull(clock);
        this.maxAttempts = maxAttempts;
    }

    @Override
    public EventId publish(ReliableEvent<?> event) {
        Objects.requireNonNull(event, "event must not be null");
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new MissingActiveTransactionException();
        }

        String payloadJson = serialize(event.payload(), "payload");
        String headersJson = serialize(event.headers(), "headers");
        return repository.insert(
                event.eventType(),
                event.eventKey(),
                payloadJson,
                headersJson,
                event.availableAt(),
                clock.instant(),
                maxAttempts
        );
    }

    private String serialize(Object value, String fieldName) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new ReliableEventSerializationException(
                    "Unable to serialize reliable event " + fieldName,
                    exception
            );
        }
    }
}
