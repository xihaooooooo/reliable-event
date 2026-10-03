package dev.reliableevent.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.MissingActiveTransactionException;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.ReliableEventSerializationException;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.tracing.RegistrationTracer;
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
    private final RegistrationTracer registrationTracer;

    public JdbcReliableEventPublisher(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this(jdbcTemplate, objectMapper, Clock.systemUTC(), DEFAULT_MAX_ATTEMPTS, RegistrationTracer.NOOP);
    }

    public JdbcReliableEventPublisher(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            int maxAttempts
    ) {
        this(jdbcTemplate, objectMapper, Clock.systemUTC(), maxAttempts, RegistrationTracer.NOOP);
    }

    public JdbcReliableEventPublisher(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            int maxAttempts,
            RegistrationTracer registrationTracer
    ) {
        this(jdbcTemplate, objectMapper, Clock.systemUTC(), maxAttempts, registrationTracer);
    }

    JdbcReliableEventPublisher(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            Clock clock,
            int maxAttempts
    ) {
        this(jdbcTemplate, objectMapper, clock, maxAttempts, RegistrationTracer.NOOP);
    }

    JdbcReliableEventPublisher(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            Clock clock,
            int maxAttempts,
            RegistrationTracer registrationTracer
    ) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.repository = new JdbcOutboxRepository(Objects.requireNonNull(jdbcTemplate));
        this.objectMapper = Objects.requireNonNull(objectMapper);
        this.clock = Objects.requireNonNull(clock);
        this.maxAttempts = maxAttempts;
        this.registrationTracer = Objects.requireNonNull(registrationTracer);
    }

    @Override
    public EventId publish(ReliableEvent<?> event) {
        Objects.requireNonNull(event, "event must not be null");
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new MissingActiveTransactionException();
        }

        RegistrationTracer.Registration registration = beginRegistration(event.headers());
        try {
            String payloadJson = serialize(event.payload(), "payload");
            String headersJson = serialize(registrationHeaders(registration, event.headers()), "headers");
            EventId eventId = repository.insert(
                    event.eventType(),
                    event.eventKey(),
                    payloadJson,
                    headersJson,
                    event.availableAt(),
                    clock.instant(),
                    maxAttempts
            );
            safely(() -> registration.succeeded(eventId));
            return eventId;
        } catch (RuntimeException | Error failure) {
            safely(() -> registration.failed(failure));
            throw failure;
        } finally {
            safely(registration::close);
        }
    }

    private RegistrationTracer.Registration beginRegistration(java.util.Map<String, String> headers) {
        try {
            RegistrationTracer.Registration registration = registrationTracer.begin(headers);
            return registration == null ? RegistrationTracer.NOOP.begin(headers) : registration;
        } catch (RuntimeException tracingFailure) {
            return RegistrationTracer.NOOP.begin(headers);
        }
    }

    private java.util.Map<String, String> registrationHeaders(
            RegistrationTracer.Registration registration,
            java.util.Map<String, String> original
    ) {
        try {
            java.util.Map<String, String> headers = registration.headers();
            return headers == null ? original : headers;
        } catch (RuntimeException tracingFailure) {
            return original;
        }
    }

    private void safely(Runnable tracingAction) {
        try {
            tracingAction.run();
        } catch (RuntimeException ignored) {
            // Trace recording/export failures must never change business or database outcomes.
        }
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
