package dev.reliableevent.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventSerializationException;
import dev.reliableevent.jdbc.internal.tracing.RegistrationTracer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class JdbcReliableEventPublisherTracingTest {
    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);

    @AfterEach
    void clearTransactionFlag() {
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @Test
    void tracingCallbackFailuresDoNotReplaceDatabaseFailure() {
        DataAccessResourceFailureException databaseFailure =
                new DataAccessResourceFailureException("database unavailable");
        doThrow(databaseFailure).when(jdbcTemplate)
                .update(any(PreparedStatementCreator.class), any(KeyHolder.class));
        TransactionSynchronizationManager.setActualTransactionActive(true);

        JdbcReliableEventPublisher publisher = new JdbcReliableEventPublisher(
                jdbcTemplate, new ObjectMapper(), 8, failingCallbacks());
        assertThatThrownBy(() -> publisher.publish(event()))
                .isSameAs(databaseFailure);
    }

    @Test
    void tracingCallbackFailuresDoNotReplaceSerializationFailure() {
        ObjectMapper mapper = new ObjectMapper() {
            @Override
            public String writeValueAsString(Object value) throws JsonProcessingException {
                throw new JsonProcessingException("serialization failed") { };
            }
        };
        TransactionSynchronizationManager.setActualTransactionActive(true);

        JdbcReliableEventPublisher publisher = new JdbcReliableEventPublisher(
                jdbcTemplate, mapper, 8, failingCallbacks());
        assertThatThrownBy(() -> publisher.publish(event()))
                .isInstanceOf(ReliableEventSerializationException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void tracingStartFailureUsesNoopAndDoesNotPreventSqlCall() {
        IllegalStateException traceFailure = new IllegalStateException("tracer unavailable");
        RegistrationTracer failingStart = headers -> { throw traceFailure; };
        doThrow(new DataAccessResourceFailureException("database unavailable")).when(jdbcTemplate)
                .update(any(PreparedStatementCreator.class), any(KeyHolder.class));
        TransactionSynchronizationManager.setActualTransactionActive(true);

        JdbcReliableEventPublisher publisher = new JdbcReliableEventPublisher(
                jdbcTemplate, new ObjectMapper(), 8, failingStart);
        assertThatThrownBy(() -> publisher.publish(event()))
                .isInstanceOf(DataAccessResourceFailureException.class)
                .isNotSameAs(traceFailure);
    }

    private RegistrationTracer failingCallbacks() {
        return headers -> new RegistrationTracer.Registration() {
            @Override public Map<String, String> headers() { return headers; }
            @Override public void succeeded(EventId eventId) { throw new IllegalStateException("tag failed"); }
            @Override public void failed(Throwable failure) { throw new IllegalStateException("error failed"); }
            @Override public void close() { throw new IllegalStateException("close failed"); }
        };
    }

    private ReliableEvent<String> event() {
        return new ReliableEvent<>("trace-test", "trace-test-key", "payload", Instant.now(), Map.of());
    }
}
