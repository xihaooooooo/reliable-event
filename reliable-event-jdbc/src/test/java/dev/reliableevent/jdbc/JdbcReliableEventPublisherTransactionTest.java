package dev.reliableevent.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.MissingActiveTransactionException;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.jdbc.internal.tracing.RegistrationTracer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class JdbcReliableEventPublisherTransactionTest {
    private final DataSource dataSource = mock(DataSource.class);
    private final Connection connection = mock(Connection.class);
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final ObjectMapper mapper = mock(ObjectMapper.class);
    private final RegistrationTracer tracer = mock(RegistrationTracer.class);
    private final AtomicBoolean autoCommit = new AtomicBoolean(true);
    private TransactionTemplate transaction;
    private JdbcReliableEventPublisher publisher;

    @BeforeEach
    void setUp() throws Exception {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getAutoCommit()).thenAnswer(ignored -> autoCommit.get());
        doAnswer(invocation -> {
            autoCommit.set(invocation.getArgument(0));
            return null;
        }).when(connection).setAutoCommit(anyBoolean());
        when(jdbc.getDataSource()).thenReturn(dataSource);
        transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        publisher = new JdbcReliableEventPublisher(jdbc, mapper, 8, tracer);
    }

    @Test
    void nullEventIsRejectedBeforeCheckingTransactions() {
        assertThatThrownBy(() -> publisher.publish(null))
                .isInstanceOf(NullPointerException.class).hasMessage("event must not be null");
        assertNoRegistration();
    }

    @Test
    void missingTransactionIsRejectedBeforeRegistration() {
        assertThatThrownBy(() -> publisher.publish(event()))
                .isInstanceOf(MissingActiveTransactionException.class);
        assertNoRegistration();
    }

    @Test
    void synchronizationWithoutActualTransactionIsRejected() {
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_SUPPORTS);
        transaction.executeWithoutResult(status -> {
            assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isTrue();
            assertThatThrownBy(() -> publisher.publish(event()))
                    .isInstanceOf(MissingActiveTransactionException.class);
        });
        assertNoRegistration();
    }

    @Test
    void readOnlyTransactionIsRejectedBeforeRegistration() {
        transaction.setReadOnly(true);
        transaction.executeWithoutResult(status -> assertThatThrownBy(() -> publisher.publish(event()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("read-only"));
        assertNoRegistration();
    }

    @Test
    void transactionForAnotherDataSourceDoesNotAcquireAnOutboxConnection() throws Exception {
        DataSource otherDataSource = mock(DataSource.class);
        when(jdbc.getDataSource()).thenReturn(otherDataSource);
        transaction.executeWithoutResult(status -> assertThatThrownBy(() -> publisher.publish(event()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Outbox DataSource"));
        verify(otherDataSource, never()).getConnection();
        assertNoRegistration();
    }

    @Test
    void unrelatedConnectionBoundBySynchronizationIsStillRejected() throws Exception {
        DataSource otherDataSource = mock(DataSource.class);
        Connection otherConnection = mock(Connection.class);
        when(otherDataSource.getConnection()).thenReturn(otherConnection);
        when(otherConnection.getAutoCommit()).thenReturn(true);
        when(jdbc.getDataSource()).thenReturn(otherDataSource);
        transaction.executeWithoutResult(status -> {
            new JdbcTemplate(otherDataSource).execute((ConnectionCallback<Boolean>) Connection::getAutoCommit);
            assertThat(TransactionSynchronizationManager.hasResource(otherDataSource)).isTrue();
            assertThatThrownBy(() -> publisher.publish(event()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("auto-commit");
        });
        assertNoRegistration();
    }

    @Test
    void connectionStateFailureIsReportedBeforeRegistration() throws Exception {
        transaction.executeWithoutResult(status -> {
            SQLException failure = new SQLException("connection state unavailable");
            try {
                when(connection.getAutoCommit()).thenThrow(failure);
                assertThatThrownBy(() -> publisher.publish(event()))
                        .isInstanceOf(DataAccessResourceFailureException.class).hasCause(failure);
                doAnswer(ignored -> autoCommit.get()).when(connection).getAutoCommit();
            } catch (SQLException unexpected) {
                throw new AssertionError(unexpected);
            }
        });
        assertNoRegistration();
    }

    @Test
    void matchingWritableTransactionRegistersBothRowsAndCommits() throws Exception {
        prepareSuccessfulInsert();
        EventId eventId = transaction.execute(status -> publisher.publish(event()));
        assertThat(eventId).isEqualTo(new EventId(42));
        verify(connection).commit();
        verify(jdbc).update(any(PreparedStatementCreator.class), any(KeyHolder.class));
        verify(jdbc).update(anyString(), any(Object[].class));
    }

    @Test
    void transactionAwareProxyUsesTheTargetsTransactionResource() {
        when(jdbc.getDataSource()).thenReturn(new TransactionAwareDataSourceProxy(dataSource));
        prepareSuccessfulInsert();
        EventId eventId = transaction.execute(status -> publisher.publish(event()));
        assertThat(eventId).isEqualTo(new EventId(42));
    }

    @Test
    void missingOutboxDataSourceIsRejectedBeforeRegistration() {
        when(jdbc.getDataSource()).thenReturn(null);
        transaction.executeWithoutResult(status -> assertThatThrownBy(() -> publisher.publish(event()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Outbox DataSource"));
        assertNoRegistration();
    }

    private void prepareSuccessfulInsert() {
        when(jdbc.update(any(PreparedStatementCreator.class), any(KeyHolder.class))).thenAnswer(invocation -> {
            KeyHolder keys = invocation.getArgument(1);
            keys.getKeyList().add(Map.of("id", 42L));
            return 1;
        });
    }

    private void assertNoRegistration() {
        verifyNoInteractions(mapper, tracer);
        verify(jdbc, never()).update(any(PreparedStatementCreator.class), any(KeyHolder.class));
        verify(jdbc, never()).update(anyString(), any(Object[].class));
    }

    private ReliableEvent<String> event() {
        return new ReliableEvent<>("transaction-test", "key", "payload", Instant.now(), Map.of());
    }
}
