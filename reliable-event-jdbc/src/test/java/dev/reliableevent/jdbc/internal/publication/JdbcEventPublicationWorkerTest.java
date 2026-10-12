package dev.reliableevent.jdbc.internal.publication;

import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventStatus;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.JdbcPublicationStore;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportReceipt;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class JdbcEventPublicationWorkerTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
    private final PublicationObserver observer = mock(PublicationObserver.class);
    private final List<Object> states = new ArrayList<>();
    private final List<String> updates = new ArrayList<>();

    private JdbcEventPublicationWorker worker(EventTransport transport) {
        when(transactions.getTransaction(any())).thenReturn(mock(TransactionStatus.class));
        when(jdbc.update(anyString(), any(Object[].class))).thenAnswer(invocation -> {
            states.add(invocation.getArgument(1));
            updates.add(java.util.Arrays.toString(invocation.getArguments()));
            return 1;
        });
        JdbcPublicationStore store = new JdbcPublicationStore(jdbc, transactions, Clock.systemUTC(),
                "worker-test", Duration.ofSeconds(30), observer);
        return new JdbcEventPublicationWorker(store, transport, Clock.systemUTC(),
                new ExponentialBackoff(java.time.Duration.ofSeconds(1), java.time.Duration.ofMinutes(5), 0.2), observer, dev.reliableevent.jdbc.internal.tracing.PublicationTracer.NOOP);
    }

    @Test
    void forwardsImmutableHeadersAndOriginalPayloadAndAcceptsReceiptWithoutId() {
        var event = claimed("{\"traceparent\":\"00-abc\"}", 1);
        TransportReceipt receipt = new TransportReceipt(Optional.empty(), Map.of("topic", "events"));
        List<OutboundEvent> sent = new ArrayList<>();
        assertThat(worker(outbound -> { sent.add(outbound); return receipt; })
                .publishClaimedEvent(event)).isTrue();
        assertThat(sent).singleElement().satisfies(outbound -> {
            assertThat(outbound.id()).isEqualTo(event.event().id());
            assertThat(outbound.eventType()).isEqualTo(event.event().eventType());
            assertThat(outbound.eventKey()).isEqualTo(event.event().eventKey());
            assertThat(outbound.payloadJson()).isEqualTo("{ \"n\" : 1 }");
            assertThat(outbound.headers()).containsExactlyEntriesOf(Map.of("traceparent", "00-abc"));
            assertThatThrownBy(() -> outbound.headers().put("x", "y"))
                    .isInstanceOf(UnsupportedOperationException.class);
        });
        assertThat(states).containsExactly(EventStatus.PUBLISHED.code());
        verify(observer).sendSucceeded(eq(event), eq(receipt), anyLong(), any(Instant.class));
    }

    @Test
    void classifiedTransportFailuresChooseRetryOrDead() {
        for (TransportFailureType type : TransportFailureType.values()) {
            var event = claimed("{}", 1);
            var failure = new TransportException(type, "classified");
            assertThat(worker(outbound -> { throw failure; }).publishClaimedEvent(event)).isFalse();
            verify(observer).sendFailed(eq(event), eq(type), anyLong());
            assertThat(states.get(states.size() - 1)).isEqualTo(
                    type == TransportFailureType.NON_RETRYABLE
                            ? EventStatus.DEAD.code() : EventStatus.RETRY_WAIT.code());
        }
    }

    @Test
    void missingReceiptAndUnclassifiedExceptionAreUnknownAndRetried() {
        var missing = claimed("{}", 1);
        assertThat(worker(outbound -> null).publishClaimedEvent(missing)).isFalse();
        verify(observer).sendFailed(eq(missing), eq(TransportFailureType.RESULT_UNKNOWN), anyLong());
        var unexpected = claimed("{}", 2);
        assertThat(worker(outbound -> { throw new IllegalStateException("private-payload-do-not-store"); })
                .publishClaimedEvent(unexpected)).isFalse();
        verify(observer).sendFailed(eq(unexpected), eq(TransportFailureType.RESULT_UNKNOWN), anyLong());
        assertThat(states).containsExactly(EventStatus.RETRY_WAIT.code(), EventStatus.RETRY_WAIT.code());
        assertThat(updates).allSatisfy(update -> assertThat(update).doesNotContain("private-payload-do-not-store"));
    }

    @Test
    void exhaustedUnknownResultBecomesDead() {
        var event = claimed("{}", 4);
        assertThat(worker(outbound -> null).publishClaimedEvent(event)).isFalse();
        assertThat(states).containsExactly(EventStatus.DEAD.code());
    }

    @Test
    void malformedReservedAndOversizedHeadersBecomeDeadBeforeCallingTransport() {
        EventTransport transport = mock(EventTransport.class);
        var worker = worker(transport);
        for (String headers : new String[]{null, " ", "not-json", "null", "[]", "{\"x\":1}",
                "{\"x\":null}", "{\"reliable_event_id\":\"do-not-print\"}",
                "{\"bad\\nname\":\"do-not-print\"}",
                "{\"oversized\":\"" + "x".repeat(4097) + "\"}"}) {
            var event = claimed(headers, 1);
            assertThat(worker.publishClaimedEvent(event)).isFalse();
            verify(observer).sendFailed(eq(event), eq(TransportFailureType.NON_RETRYABLE), anyLong());
        }
        verifyNoInteractions(transport);
        assertThat(states).hasSize(10).containsOnly(EventStatus.DEAD.code());
    }

    @Test
    void invalidStoredFieldsBecomeDeadBeforeCallingTransport() {
        EventTransport transport = mock(EventTransport.class);
        var event = new ClaimedEvent(new StoredEvent(new EventId(1), " ", "key", "{}", "{}"),
                1, 1, 4, "worker-test", Instant.now().plusSeconds(30));
        assertThat(worker(transport).publishClaimedEvent(event)).isFalse();
        verifyNoInteractions(transport);
        assertThat(states).containsExactly(EventStatus.DEAD.code());
    }

    private ClaimedEvent claimed(String headers, int attempt) {
        return new ClaimedEvent(new StoredEvent(new EventId(attempt), "test.event", "key",
                "{ \"n\" : 1 }", headers), attempt, attempt, 4, "worker-test",
                Instant.now().plusSeconds(30));
    }
}
