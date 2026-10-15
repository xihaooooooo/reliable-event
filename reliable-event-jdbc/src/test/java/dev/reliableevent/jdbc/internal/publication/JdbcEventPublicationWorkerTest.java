package dev.reliableevent.jdbc.internal.publication;

import dev.reliableevent.EventId;
import dev.reliableevent.EventFallbackContext;
import dev.reliableevent.EventFallbackHandler;
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
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

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
        return worker(transport, null);
    }

    private JdbcEventPublicationWorker worker(EventTransport transport, EventFallbackHandler fallbackHandler) {
        when(transactions.getTransaction(any())).thenReturn(mock(TransactionStatus.class));
        when(jdbc.update(anyString(), any(Object[].class))).thenAnswer(invocation -> {
            states.add(invocation.getArgument(1));
            updates.add(java.util.Arrays.toString(invocation.getArguments()));
            return 1;
        });
        JdbcPublicationStore store = new JdbcPublicationStore(jdbc, transactions, Clock.systemUTC(),
                "worker-test", Duration.ofSeconds(30), observer);
        return new JdbcEventPublicationWorker(store, transport, Clock.systemUTC(),
                new ExponentialBackoff(java.time.Duration.ofSeconds(1), java.time.Duration.ofMinutes(5), 0.2),
                observer, dev.reliableevent.jdbc.internal.tracing.PublicationTracer.NOOP, fallbackHandler);
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
    void classifiedTransportFailuresChooseRetryOrFallback() {
        for (TransportFailureType type : TransportFailureType.values()) {
            var event = claimed("{}", 1);
            var failure = new TransportException(type, "classified");
            assertThat(worker(outbound -> { throw failure; }).publishClaimedEvent(event)).isFalse();
            verify(observer).sendFailed(eq(event), eq(type), anyLong());
            assertThat(states.get(states.size() - 1)).isEqualTo(
                    type == TransportFailureType.NON_RETRYABLE
                            ? EventStatus.FAILED.code() : EventStatus.RETRY_WAIT.code());
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
    void exhaustedUnknownResultFallsBackAndMissingHandlerLeavesFailed() {
        var event = claimed("{}", 4);
        assertThat(worker(outbound -> null).publishClaimedEvent(event)).isFalse();
        assertThat(states).contains(EventStatus.FAILED.code());
        assertThat(updates.toString()).contains("status = 6");
    }

    @Test
    void fallbackIsCalledOnlyForTerminalFailureAndReceivesRawInvalidHeaders() {
        AtomicInteger calls = new AtomicInteger();
        List<EventFallbackContext> contexts = new ArrayList<>();
        EventFallbackHandler handler = context -> { calls.incrementAndGet(); contexts.add(context); };
        var retryable = claimed("{}", 1);
        assertThat(worker(outbound -> { throw new TransportException(TransportFailureType.RETRYABLE, "retry"); },
                handler).publishClaimedEvent(retryable)).isFalse();
        assertThat(calls).hasValue(0);

        var terminal = claimed("not-json", 1);
        assertThat(worker(outbound -> { throw new TransportException(TransportFailureType.NON_RETRYABLE, "reject"); },
                handler).publishClaimedEvent(terminal)).isFalse();
        assertThat(calls).hasValue(1);
        assertThat(contexts.get(0).event().headersJson()).isEqualTo("not-json");
        assertThat(contexts.get(0).failureClassification())
                .isEqualTo(EventFallbackContext.FailureClassification.NON_RETRYABLE);
        assertThat(states).contains(EventStatus.FALLBACK_COMPLETED.code());
    }

    @Test
    void fallbackExceptionLeavesFailedWithFallbackAndOriginalErrors() {
        EventFallbackHandler handler = context -> { throw new IllegalArgumentException("fallback-broke"); };
        assertThat(worker(outbound -> { throw new TransportException(TransportFailureType.NON_RETRYABLE, "send-broke"); },
                handler).publishClaimedEvent(claimed("{}", 1))).isFalse();
        assertThat(updates.toString()).contains("fallback-broke", "send-broke");
        assertThat(states).contains(EventStatus.FAILED.code());
    }

    @Test
    void exhaustedUnknownResultPreservesUnknownClassificationAndStopReason() {
        List<EventFallbackContext> handled = new ArrayList<>();
        assertThat(worker(outbound -> null, handled::add)
                .publishClaimedEvent(claimed("{}", 4))).isFalse();
        assertThat(handled).singleElement().satisfies(context -> {
            assertThat(context.stopReason()).isEqualTo(EventFallbackContext.StopReason.ATTEMPTS_EXHAUSTED);
            assertThat(context.failureClassification())
                    .isEqualTo(EventFallbackContext.FailureClassification.RESULT_UNKNOWN);
        });
        assertThat(states).contains(EventStatus.FALLBACK_COMPLETED.code());
    }

    @Test
    void fallbackIsRejectedInsideCallerTransaction() {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            AtomicInteger calls = new AtomicInteger();
            assertThatThrownBy(() -> worker(outbound -> {
                throw new TransportException(TransportFailureType.NON_RETRYABLE, "reject");
            }, context -> calls.incrementAndGet()).publishClaimedEvent(claimed("{}", 1)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("outside an active transaction");
            assertThat(calls).hasValue(0);
        } finally {
            TransactionSynchronizationManager.clear();
        }
    }

    @Test
    void malformedReservedAndOversizedHeadersEnterFallbackBeforeCallingTransport() {
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
        assertThat(states).hasSize(20);
        assertThat(states).contains(EventStatus.FAILED.code());
        assertThat(updates.toString()).contains("status = 6");
    }

    @Test
    void invalidStoredFieldsEnterFallbackBeforeCallingTransport() {
        EventTransport transport = mock(EventTransport.class);
        var event = new ClaimedEvent(new StoredEvent(new EventId(1), " ", "key", "{}", "{}"),
                1, 1, 4, "worker-test", Instant.now().plusSeconds(30));
        assertThat(worker(transport).publishClaimedEvent(event)).isFalse();
        verifyNoInteractions(transport);
        assertThat(states).contains(EventStatus.FAILED.code());
        assertThat(updates.toString()).contains("status = 6");
    }

    private ClaimedEvent claimed(String headers, int attempt) {
        return new ClaimedEvent(new StoredEvent(new EventId(attempt), "test.event", "key",
                "{ \"n\" : 1 }", headers), attempt, attempt, 4, "worker-test",
                Instant.now().plusSeconds(30));
    }
}
