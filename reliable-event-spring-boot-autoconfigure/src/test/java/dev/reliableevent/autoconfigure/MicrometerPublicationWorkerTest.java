package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.StaleEventClaimException;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelBaggageManager;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MicrometerPublicationWorkerTest {
    private static final Duration LEASE = Duration.ofSeconds(10);
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void workerRecordsSenderAndStateOutcomesAndKeepsExternalTransactionPending() throws Exception {
        Fixture fixture = new Fixture();
        Map<String, String> parent = fixture.parentHeaders();
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        MicrometerPublicationObserver metricsObserver = new MicrometerPublicationObserver(
                metrics, mock(JdbcOutboxRepository.class));

        JdbcTemplate successJdbc = jdbcReturning(1);
        JdbcEventPublicationWorker successWorker = worker(successJdbc, txManager(),
                event -> new SendReceipt("success-message"), fixture.publicationTracer, metricsObserver);
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            assertThat(successWorker.publishClaimedEvent(claimed(1001, 1, parent))).isTrue();
        } finally {
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }

        JdbcEventPublicationWorker definiteFailureWorker = worker(jdbcReturning(1), txManager(),
                event -> { throw EventSendException.retryable("broker rejected"); }, fixture.publicationTracer,
                metricsObserver);
        assertThat(definiteFailureWorker.publishClaimedEvent(claimed(1002, 1, parent))).isFalse();

        JdbcEventPublicationWorker unknownWorker = worker(jdbcReturning(1), txManager(),
                event -> { throw EventSendException.resultUnknown("ack lost"); }, fixture.publicationTracer,
                metricsObserver);
        assertThat(unknownWorker.publishClaimedEvent(claimed(1003, 1, parent))).isFalse();

        DataAccessResourceFailureException databaseFailure =
                new DataAccessResourceFailureException("status database unavailable");
        JdbcTemplate failedJdbc = mock(JdbcTemplate.class);
        when(failedJdbc.update(anyString(), any(Object[].class))).thenThrow(databaseFailure);
        JdbcEventPublicationWorker failedUpdateWorker = worker(failedJdbc, txManager(),
                event -> new SendReceipt("sent-before-db-failure"), fixture.publicationTracer, metricsObserver);
        assertThatThrownBy(() -> failedUpdateWorker.publishClaimedEvent(claimed(1004, 1, parent)))
                .isSameAs(databaseFailure);

        JdbcEventPublicationWorker ownershipWorker = worker(jdbcReturning(0), txManager(),
                event -> new SendReceipt("sent-before-ownership-reject"), fixture.publicationTracer, metricsObserver);
        assertThatThrownBy(() -> ownershipWorker.publishClaimedEvent(claimed(1005, 1, parent)))
                .isInstanceOf(StaleEventClaimException.class);

        fixture.flush();
        List<SpanData> attempts = fixture.attempts();
        assertThat(attempts).hasSize(5);
        assertThat(attribute(attempts, 1001, "reliable_event.send.result")).isEqualTo("success");
        assertThat(attribute(attempts, 1001, "reliable_event.state_update.commit")).isEqualTo("pending");
        assertThat(attribute(attempts, 1001, "reliable_event.state_update.target")).isEqualTo("PUBLISHED");
        assertThat(attribute(attempts, 1002, "reliable_event.send.result")).isEqualTo("definite_failure");
        assertThat(attribute(attempts, 1002, "reliable_event.state_update.target")).isEqualTo("RETRY_WAIT");
        assertThat(attribute(attempts, 1003, "reliable_event.send.result")).isEqualTo("unknown");
        assertThat(attribute(attempts, 1003, "reliable_event.state_update.target")).isEqualTo("RETRY_WAIT");
        assertThat(attribute(attempts, 1004, "reliable_event.send.result")).isEqualTo("success");
        assertThat(attribute(attempts, 1004, "reliable_event.state_update.result")).isEqualTo("failed");
        assertThat(attribute(attempts, 1005, "reliable_event.send.result")).isEqualTo("success");
        assertThat(attribute(attempts, 1005, "reliable_event.state_update.result")).isEqualTo("ownership_rejected");
        assertThat(attempts).allSatisfy(span -> assertThat(span.getParentSpanId()).isEqualTo(fixture.parentSpanId));
        assertThat(metrics.get("reliable_event.publish.success").counter().count()).isEqualTo(3);
        assertThat(metrics.get("reliable_event.publication.persisted").counter().count()).isZero();
        assertThat(metrics.get("reliable_event.publication.state_update_failure").counter().count()).isEqualTo(2);
        assertThat(metrics.get("reliable_event.worker.inflight").gauge().value()).isZero();
        metricsObserver.close();
        metrics.close();
        fixture.close();
    }

    @Test
    void tracingCallbacksAndInterruptionCannotChangeStatePathAndAlwaysCloseAttempt() throws Exception {
        Fixture fixture = new Fixture();
        Map<String, String> parent = fixture.parentHeaders();
        int[] closes = {0};
        PublicationTracer brokenTracer = event -> new PublicationTracer.Attempt() {
            @Override public StoredEvent eventForSend() { throw new IllegalStateException("inject failure"); }
            @Override public void sendSucceeded(SendReceipt receipt) { throw new IllegalStateException("record failure"); }
            @Override public void sendFailed(dev.reliableevent.internal.publication.EventSendFailureType type,
                                             Throwable failure) { throw new IllegalStateException("record failure"); }
            @Override public void stateUpdated(String status, boolean commitPending) {
                throw new IllegalStateException("record failure");
            }
            @Override public void stateUpdateFailed(String status, Throwable failure, boolean rejected) {
                throw new IllegalStateException("record failure");
            }
            @Override public void close() { closes[0]++; throw new IllegalStateException("close failure"); }
        };
        JdbcEventPublicationWorker brokenWorker = worker(jdbcReturning(1), txManager(),
                event -> new SendReceipt("still-published"), brokenTracer);
        assertThat(brokenWorker.publishClaimedEvent(claimed(2001, 1, parent))).isTrue();
        assertThat(closes[0]).isEqualTo(1);

        PublicationTracer tracingOnlyFailBegin = event -> { throw new IllegalStateException("start failure"); };
        JdbcEventPublicationWorker noSpanWorker = worker(jdbcReturning(1), txManager(),
                event -> new SendReceipt("published-with-noop"), tracingOnlyFailBegin);
        assertThat(noSpanWorker.publishClaimedEvent(claimed(2002, 1, parent))).isTrue();

        PublicationTracer closeCounter = event -> {
            PublicationTracer.Attempt delegate = fixture.publicationTracer.begin(event);
            return new PublicationTracer.Attempt() {
            @Override public StoredEvent eventForSend() { return delegate.eventForSend(); }
            @Override public void sendSucceeded(SendReceipt receipt) { delegate.sendSucceeded(receipt); }
            @Override public void sendFailed(dev.reliableevent.internal.publication.EventSendFailureType type,
                                             Throwable failure) { delegate.sendFailed(type, failure); }
            @Override public void stateUpdated(String status, boolean commitPending) {
                delegate.stateUpdated(status, commitPending);
            }
            @Override public void stateUpdateFailed(String status, Throwable failure, boolean rejected) { }
            @Override public void close() {
                try { delegate.close(); } finally { closes[0]++; }
            }
            };
        };
        JdbcEventPublicationWorker interruptedWorker = worker(jdbcReturning(1), txManager(),
                event -> {
                    Thread.currentThread().interrupt();
                    throw EventSendException.resultUnknown("interrupted send");
                }, closeCounter);
        try {
            assertThat(interruptedWorker.publishClaimedEvent(claimed(2003, 1, parent))).isFalse();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(closes[0]).isEqualTo(2);
        } finally {
            Thread.interrupted();
        }
        fixture.flush();
        assertThat(fixture.tracer.currentSpan()).isNull();
        assertThat(fixture.attempts()).anySatisfy(span -> {
            assertThat(span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                    "reliable_event.id"))).isEqualTo("2003");
            assertThat(span.hasEnded()).isTrue();
        });
        fixture.close();
    }

    private JdbcTemplate jdbcReturning(int updateCount) {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(updateCount);
        return jdbc;
    }

    private JdbcEventPublicationWorker worker(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
                                              EventSender sender, PublicationTracer tracer) {
        return worker(jdbc, transactionManager, sender, tracer, PublicationObserver.NOOP);
    }

    private JdbcEventPublicationWorker worker(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
                                              EventSender sender, PublicationTracer tracer,
                                              PublicationObserver observer) {
        return new JdbcEventPublicationWorker(jdbc, transactionManager, sender, java.time.Clock.systemUTC(),
                10, "worker-test", LEASE, new ExponentialBackoff(Duration.ofSeconds(1), Duration.ofMinutes(1), 0.2, () -> 0.0),
                observer, tracer);
    }

    private ClaimedEvent claimed(long eventId, int attempt, Map<String, String> headers) throws Exception {
        return new ClaimedEvent(new StoredEvent(new EventId(eventId), "test.event", "key-" + eventId,
                "{}", mapper.writeValueAsString(headers)), attempt, attempt, 4, "worker-test",
                Instant.now().plusSeconds(LEASE.toSeconds()));
    }

    private static String attribute(List<SpanData> spans, long eventId, String key) {
        SpanData span = spans.stream().filter(value -> value.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("reliable_event.id"))
                .equals(Long.toString(eventId))).findFirst().orElseThrow();
        return span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(key));
    }

    private final class Fixture implements AutoCloseable {
        private final CollectingExporter exporter = new CollectingExporter();
        private final SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        private final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance())).build();
        private final OtelCurrentTraceContext current = new OtelCurrentTraceContext();
        private final Tracer tracer = new OtelTracer(sdk.getTracer("worker-m8.2-test"), current,
                ignored -> { }, new OtelBaggageManager(current, List.of(), List.of()));
        private final io.micrometer.tracing.propagation.Propagator propagator =
                new OtelPropagator(sdk.getPropagators(), sdk.getTracer("worker-m8.2-test"));
        private final PublicationTracer publicationTracer =
                new MicrometerPublicationTracer(tracer, propagator, mapper);
        private String parentSpanId;

        private Fixture() { }

        private Map<String, String> parentHeaders() {
            var parent = tracer.nextSpan().name("registration-context").start();
            parentSpanId = parent.context().spanId();
            java.util.Map<String, String> headers = new java.util.LinkedHashMap<>();
            propagator.inject(parent.context(), headers, (carrier, key, value) -> carrier.put(key, value));
            parent.end();
            return headers;
        }

        private List<SpanData> attempts() {
            return exporter.spans.stream().filter(span -> span.getName().equals("reliable-event.publish")).toList();
        }

        private void flush() { provider.forceFlush().join(10, TimeUnit.SECONDS); }

        @Override public void close() { sdk.close(); }
    }

    private static final class CollectingExporter implements SpanExporter {
        private final ConcurrentLinkedQueue<SpanData> spans = new ConcurrentLinkedQueue<>();
        @Override public CompletableResultCode export(Collection<SpanData> values) {
            spans.addAll(values);
            return CompletableResultCode.ofSuccess();
        }
        @Override public CompletableResultCode flush() { return CompletableResultCode.ofSuccess(); }
        @Override public CompletableResultCode shutdown() { return CompletableResultCode.ofSuccess(); }
    }

    private PlatformTransactionManager txManager() {
        PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
        when(manager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));
        return manager;
    }
}
