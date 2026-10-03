package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import dev.reliableevent.spi.EventTransport;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MicrometerPublicationTracerTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void confirmedReceiptWithoutMessageIdDoesNotAddMessageIdAttribute() throws Exception {
        CollectingExporter exporter = new CollectingExporter();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance())).build();
        OtelCurrentTraceContext current = new OtelCurrentTraceContext();
        Tracer tracer = new OtelTracer(sdk.getTracer("m9.1-test"), current, ignored -> { },
                new OtelBaggageManager(current, List.of(), List.of()));
        Propagator propagator = new OtelPropagator(sdk.getPropagators(), sdk.getTracer("m9.1-test"));
        PublicationTracer.Attempt attempt = new MicrometerPublicationTracer(tracer, propagator, objectMapper)
                .begin(claimed(storedEvent(Map.of()), 1));
        attempt.sendSucceeded(new SendReceipt(null));
        attempt.close();

        assertThat(provider.forceFlush().join(10, TimeUnit.SECONDS).isSuccess()).isTrue();
        SpanData span = exporter.spans.stream().findFirst().orElseThrow();
        assertThat(span.getAttributes().asMap().keySet())
                .noneMatch(key -> key.getKey().equals("reliable_event.message_id"));
        sdk.close();
    }

    @Test
    void tracingDoesNotReplaceInvalidPersistedHeadersBeforeTransportValidation() throws Exception {
        CollectingExporter exporter = new CollectingExporter();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance())).build();
        OtelCurrentTraceContext current = new OtelCurrentTraceContext();
        Tracer tracer = new OtelTracer(sdk.getTracer("m9.1-invalid-header-test"), current, ignored -> { },
                new OtelBaggageManager(current, List.of(), List.of()));
        Propagator propagator = new OtelPropagator(sdk.getPropagators(), sdk.getTracer("m9.1-invalid-header-test"));
        EventTransport transport = org.mockito.Mockito.mock(EventTransport.class);
        EventTransportSenderBridge bridge = new EventTransportSenderBridge(transport, objectMapper);
        PublicationTracer micrometerTracer = new MicrometerPublicationTracer(tracer, propagator, objectMapper);

        for (Map<String, String> invalidHeaders : List.of(
                Map.of("traceparent", " "),
                Map.of("tracestate", "x".repeat(4097)))) {
            StoredEvent original = storedEvent(invalidHeaders);
            PublicationTracer.Attempt attempt = micrometerTracer.begin(claimed(original, 1));
            try {
                assertThat(attempt.eventForSend()).isSameAs(original);
                assertThatThrownBy(() -> bridge.send(attempt.eventForSend()))
                        .isInstanceOf(dev.reliableevent.internal.publication.EventSendException.class)
                        .extracting(failure -> ((dev.reliableevent.internal.publication.EventSendException) failure)
                                .failureType())
                        .isEqualTo(dev.reliableevent.internal.publication.EventSendFailureType.NON_RETRYABLE);
            } finally {
                attempt.close();
            }
        }
        org.mockito.Mockito.verifyNoInteractions(transport);
        sdk.close();
    }

    @Test
    void retriesAreIndependentChildrenAndMissingOrBadContextStartsRootWithoutLeakingScope() throws Exception {
        CollectingExporter exporter = new CollectingExporter();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        OtelCurrentTraceContext current = new OtelCurrentTraceContext();
        Tracer tracer = new OtelTracer(sdk.getTracer("m8.2-test"), current, ignored -> { },
                new OtelBaggageManager(current, List.of(), List.of()));
        Propagator delegate =
                new OtelPropagator(sdk.getPropagators(), sdk.getTracer("m8.2-test"));
        boolean[] baggageSuppressed = {false};
        boolean[] failExtraction = {false};
        boolean[] failInjection = {false};
        Propagator propagator = new Propagator() {
            @Override public List<String> fields() { return List.of("traceparent", "tracestate", "baggage"); }
            @Override public <C> void inject(io.micrometer.tracing.TraceContext context, C carrier, Setter<C> setter) {
                if (failInjection[0]) throw new IllegalStateException("propagator injection failed");
                delegate.inject(context, carrier, setter);
            }
            @Override public <C> io.micrometer.tracing.Span.Builder extract(C carrier, Getter<C> getter) {
                baggageSuppressed[0] = getter.get(carrier, "baggage") == null;
                if (failExtraction[0]) throw new IllegalStateException("propagator extraction failed");
                return delegate.extract(carrier, getter);
            }
        };
        PublicationTracer publicationTracer = new MicrometerPublicationTracer(tracer, propagator, objectMapper);

        var registration = tracer.nextSpan().name("registration").start();
        Map<String, String> registrationHeaders = new java.util.LinkedHashMap<>();
        propagator.inject(registration.context(), registrationHeaders,
                (carrier, key, value) -> carrier.put(key, value));
        String registeredSpanId = registration.context().spanId();
        String registeredTraceId = registration.context().traceId();
        registration.end();

        StoredEvent registeredEvent = storedEvent(registrationHeaders);
        String registeredHeadersJson = registeredEvent.headersJson();
        PublicationTracer.Attempt first = publicationTracer.begin(claimed(registeredEvent, 1));
        Map<String, String> firstHeaders = objectMapper.readValue(first.eventForSend().headersJson(),
                objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class));
        String firstSpanId = spanId(firstHeaders.get("traceparent"));
        assertThat(first.eventForSend()).satisfies(copy -> {
            assertThat(copy.id()).isEqualTo(registeredEvent.id());
            assertThat(copy.eventType()).isEqualTo(registeredEvent.eventType());
            assertThat(copy.eventKey()).isEqualTo(registeredEvent.eventKey());
            assertThat(copy.payloadJson()).isEqualTo(registeredEvent.payloadJson());
        });
        assertThat(registeredEvent.headersJson()).isEqualTo(registeredHeadersJson);
        first.sendSucceeded(new SendReceipt("message-1"));
        first.stateUpdated("PUBLISHED", false);
        first.close();

        PublicationTracer.Attempt second = publicationTracer.begin(claimed(registeredEvent, 2));
        Map<String, String> secondHeaders = objectMapper.readValue(second.eventForSend().headersJson(),
                objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class));
        String secondSpanId = spanId(secondHeaders.get("traceparent"));
        second.sendSucceeded(new SendReceipt("message-2"));
        second.close();

        var unrelated = tracer.nextSpan().name("unrelated").start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(unrelated)) {
            PublicationTracer.Attempt missing = publicationTracer.begin(claimed(storedEvent(Map.of()), 1));
            Map<String, String> missingHeaders = readHeaders(missing.eventForSend());
            String missingTraceId = traceId(missingHeaders.get("traceparent"));
            assertThat(missingTraceId).isNotEqualTo(unrelated.context().traceId());
            missing.close();
            assertThat(tracer.currentSpan().context().spanId()).isEqualTo(unrelated.context().spanId());

            PublicationTracer.Attempt malformed = publicationTracer.begin(claimed(storedEvent(
                    Map.of("traceparent", "broken")), 1));
            String malformedTraceId = traceId(readHeaders(malformed.eventForSend()).get("traceparent"));
            assertThat(malformedTraceId).isNotEqualTo(unrelated.context().traceId());
            malformed.close();

            PublicationTracer.Attempt zeroIds = publicationTracer.begin(claimed(storedEvent(
                    Map.of("traceparent", "00-00000000000000000000000000000000-0000000000000000-01")), 1));
            String zeroTraceId = traceId(readHeaders(zeroIds.eventForSend()).get("traceparent"));
            assertThat(zeroTraceId).isNotEqualTo(unrelated.context().traceId());
            zeroIds.close();
        } finally {
            unrelated.end();
        }

        assertThat(provider.forceFlush().join(10, TimeUnit.SECONDS).isSuccess()).isTrue();
        List<SpanData> spans = new ArrayList<>(exporter.spans);
        SpanData firstAttempt = findSpan(spans, firstSpanId);
        SpanData secondAttempt = findSpan(spans, secondSpanId);
        assertThat(firstSpanId).isNotEqualTo(secondSpanId);
        assertThat(firstAttempt.getTraceId()).isEqualTo(registeredTraceId);
        assertThat(secondAttempt.getTraceId()).isEqualTo(registeredTraceId);
        assertThat(firstAttempt.getParentSpanId()).isEqualTo(registeredSpanId);
        assertThat(secondAttempt.getParentSpanId()).isEqualTo(registeredSpanId);
        assertThat(baggageSuppressed[0]).isTrue();
        assertThat(readHeaders(registeredEvent).get("traceparent"))
                .contains(registeredSpanId);

        failExtraction[0] = true;
        PublicationTracer.Attempt extractionFailure = publicationTracer.begin(claimed(
                storedEvent(registrationHeaders), 1));
        Map<String, String> extractionFallbackHeaders = readHeaders(extractionFailure.eventForSend());
        String extractionFallbackTraceId = traceId(extractionFallbackHeaders.get("traceparent"));
        String extractionFallbackSpanId = spanId(extractionFallbackHeaders.get("traceparent"));
        extractionFailure.close();
        failExtraction[0] = false;
        assertThat(extractionFallbackTraceId).isNotEqualTo(registeredTraceId);

        StoredEvent injectionFailureInput = storedEvent(registrationHeaders);
        failInjection[0] = true;
        PublicationTracer.Attempt injectionFailure = publicationTracer.begin(claimed(injectionFailureInput, 1));
        assertThat(injectionFailure.eventForSend()).isSameAs(injectionFailureInput);
        injectionFailure.close();
        failInjection[0] = false;

        assertThat(provider.forceFlush().join(10, TimeUnit.SECONDS).isSuccess()).isTrue();
        assertThat(findSpan(new ArrayList<>(exporter.spans), extractionFallbackSpanId)
                .getParentSpanContext().isValid()).isFalse();
        sdk.close();
    }

    private StoredEvent storedEvent(Map<String, String> headers) throws Exception {
        return new StoredEvent(new EventId(51), "test.event", "same-key", "{\"payload\":true}",
                objectMapper.writeValueAsString(headers));
    }

    private ClaimedEvent claimed(StoredEvent event, int attempt) {
        return new ClaimedEvent(event, attempt, attempt, 8, "worker",
                java.time.Instant.now().plusSeconds(30));
    }

    private Map<String, String> readHeaders(StoredEvent event) {
        try {
            return objectMapper.readValue(event.headersJson(),
                    objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static String traceId(String traceparent) { return traceparent.substring(3, 35); }
    private static String spanId(String traceparent) { return traceparent.substring(36, 52); }

    private static SpanData findSpan(List<SpanData> spans, String spanId) {
        return spans.stream().filter(span -> span.getSpanId().equals(spanId)).findFirst().orElseThrow();
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
}
