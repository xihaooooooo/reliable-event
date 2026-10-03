package dev.reliableevent.example;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExampleConsumerTracingTest {

    @Test
    void missingAndInvalidParentsStartExplicitRootsAndStillProcessAndAcknowledge() throws Exception {
        for (Map<String, String> properties : List.of(
                Map.of("reliable_event_id", "1"),
                Map.of("reliable_event_id", "2", "TrAcEpArEnT",
                        "00-00000000000000000000000000000000-bbbbbbbbbbbbbbbb-01"))) {
            SpanHarness spans = new SpanHarness();
            Run run = start(properties, spans.tracer, spans.propagator);

            assertThat(run.acknowledged.await(5, TimeUnit.SECONDS)).isTrue();
            run.loop.stop();

            verify(spans.tracer).spanBuilder();
            verify(spans.builder).setNoParent();
            verify(spans.propagator, never()).extract(any(), any());
            verify(spans.scope).close();
            verify(run.handler).handle(run.message);
            verify(run.consumer).ack(run.message);
        }
    }

    @Test
    void validW3cContextExtractsOnlyTraceFieldsAndClosesScope() throws Exception {
        Map<String, String> properties = Map.of(
                "reliable_event_id", "3",
                "TrAcEpArEnT", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "TrAcEsTaTe", "vendor=value",
                "baggage", "secret=value");
        SpanHarness spans = new SpanHarness();
        when(spans.propagator.extract(eq(properties), any())).thenReturn(spans.builder);
        Run run = start(properties, spans.tracer, spans.propagator);

        assertThat(run.acknowledged.await(5, TimeUnit.SECONDS)).isTrue();
        run.loop.stop();

        var getter = org.mockito.ArgumentCaptor.forClass(Propagator.Getter.class);
        verify(spans.propagator).extract(eq(properties), getter.capture());
        assertThat(getter.getValue().get(properties, "traceparent"))
                .isEqualTo("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
        assertThat(getter.getValue().get(properties, "tracestate")).isEqualTo("vendor=value");
        assertThat(getter.getValue().get(properties, "baggage")).isNull();
        verify(spans.builder, never()).setNoParent();
        verify(spans.scope).close();
        verify(run.consumer).ack(run.message);
    }

    @Test
    void tracerStartRecordAndCloseFailuresDoNotChangeBusinessOrAck() throws Exception {
        Map<String, String> properties = Map.of(
                "reliable_event_id", "4",
                "traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
        SpanHarness spans = new SpanHarness();
        when(spans.propagator.extract(eq(properties), any())).thenReturn(spans.builder);
        when(spans.span.tag(anyString(), anyString())).thenThrow(new IllegalStateException("record failed"));
        doAnswer(invocation -> { throw new IllegalStateException("scope close failed"); })
                .when(spans.scope).close();
        when(spans.span.start()).thenReturn(spans.span);
        Run run = start(properties, spans.tracer, spans.propagator);

        assertThat(run.acknowledged.await(5, TimeUnit.SECONDS)).isTrue();
        run.loop.stop();

        verify(run.handler).handle(run.message);
        verify(run.consumer).ack(run.message);
        verify(spans.scope).close();
        verify(spans.span).end();

        SpanHarness unavailable = new SpanHarness();
        when(unavailable.tracer.spanBuilder()).thenThrow(new IllegalStateException("span start failed"));
        Run noSpan = start(Map.of("reliable_event_id", "4"),
                unavailable.tracer, unavailable.propagator);
        assertThat(noSpan.acknowledged.await(5, TimeUnit.SECONDS)).isTrue();
        noSpan.loop.stop();
        verify(noSpan.handler).handle(noSpan.message);
        verify(noSpan.consumer).ack(noSpan.message);
    }

    @Test
    void realOtelScopesRestoreAmbientContextAfterSuccessAndBusinessFailure() throws Exception {
        MemoryExporter exporter = new MemoryExporter();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        io.opentelemetry.api.trace.Tracer otelTracer = openTelemetry.getTracer("example-test");
        Tracer tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), ignored -> { });
        Propagator propagator = new OtelPropagator(openTelemetry.getPropagators(), otelTracer);
        OrderMessageHandler handler = mock(OrderMessageHandler.class);
        SimpleConsumer consumer = mock(SimpleConsumer.class);
        MessageView success = mock(MessageView.class);
        MessageView failure = mock(MessageView.class);
        when(success.getProperties()).thenReturn(Map.of("reliable_event_id", "6"));
        when(failure.getProperties()).thenReturn(Map.of("reliable_event_id", "7", "traceparent", "invalid"));
        when(handler.handle(success)).thenReturn(true);
        when(handler.handle(failure)).thenThrow(new IllegalStateException("business failure"));
        ExampleConsumerLoop loop = new ExampleConsumerLoop(consumer, handler, tracer, propagator);
        Method consume = ExampleConsumerLoop.class.getDeclaredMethod("consume", MessageView.class);
        consume.setAccessible(true);
        Span ambient = tracer.nextSpan().name("ambient").start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(ambient)) {
            String ambientId = tracer.currentSpan().context().spanId();
            consume.invoke(loop, success);
            assertThat(tracer.currentSpan().context().spanId()).isEqualTo(ambientId);
            consume.invoke(loop, failure);
            assertThat(tracer.currentSpan().context().spanId()).isEqualTo(ambientId);
        } finally {
            ambient.end();
            provider.close();
        }
        assertThat(tracer.currentSpan()).isNull();
        assertThat(exporter.spans()).filteredOn(span -> span.getName().equals("example.order.consume"))
                .hasSize(2).allSatisfy(span -> {
                    assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
                    assertThat(span.getParentSpanId()).isEqualTo("0000000000000000");
                });
        verify(consumer).ack(success);
        verify(consumer, never()).ack(failure);
    }

    @Test
    void disabledTracingConfigurationDoesNotResolveOptionalTracingBeans() throws Exception {
        SpanHarness spans = new SpanHarness();
        @SuppressWarnings("unchecked") ObjectProvider<Tracer> tracers = mock(ObjectProvider.class);
        @SuppressWarnings("unchecked") ObjectProvider<Propagator> propagators = mock(ObjectProvider.class);
        SimpleConsumer consumer = mock(SimpleConsumer.class);
        OrderMessageHandler handler = mock(OrderMessageHandler.class);
        ExampleConsumerLoop loop = new ExampleConsumerConfiguration().exampleConsumerLoop(
                consumer, handler, tracers, propagators, false);
        Run run = start(loop, consumer, handler, Map.of("reliable_event_id", "8",
                "traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"));

        assertThat(run.acknowledged.await(5, TimeUnit.SECONDS)).isTrue();
        run.loop.stop();

        verify(tracers, never()).getIfAvailable();
        verify(propagators, never()).getIfAvailable();
        verify(spans.tracer, never()).spanBuilder();
        verify(run.handler).handle(run.message);
        verify(run.consumer).ack(run.message);
    }

    @Test
    void existingConstructorRunsWithoutAnyTracer() throws Exception {
        Run run = start(Map.of("reliable_event_id", "5"), null, null);

        assertThat(run.acknowledged.await(5, TimeUnit.SECONDS)).isTrue();
        run.loop.stop();

        verify(run.handler).handle(run.message);
        verify(run.consumer).ack(run.message);
    }

    private static Run start(Map<String, String> properties, Tracer tracer, Propagator propagator)
            throws Exception {
        SimpleConsumer consumer = mock(SimpleConsumer.class);
        OrderMessageHandler handler = mock(OrderMessageHandler.class);
        MessageView message = mock(MessageView.class);
        ExampleConsumerLoop loop = tracer == null
                ? new ExampleConsumerLoop(consumer, handler)
                : new ExampleConsumerLoop(consumer, handler, tracer, propagator);
        return start(loop, consumer, handler, message, properties);
    }

    private static Run start(ExampleConsumerLoop loop, SimpleConsumer consumer,
                             OrderMessageHandler handler, Map<String, String> properties) throws Exception {
        return start(loop, consumer, handler, mock(MessageView.class), properties);
    }

    private static Run start(ExampleConsumerLoop loop, SimpleConsumer consumer,
                             OrderMessageHandler handler, MessageView message,
                             Map<String, String> properties) throws Exception {
        CountDownLatch acknowledged = new CountDownLatch(1);
        AtomicInteger receives = new AtomicInteger();
        when(handler.handle(message)).thenReturn(true);
        when(message.getProperties()).thenReturn(properties);
        when(consumer.receive(anyInt(), any(Duration.class))).thenAnswer(invocation -> {
            if (receives.getAndIncrement() == 0) return List.of(message);
            new CountDownLatch(1).await();
            return List.of();
        });
        doAnswer(invocation -> {
            acknowledged.countDown();
            return null;
        }).when(consumer).ack(message);
        loop.start();
        return new Run(loop, consumer, handler, message, acknowledged);
    }

    private record Run(ExampleConsumerLoop loop, SimpleConsumer consumer,
                       OrderMessageHandler handler, MessageView message,
                       CountDownLatch acknowledged) { }

    private static final class SpanHarness {
        private final Tracer tracer = mock(Tracer.class);
        private final Propagator propagator = mock(Propagator.class);
        private final Span.Builder builder = mock(Span.Builder.class);
        private final Span span = mock(Span.class);
        private final Tracer.SpanInScope scope = mock(Tracer.SpanInScope.class);

        private SpanHarness() {
            when(tracer.spanBuilder()).thenReturn(builder);
            when(tracer.withSpan(span)).thenReturn(scope);
            when(builder.setNoParent()).thenReturn(builder);
            when(builder.name(anyString())).thenReturn(builder);
            when(builder.kind(any())).thenReturn(builder);
            when(builder.tag(anyString(), anyString())).thenReturn(builder);
            when(builder.start()).thenReturn(span);
            when(span.tag(anyString(), anyString())).thenReturn(span);
            when(span.event(anyString())).thenReturn(span);
            when(span.error(any())).thenReturn(span);
        }
    }

    private static final class MemoryExporter implements SpanExporter {
        private final CopyOnWriteArrayList<SpanData> spans = new CopyOnWriteArrayList<>();

        List<SpanData> spans() { return List.copyOf(spans); }
        @Override public CompletableResultCode export(Collection<SpanData> batch) {
            spans.addAll(batch);
            return CompletableResultCode.ofSuccess();
        }
        @Override public CompletableResultCode flush() { return CompletableResultCode.ofSuccess(); }
        @Override public CompletableResultCode shutdown() { return CompletableResultCode.ofSuccess(); }
    }
}
