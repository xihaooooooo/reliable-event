package dev.reliableevent.kafka.example;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerTracingTest {
    @Test
    void extractsMixedCaseParentAndCreatesDistinctConsumerAttemptsInOneTrace() {
        try (TracingFixture fixture = new TracingFixture()) {
            var tracing = new ExampleConsumerLoop.ConsumerTracing(fixture.tracer, fixture.propagator);
            String traceId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            String firstParent = "bbbbbbbbbbbbbbbb";
            String secondParent = "cccccccccccccccc";

            var first = tracing.begin(record("TrAcEpArEnT", "00-" + traceId + "-" + firstParent + "-01"));
            String firstSpanId = fixture.tracer.currentSpan().context().spanId();
            assertThat(fixture.tracer.currentSpan().context().traceId()).isEqualTo(traceId);
            first.processed(true);
            first.offsetCommitted();
            first.close();
            assertThat(fixture.tracer.currentSpan()).isNull();

            var second = tracing.begin(record("traceparent", "00-" + traceId + "-" + secondParent + "-01"));
            String secondSpanId = fixture.tracer.currentSpan().context().spanId();
            assertThat(fixture.tracer.currentSpan().context().traceId()).isEqualTo(traceId);
            second.processed(false);
            second.offsetCommitFailed(new IllegalStateException("injected offset commit failure"));
            second.close();
            assertThat(fixture.tracer.currentSpan()).isNull();

            assertThat(firstSpanId).isNotEqualTo(secondSpanId);
            assertThat(fixture.ended).hasSize(2);
            assertThat(fixture.ended.get(0).getParentSpanId()).isEqualTo(firstParent);
            assertThat(fixture.ended.get(1).getParentSpanId()).isEqualTo(secondParent);
            assertThat(fixture.ended).allSatisfy(span -> assertThat(span.getTraceId()).isEqualTo(traceId));
            assertThat(fixture.ended.get(1).getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                    "reliable_event.consume.result"))).isEqualTo("idempotent_skip");
            assertThat(fixture.ended.get(1).getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                    "reliable_event.offset_commit.result"))).isEqualTo("failed");
        }
    }

    @Test
    void missingAndInvalidParentsCreateIndependentRootsWithoutThreadContextLeak() {
        try (TracingFixture fixture = new TracingFixture()) {
            var tracing = new ExampleConsumerLoop.ConsumerTracing(fixture.tracer, fixture.propagator);
            var invalid = tracing.begin(record("traceparent", "not-a-w3c-parent"));
            String invalidRoot = fixture.tracer.currentSpan().context().traceId();
            invalid.close();
            assertThat(fixture.tracer.currentSpan()).isNull();

            var missing = tracing.begin(record(null, null));
            String missingRoot = fixture.tracer.currentSpan().context().traceId();
            missing.close();
            assertThat(fixture.tracer.currentSpan()).isNull();

            assertThat(invalidRoot).isNotEqualTo("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    .isNotEqualTo("00000000000000000000000000000000");
            assertThat(missingRoot).isNotEqualTo(invalidRoot)
                    .isNotEqualTo("00000000000000000000000000000000");
            assertThat(fixture.ended).hasSize(2);
            assertThat(fixture.ended).allSatisfy(span ->
                    assertThat(span.getParentSpanId()).isEqualTo("0000000000000000"));
        }
    }

    private static ConsumerRecord<String, byte[]> record(String name, String value) {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("orders", 0, 0, "1", new byte[]{1});
        if (name != null) ((RecordHeaders) record.headers()).add(name,
                value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return record;
    }

    static final class TracingFixture implements AutoCloseable {
        final List<SpanData> ended = new CopyOnWriteArrayList<>();
        private final SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(new SpanProcessor() {
                    @Override public void onStart(Context parentContext, ReadWriteSpan span) { }
                    @Override public boolean isStartRequired() { return false; }
                    @Override public void onEnd(ReadableSpan span) { ended.add(span.toSpanData()); }
                    @Override public boolean isEndRequired() { return true; }
                }).build();
        private final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        private final io.opentelemetry.api.trace.Tracer otelTracer = sdk.getTracer("kafka-example-consumer-test");
        final Tracer tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), ignored -> { });
        final io.micrometer.tracing.propagation.Propagator propagator =
                new OtelPropagator(sdk.getPropagators(), otelTracer);

        @Override public void close() { provider.close(); }
    }
}
