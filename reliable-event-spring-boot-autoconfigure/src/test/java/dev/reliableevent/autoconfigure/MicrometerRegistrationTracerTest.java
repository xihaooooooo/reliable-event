package dev.reliableevent.autoconfigure;

import dev.reliableevent.jdbc.internal.tracing.RegistrationTracer;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MicrometerRegistrationTracerTest {

    @Test
    void exactHeaderCountBudgetReplacesExistingW3cFieldsAndDropsBaggageInjection() {
        Harness harness = new Harness();
        Map<String, String> original = headers(62);
        original.put("TraceParent", "untrusted-parent");
        original.put("tracestate", "untrusted-state");
        harness.injection = (carrier, setter) -> {
            setter.set(carrier, "traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
            setter.set(carrier, "tracestate", "vendor=value");
            setter.set(carrier, "baggage", "secret=value");
        };

        RegistrationTracer.Registration registration = harness.tracer.begin(original);
        Map<String, String> result = registration.headers();
        assertThat(result).hasSize(64);
        assertThat(result).containsEntry("traceparent",
                "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
        assertThat(result).containsEntry("tracestate", "vendor=value");
        assertThat(result).doesNotContainKey("TraceParent").doesNotContainKey("baggage");
        assertThat(original).containsEntry("TraceParent", "untrusted-parent");
        registration.close();
    }

    @Test
    void headerBudgetOverflowAndInjectionFailurePreserveOriginalHeaders() {
        Harness harness = new Harness();
        Map<String, String> full = headers(64);
        RegistrationTracer.Registration tooMany = harness.tracer.begin(full);
        assertThat(tooMany.headers()).isEqualTo(full);
        tooMany.close();

        Map<String, String> small = Map.of("business", "value", "traceparent", "caller-value");
        harness.failInjection = true;
        RegistrationTracer.Registration failed = harness.tracer.begin(small);
        assertThat(failed.headers()).isSameAs(small);
        failed.close();
    }

    @Test
    void maximumHeaderValueFitsButOneByteOverAndTotalBudgetOverflowAreSkipped() {
        Harness harness = new Harness();
        harness.injection = (carrier, setter) -> setter.set(carrier, "tracestate", "x".repeat(4096));
        RegistrationTracer.Registration maximumValue = harness.tracer.begin(Map.of());
        assertThat(maximumValue.headers().get("tracestate")).hasSize(4096);
        maximumValue.close();

        harness.injection = (carrier, setter) -> setter.set(carrier, "tracestate", "x".repeat(4097));
        RegistrationTracer.Registration valueOverflow = harness.tracer.begin(Map.of("business", "kept"));
        assertThat(valueOverflow.headers()).containsExactly(Map.entry("business", "kept"));
        valueOverflow.close();

        Map<String, String> nearTotalLimit = new LinkedHashMap<>();
        for (int index = 0; index < 4; index++) {
            nearTotalLimit.put("a" + index, "x".repeat(4080));
        }
        assertThat(dev.reliableevent.internal.headers.EventHeaderConstraints.validationError(nearTotalLimit))
                .isNull();
        harness.injection = (carrier, setter) -> setter.set(carrier, "traceparent",
                "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
        RegistrationTracer.Registration totalOverflow = harness.tracer.begin(nearTotalLimit);
        assertThat(totalOverflow.headers()).isEqualTo(nearTotalLimit);
        totalOverflow.close();
    }

    @Test
    void traceRecordingAndScopeCloseFailuresAreContained() {
        Harness harness = new Harness();
        Tracer.SpanInScope scope = mock(Tracer.SpanInScope.class);
        when(harness.micrometerTracer.withSpan(harness.span)).thenReturn(scope);
        doThrow(new IllegalStateException("tag failed")).when(harness.span).tag(anyString(), anyString());
        doThrow(new IllegalStateException("error failed")).when(harness.span).error(any(Throwable.class));
        doThrow(new IllegalStateException("close failed")).when(scope).close();

        RegistrationTracer.Registration registration = harness.tracer.begin(Map.of());
        registration.succeeded(new dev.reliableevent.EventId(4));
        registration.failed(new IllegalStateException("business failure"));
        org.assertj.core.api.Assertions.assertThatCode(registration::close).doesNotThrowAnyException();
    }

    private static Map<String, String> headers(int count) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < count; index++) {
            values.put("h" + index, "v");
        }
        return values;
    }

    private static final class Harness {
        private final Tracer micrometerTracer = mock(Tracer.class);
        private final Propagator propagator = mock(Propagator.class);
        private final Span span = mock(Span.class);
        private boolean failInjection;
        private Injection injection = (carrier, setter) -> {
            setter.set(carrier, "traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
        };
        private final RegistrationTracer tracer;

        private Harness() {
            when(span.name(anyString())).thenReturn(span);
            when(span.start()).thenReturn(span);
            when(span.context()).thenReturn(mock(TraceContext.class));
            when(micrometerTracer.nextSpan()).thenReturn(span);
            when(micrometerTracer.withSpan(span)).thenReturn(mock(Tracer.SpanInScope.class));
            doAnswer(invocation -> {
                if (failInjection) {
                    throw new IllegalStateException("injection failed");
                }
                Object carrier = invocation.getArgument(1);
                Propagator.Setter<Object> setter = invocation.getArgument(2);
                injection.apply(carrier, setter);
                return null;
            }).when(propagator).inject(any(TraceContext.class), any(), any());
            tracer = new MicrometerRegistrationTracer(micrometerTracer, propagator);
        }
    }

    @FunctionalInterface
    private interface Injection {
        void apply(Object carrier, Propagator.Setter<Object> setter);
    }
}
