package dev.reliableevent.autoconfigure;

import dev.reliableevent.jdbc.internal.tracing.RegistrationTracer;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.util.Map;

@AutoConfiguration(after = ReliableEventAutoConfiguration.class, afterName = {
        "org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.tracing.OpenTelemetryTracingAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.opentelemetry.OpenTelemetryAutoConfiguration"
})
@ConditionalOnProperty(prefix = "reliable-event", name = "tracing-enabled",
        havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(prefix = "reliable-event", name = "enabled",
        havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({Tracer.class, Propagator.class, RegistrationTracer.class})
@ConditionalOnBean({Tracer.class, Propagator.class})
public class ReliableEventTracingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RegistrationTracer.class)
    RegistrationTracer reliableEventRegistrationTracer(Tracer tracer, Propagator propagator) {
        return new MicrometerRegistrationTracer(tracer, propagator);
    }

    @Bean
    @ConditionalOnMissingBean(PublicationTracer.class)
    @ConditionalOnBean(ObjectMapper.class)
    PublicationTracer reliableEventPublicationTracer(Tracer tracer, Propagator propagator,
                                                     ObjectMapper objectMapper) {
        return new MicrometerPublicationTracer(tracer, propagator, objectMapper);
    }
}

final class MicrometerRegistrationTracer implements RegistrationTracer {
    private static final System.Logger LOGGER = System.getLogger(MicrometerRegistrationTracer.class.getName());
    private static final String TRACEPARENT = "traceparent";
    private static final String TRACESTATE = "tracestate";
    private final Tracer tracer;
    private final Propagator propagator;

    MicrometerRegistrationTracer(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Override
    public Registration begin(Map<String, String> headers) {
        Span span;
        try {
            // The active request context is authoritative; caller supplied trace headers are
            // replaced after span creation and are never trusted as this span's parent.
            span = tracer.nextSpan().name("reliable-event.register").start();
        } catch (RuntimeException spanFailure) {
            warn("Unable to start registration trace span");
            throw spanFailure;
        }

        Tracer.SpanInScope scope;
        try {
            scope = tracer.withSpan(span);
        } catch (RuntimeException scopeFailure) {
            endSafely(span);
            throw scopeFailure;
        }

        Map<String, String> enriched = inject(headers, span);
        return new Registration() {
            private boolean closed;

            @Override
            public Map<String, String> headers() {
                return enriched;
            }

            @Override
            public void succeeded(dev.reliableevent.EventId eventId) {
                try {
                    span.tag("reliable_event.id", Long.toString(eventId.value()));
                } catch (RuntimeException failure) {
                    warn("Unable to record registration trace result");
                }
            }

            @Override
            public void failed(Throwable failure) {
                try {
                    span.error(failure);
                } catch (RuntimeException traceFailure) {
                    warn("Unable to record registration trace failure");
                }
            }

            @Override
            public void close() {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    scope.close();
                } catch (RuntimeException scopeFailure) {
                    warn("Unable to close registration trace scope");
                } finally {
                    endSafely(span);
                }
            }
        };
    }

    private Map<String, String> inject(Map<String, String> original, Span span) {
        try {
            Map<String, String> generated = new java.util.LinkedHashMap<>();
            propagator.inject(span.context(), generated, (carrier, key, value) -> {
                if (isW3cField(key) && value != null && !value.isBlank()) {
                    carrier.put(key.toLowerCase(java.util.Locale.ROOT), value);
                }
            });
            if (generated.isEmpty()) {
                return original;
            }
            Map<String, String> merged = new java.util.LinkedHashMap<>(original);
            merged.keySet().removeIf(key -> key.equalsIgnoreCase(TRACEPARENT)
                    || key.equalsIgnoreCase(TRACESTATE));
            merged.putAll(generated);
            if (dev.reliableevent.internal.headers.EventHeaderConstraints.validationError(merged) != null) {
                warn("Registration trace headers exceed the event header budget");
                return original;
            }
            return Map.copyOf(merged);
        } catch (RuntimeException injectionFailure) {
            warn("Unable to inject registration trace context");
            return original;
        }
    }

    private static boolean isW3cField(String key) {
        return key != null && (key.equalsIgnoreCase(TRACEPARENT) || key.equalsIgnoreCase(TRACESTATE));
    }

    private static void endSafely(Span span) {
        try {
            span.end();
        } catch (RuntimeException endFailure) {
            warn("Unable to close registration trace span");
        }
    }

    private static void warn(String message) {
        LOGGER.log(System.Logger.Level.WARNING, message);
    }
}
