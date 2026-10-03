package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.headers.EventHeaderConstraints;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** Micrometer implementation of the JDBC publication-attempt tracing seam. */
final class MicrometerPublicationTracer implements PublicationTracer {
    private static final System.Logger LOGGER = System.getLogger(MicrometerPublicationTracer.class.getName());
    private static final Pattern W3C_TRACEPARENT = Pattern.compile(
            "00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
    private static final String TRACEPARENT = "traceparent";
    private static final String TRACESTATE = "tracestate";

    private final Tracer tracer;
    private final Propagator propagator;
    private final ObjectMapper objectMapper;

    MicrometerPublicationTracer(Tracer tracer, Propagator propagator, ObjectMapper objectMapper) {
        this.tracer = tracer;
        this.propagator = propagator;
        this.objectMapper = objectMapper;
    }

    @Override
    public Attempt begin(ClaimedEvent claimedEvent) {
        Map<String, String> parsedHeaders = parseHeaders(claimedEvent.event().headersJson());
        boolean headersParseable = parsedHeaders != null;
        Map<String, String> storedHeaders = headersParseable ? parsedHeaders : Map.of();
        Span span = startAttempt(claimedEvent, storedHeaders);
        final Tracer.SpanInScope scope;
        try {
            scope = tracer.withSpan(span);
        } catch (RuntimeException failure) {
            endSafely(span);
            throw failure;
        }
        StoredEvent sendEvent = headersParseable
                ? injectAttemptContext(claimedEvent.event(), storedHeaders, span)
                : claimedEvent.event();
        return new Attempt() {
            private boolean closed;

            @Override public StoredEvent eventForSend() { return sendEvent; }

            @Override
            public void sendSucceeded(SendReceipt receipt) {
                safely(() -> {
                    span.tag("reliable_event.send.result", "success");
                    if (receipt.messageId() != null) {
                        span.tag("reliable_event.message_id", receipt.messageId());
                    }
                    span.event("reliable_event.sender.acknowledged");
                }, "Unable to record successful sender receipt");
            }

            @Override
            public void sendFailed(EventSendFailureType type, Throwable failure) {
                safely(() -> {
                    span.tag("reliable_event.send.result", sendResult(type));
                    span.error(failure);
                    span.event("reliable_event.sender.failed");
                }, "Unable to record sender failure");
            }

            @Override
            public void stateUpdated(String targetStatus, boolean commitPending) {
                safely(() -> {
                    span.tag("reliable_event.state_update.result", "executed");
                    span.tag("reliable_event.state_update.target", targetStatus);
                    span.tag("reliable_event.state_update.commit", commitPending ? "pending" : "completed");
                    span.event("reliable_event.state_update.executed");
                }, "Unable to record state update result");
            }

            @Override
            public void stateUpdateFailed(String targetStatus, Throwable failure, boolean ownershipRejected) {
                safely(() -> {
                    span.tag("reliable_event.state_update.result", ownershipRejected ? "ownership_rejected" : "failed");
                    span.tag("reliable_event.state_update.target", targetStatus);
                    span.error(failure);
                    span.event("reliable_event.state_update.failed");
                }, "Unable to record state update failure");
            }

            @Override
            public void close() {
                if (closed) return;
                closed = true;
                try {
                    scope.close();
                } catch (RuntimeException failure) {
                    warn("Unable to close publication trace scope");
                } finally {
                    endSafely(span);
                }
            }
        };
    }

    private Span startAttempt(ClaimedEvent claimedEvent, Map<String, String> headers) {
        try {
            Span.Builder builder = propagator.extract(headers,
                    new Propagator.Getter<Map<String, String>>() {
                        @Override public String get(Map<String, String> carrier, String key) {
                            return isW3cField(key) ? valueIgnoreCase(carrier, key) : null;
                        }
                    });
            if (!hasSupportedTraceParent(headers)) {
                builder.setNoParent();
            }
            return builder.name("reliable-event.publish")
                    .tag("reliable_event.id", Long.toString(claimedEvent.event().id().value()))
                    .tag("reliable_event.attempt", Integer.toString(claimedEvent.attemptCount()))
                    .start();
        } catch (RuntimeException extractionFailure) {
            warn("Unable to extract publication parent; starting a root attempt span");
            return tracer.spanBuilder().setNoParent().name("reliable-event.publish")
                    .tag("reliable_event.id", Long.toString(claimedEvent.event().id().value()))
                    .tag("reliable_event.attempt", Integer.toString(claimedEvent.attemptCount()))
                    .start();
        }
    }

    private Map<String, String> parseHeaders(String headersJson) {
        try {
            JsonNode root = objectMapper.readTree(headersJson);
            if (root == null || !root.isObject()) return null;
            Map<String, String> result = new LinkedHashMap<>();
            var fields = root.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                if (!entry.getValue().isTextual()) return null;
                result.put(entry.getKey(), entry.getValue().textValue());
            }
            return Map.copyOf(result);
        } catch (RuntimeException | JsonProcessingException failure) {
            warn("Unable to read stored trace context; starting a root attempt span");
            return null;
        }
    }

    private StoredEvent injectAttemptContext(StoredEvent original, Map<String, String> headers, Span span) {
        if (EventHeaderConstraints.validationError(headers) != null) {
            warn("Stored publication headers exceed the event header budget");
            return original;
        }
        try {
            Map<String, String> generated = new LinkedHashMap<>();
            propagator.inject(span.context(), generated, (carrier, key, value) -> {
                if (isW3cField(key) && value != null && !value.isBlank()) {
                    carrier.put(key.toLowerCase(Locale.ROOT), value);
                }
            });
            if (generated.isEmpty()) return original;
            Map<String, String> merged = new LinkedHashMap<>(headers);
            merged.keySet().removeIf(key -> key.equalsIgnoreCase(TRACEPARENT)
                    || key.equalsIgnoreCase(TRACESTATE));
            merged.putAll(generated);
            if (EventHeaderConstraints.validationError(merged) != null) {
                warn("Publication trace headers exceed the event header budget");
                return original;
            }
            return new StoredEvent(original.id(), original.eventType(), original.eventKey(),
                    original.payloadJson(), objectMapper.writeValueAsString(merged));
        } catch (RuntimeException | JsonProcessingException failure) {
            warn("Unable to inject publication trace context");
            return original;
        }
    }

    private static boolean hasSupportedTraceParent(Map<String, String> headers) {
        String value = valueIgnoreCase(headers, TRACEPARENT);
        if (value == null || !W3C_TRACEPARENT.matcher(value).matches()) return false;
        String traceId = value.substring(3, 35);
        String spanId = value.substring(36, 52);
        return !traceId.equals("00000000000000000000000000000000")
                && !spanId.equals("0000000000000000");
    }

    private static String valueIgnoreCase(Map<String, String> headers, String key) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(key)) return entry.getValue();
        }
        return null;
    }

    private static boolean isW3cField(String key) {
        return key != null && (key.equalsIgnoreCase(TRACEPARENT) || key.equalsIgnoreCase(TRACESTATE));
    }

    private static String sendResult(EventSendFailureType type) {
        return switch (type) {
            case RETRYABLE -> "definite_failure";
            case NON_RETRYABLE -> "definite_failure";
            case RESULT_UNKNOWN -> "unknown";
        };
    }

    private static void safely(Runnable action, String message) {
        try { action.run(); } catch (RuntimeException failure) { warn(message); }
    }

    private static void endSafely(Span span) {
        try { span.end(); } catch (RuntimeException failure) { warn("Unable to end publication trace span"); }
    }

    private static void warn(String message) {
        LOGGER.log(System.Logger.Level.WARNING, message);
    }
}
