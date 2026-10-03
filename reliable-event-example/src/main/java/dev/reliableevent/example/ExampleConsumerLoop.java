package dev.reliableevent.example;

import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.concurrent.atomic.AtomicBoolean;

/** Example-only consumer; all business writes happen before acknowledging the message. */
final class ExampleConsumerLoop implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleConsumerLoop.class);

    private final SimpleConsumer consumer;
    private final OrderMessageHandler handler;
    private final ConsumerTracing tracing;
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread thread;

    ExampleConsumerLoop(SimpleConsumer consumer, OrderMessageHandler handler) {
        this(consumer, handler, null, null);
    }

    ExampleConsumerLoop(SimpleConsumer consumer, OrderMessageHandler handler,
                       Tracer tracer, Propagator propagator) {
        this.consumer = Objects.requireNonNull(consumer);
        this.handler = Objects.requireNonNull(handler);
        this.tracing = new ConsumerTracing(tracer, propagator);
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        thread = new Thread(this::poll, "reliable-event-example-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void poll() {
        while (running.get()) {
            try {
                List<MessageView> messages = consumer.receive(8, Duration.ofSeconds(15));
                for (MessageView message : messages) {
                    if (!running.get()) {
                        return;
                    }
                    consume(message);
                }
            } catch (Exception failure) {
                if (running.get()) {
                    LOG.warn("event=example.order.receive_failed exceptionType={} message={}",
                            failure.getClass().getName(), failure.getMessage());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    private void consume(MessageView message) {
        ConsumerTracing.Scope trace = tracing.begin(message);
        try (trace) {
            boolean applied;
            try {
                // The Spring transaction proxy has committed before handle returns here.
                applied = handler.handle(message);
                trace.processed(applied);
            } catch (Exception failure) {
                trace.processingFailed(failure);
                LOG.warn("event=example.order.consume_failed exceptionType={} message={}",
                        failure.getClass().getName(), failure.getMessage());
                return;
            }

            try {
                consumer.ack(message);
                trace.acknowledged();
                LOG.info("event=example.order.consumed eventId={} applied={}",
                        property(message, "reliable_event_id"), applied);
            } catch (Exception failure) {
                trace.acknowledgementFailed(failure);
                LOG.warn("event=example.order.ack_failed exceptionType={} message={}",
                        failure.getClass().getName(), failure.getMessage());
            }
        }
    }

    @Override
    public synchronized void stop() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 200;
    }

    /** Small example-only W3C consumer tracer. Tracing failures never affect processing or ACK. */
    private static final class ConsumerTracing {
        private static final System.Logger LOGGER = System.getLogger(ConsumerTracing.class.getName());
        private static final Pattern TRACEPARENT = Pattern.compile(
                "00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
        private final Tracer tracer;
        private final Propagator propagator;

        private ConsumerTracing(Tracer tracer, Propagator propagator) {
            this.tracer = tracer;
            this.propagator = propagator;
        }

        Scope begin(MessageView message) {
            if (tracer == null || propagator == null) return Scope.NOOP;
            Span span = null;
            Tracer.SpanInScope scope = null;
            try {
                Map<String, String> properties = message.getProperties();
                boolean validParent = validTraceParent(valueIgnoreCase(properties, "traceparent"));
                Span.Builder builder = validParent
                        ? propagator.extract(properties, (carrier, key) ->
                                isTraceField(key) ? valueIgnoreCase(carrier, key) : null)
                        : tracer.spanBuilder().setNoParent();
                span = builder.name("example.order.consume")
                        .kind(Span.Kind.CONSUMER)
                        .tag("reliable_event.consume.result", "started")
                        .tag("reliable_event.message_id", safeMessageId(message))
                        .start();
                scope = tracer.withSpan(span);
                return new Scope(span, scope);
            } catch (RuntimeException failure) {
                warn("Unable to start consumer span; continuing without tracing");
                closeSafely(scope);
                endSafely(span);
                return Scope.NOOP;
            }
        }

        private static String safeMessageId(MessageView message) {
            try { return message.getMessageId().toString(); }
            catch (RuntimeException failure) { return "unknown"; }
        }

        private static boolean validTraceParent(String value) {
            if (value == null || !TRACEPARENT.matcher(value).matches()) return false;
            return !value.substring(3, 35).equals("00000000000000000000000000000000")
                    && !value.substring(36, 52).equals("0000000000000000");
        }

        private static boolean isTraceField(String key) {
            return key != null && (key.equalsIgnoreCase("traceparent")
                    || key.equalsIgnoreCase("tracestate"));
        }

        private static String valueIgnoreCase(Map<String, String> values, String key) {
            if (values == null) return null;
            for (Map.Entry<String, String> entry : values.entrySet()) {
                if (entry.getKey() != null && entry.getKey().toLowerCase(Locale.ROOT)
                        .equals(key.toLowerCase(Locale.ROOT))) return entry.getValue();
            }
            return null;
        }

        private static void closeSafely(Tracer.SpanInScope scope) {
            if (scope == null) return;
            try { scope.close(); } catch (RuntimeException failure) {
                warn("Unable to close consumer trace scope");
            }
        }

        private static void endSafely(Span span) {
            if (span == null) return;
            try { span.end(); } catch (RuntimeException failure) {
                warn("Unable to end consumer span");
            }
        }

        private static void warn(String message) {
            LOGGER.log(System.Logger.Level.WARNING, message);
        }

        private static final class Scope implements AutoCloseable {
            private static final Scope NOOP = new Scope(null, null);
            private final Span span;
            private final Tracer.SpanInScope scope;
            private boolean closed;

            private Scope(Span span, Tracer.SpanInScope scope) {
                this.span = span;
                this.scope = scope;
            }

            void processed(boolean applied) {
                record(() -> {
                    span.tag("reliable_event.consume.result", applied ? "processed" : "idempotent_skip");
                    span.event(applied ? "reliable_event.consumer.processed" : "reliable_event.consumer.idempotent_skip");
                });
            }

            void processingFailed(Throwable failure) {
                record(() -> {
                    span.tag("reliable_event.consume.result", "failed");
                    span.error(failure);
                    span.event("reliable_event.consumer.failed");
                });
            }

            void acknowledged() {
                record(() -> {
                    span.tag("reliable_event.ack.result", "success");
                    span.event("reliable_event.consumer.acknowledged");
                });
            }

            void acknowledgementFailed(Throwable failure) {
                record(() -> {
                    span.tag("reliable_event.ack.result", "failed");
                    span.error(failure);
                    span.event("reliable_event.consumer.ack_failed");
                });
            }

            private void record(Runnable action) {
                if (span == null) return;
                try { action.run(); } catch (RuntimeException failure) {
                    warn("Unable to record consumer span result");
                }
            }

            @Override
            public void close() {
                if (closed) return;
                closed = true;
                closeSafely(scope);
                endSafely(span);
            }
        }
    }

    private static String property(MessageView message, String key) {
        try { return ConsumerTracing.valueIgnoreCase(message.getProperties(), key); }
        catch (RuntimeException failure) { return null; }
    }
}
