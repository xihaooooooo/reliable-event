package dev.reliableevent.kafka.example;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** One poll thread owns the Consumer and commits each record only after handler transaction returns. */
final class ExampleConsumerLoop implements SmartLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleConsumerLoop.class);
    private final Map<String, Object> configuration;
    private final String topic;
    private final Duration pollTimeout;
    private final OrderMessageHandler handler;
    private final ConsumerTracing tracing;
    private final ConsumerFactory consumerFactory;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Consumer<String, byte[]> consumer;
    private volatile long lastFailedOffset = -1;
    private Thread thread;

    ExampleConsumerLoop(Map<String, Object> configuration, String topic, Duration pollTimeout,
                        OrderMessageHandler handler) {
        this(configuration, topic, pollTimeout, handler, null, null);
    }

    ExampleConsumerLoop(Map<String, Object> configuration, String topic, Duration pollTimeout,
                        OrderMessageHandler handler, Tracer tracer, Propagator propagator) {
        this(configuration, topic, pollTimeout, handler, tracer, propagator, KafkaConsumer::new);
    }

    ExampleConsumerLoop(Map<String, Object> configuration, String topic, Duration pollTimeout,
                        OrderMessageHandler handler, Tracer tracer, Propagator propagator,
                        ConsumerFactory consumerFactory) {
        this.configuration = Map.copyOf(configuration);
        this.topic = Objects.requireNonNull(topic);
        this.pollTimeout = Objects.requireNonNull(pollTimeout);
        this.handler = Objects.requireNonNull(handler);
        this.tracing = new ConsumerTracing(tracer, propagator);
        this.consumerFactory = Objects.requireNonNull(consumerFactory);
    }

    @Override public synchronized void start() {
        if (!running.compareAndSet(false, true)) return;
        thread = new Thread(this::poll, "reliable-event-kafka-example-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void poll() {
        Properties properties = new Properties();
        properties.putAll(configuration);
        properties.put(org.apache.kafka.clients.CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        Consumer<String, byte[]> owned = null;
        try {
            owned = consumerFactory.create(properties);
            consumer = owned;
            owned.subscribe(java.util.List.of(topic));
            while (running.get()) {
                var records = owned.poll(pollTimeout);
                int index = 0;
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (!running.get()) return;
                    ConsumerTracing.Scope trace = tracing.begin(record);
                    try (trace) {
                        boolean applied;
                        try {
                            // Spring's @Transactional proxy commits before handle returns.
                            applied = handler.handle(record);
                            trace.processed(applied);
                        } catch (Exception failure) {
                            trace.processingFailed(failure);
                            lastFailedOffset = record.offset();
                            rewindUnprocessed(owned, records, index);
                            LOG.warn("event=example.order.consume_failed topic={} partition={} offset={} exceptionType={} message={}",
                                    record.topic(), record.partition(), record.offset(),
                                    failure.getClass().getName(), failure.getMessage());
                            retryPause();
                            break;
                        }
                        try {
                            owned.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset() + 1)), Duration.ofSeconds(5));
                            trace.offsetCommitted();
                            LOG.info("event=example.order.consumed eventId={} applied={} topic={} partition={} offset={}",
                                    header(record, "reliable_event_id"), applied, record.topic(), record.partition(), record.offset());
                        } catch (Exception failure) {
                            trace.offsetCommitFailed(failure);
                            lastFailedOffset = record.offset();
                            rewindUnprocessed(owned, records, index);
                            LOG.warn("event=example.order.offset_commit_failed topic={} partition={} offset={} exceptionType={} message={}",
                                    record.topic(), record.partition(), record.offset(),
                                    failure.getClass().getName(), failure.getMessage());
                            retryPause();
                            break;
                        }
                    }
                    index++;
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException wakeup) {
            if (running.get()) LOG.warn("Kafka example consumer was woken unexpectedly");
        } catch (Exception failure) {
            if (running.get()) LOG.error("Kafka example consumer stopped after poll failure", failure);
        } finally {
            if (owned != null) {
                try { owned.close(Duration.ofSeconds(5)); }
                catch (RuntimeException closeFailure) {
                    LOG.warn("Kafka example consumer close failed: {}", closeFailure.getMessage());
                }
            }
            consumer = null;
            running.set(false);
        }
    }

    private void retryPause() {
        try { Thread.sleep(500); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }

    static void rewindUnprocessed(Consumer<String, byte[]> consumer,
                                  Iterable<ConsumerRecord<String, byte[]>> records,
                                  int failedIndex) {
        Map<TopicPartition, Long> firstOffsets = new HashMap<>();
        int index = 0;
        for (ConsumerRecord<String, byte[]> record : records) {
            if (index++ < failedIndex) continue;
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            firstOffsets.merge(partition, record.offset(), Math::min);
        }
        firstOffsets.forEach((partition, offset) -> consumer.seek(partition, offset));
    }

    private static String header(ConsumerRecord<String, byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null
                : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    /** W3C trace propagation for the example consumer; invalid/missing parents start a new root. */
    static final class ConsumerTracing {
        private static final System.Logger LOGGER = System.getLogger(ConsumerTracing.class.getName());
        private static final Pattern TRACEPARENT = Pattern.compile("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
        private final Tracer tracer;
        private final Propagator propagator;

        ConsumerTracing(Tracer tracer, Propagator propagator) {
            this.tracer = tracer;
            this.propagator = propagator;
        }

        Scope begin(ConsumerRecord<String, byte[]> record) {
            if (tracer == null || propagator == null) return Scope.NOOP;
            Span span = null;
            Tracer.SpanInScope scope = null;
            try {
                String parent = traceHeaderIgnoreCase(record, "traceparent");
                Span.Builder builder = validTraceParent(parent)
                        ? propagator.extract(record.headers(), (carrier, key) ->
                                isTraceField(key) ? traceHeaderIgnoreCase(record, key) : null)
                        : tracer.spanBuilder().setNoParent();
                span = builder.name("example.order.consume").kind(Span.Kind.CONSUMER)
                        .tag("reliable_event.consume.result", "started")
                        .tag("reliable_event.event_id", Objects.toString(header(record, "reliable_event_id"), "unknown"))
                        .start();
                scope = tracer.withSpan(span);
                return new Scope(span, scope);
            } catch (RuntimeException failure) {
                LOGGER.log(System.Logger.Level.WARNING, "Unable to start Kafka consumer span");
                closeSafely(scope);
                endSafely(span);
                return Scope.NOOP;
            }
        }

        private static boolean validTraceParent(String value) {
            return value != null && TRACEPARENT.matcher(value).matches()
                    && !value.substring(3, 35).equals("00000000000000000000000000000000")
                    && !value.substring(36, 52).equals("0000000000000000");
        }
        private static boolean isTraceField(String key) {
            return key != null && (key.equalsIgnoreCase("traceparent") || key.equalsIgnoreCase("tracestate"));
        }
        private static String traceHeaderIgnoreCase(ConsumerRecord<String, byte[]> record, String name) {
            for (var header : record.headers()) {
                if (header.key().equalsIgnoreCase(name) && header.value() != null) {
                    return new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
                }
            }
            return null;
        }
        private static void closeSafely(Tracer.SpanInScope scope) {
            if (scope == null) return;
            try { scope.close(); } catch (RuntimeException failure) {
                LOGGER.log(System.Logger.Level.WARNING, "Unable to close Kafka consumer trace scope");
            }
        }
        private static void endSafely(Span span) {
            if (span == null) return;
            try { span.end(); } catch (RuntimeException failure) {
                LOGGER.log(System.Logger.Level.WARNING, "Unable to end Kafka consumer span");
            }
        }

        static final class Scope implements AutoCloseable {
            static final Scope NOOP = new Scope(null, null);
            private final Span span;
            private final Tracer.SpanInScope scope;
            private boolean closed;
            Scope(Span span, Tracer.SpanInScope scope) { this.span = span; this.scope = scope; }
            void processed(boolean applied) { record(() -> {
                span.tag("reliable_event.consume.result", applied ? "processed" : "idempotent_skip");
                span.event(applied ? "reliable_event.consumer.processed" : "reliable_event.consumer.idempotent_skip");
            }); }
            void processingFailed(Throwable error) { record(() -> {
                span.tag("reliable_event.consume.result", "failed"); span.error(error);
                span.event("reliable_event.consumer.failed");
            }); }
            void offsetCommitted() { record(() -> {
                span.tag("reliable_event.offset_commit.result", "success");
                span.event("reliable_event.consumer.offset_committed");
            }); }
            void offsetCommitFailed(Throwable error) { record(() -> {
                span.tag("reliable_event.offset_commit.result", "failed"); span.error(error);
                span.event("reliable_event.consumer.offset_commit_failed");
            }); }
            private void record(Runnable action) {
                if (span == null) return;
                try { action.run(); } catch (RuntimeException failure) {
                    LOGGER.log(System.Logger.Level.WARNING, "Unable to record Kafka consumer span result");
                }
            }
            @Override public void close() {
                if (closed) return; closed = true;
                closeSafely(scope);
                endSafely(span);
            }
        }
    }

    @Override public synchronized void stop() {
        running.set(false);
        Consumer<String, byte[]> active = consumer;
        if (active != null) active.wakeup();
        if (thread != null) {
            try { thread.join(Duration.ofSeconds(5).toMillis()); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
    }

    @Override public void stop(Runnable callback) {
        try { stop(); } finally { callback.run(); }
    }
    @Override public boolean isRunning() { return running.get(); }
    @Override public int getPhase() { return Integer.MAX_VALUE - 200; }
    long lastFailedOffset() { return lastFailedOffset; }

    @FunctionalInterface
    interface ConsumerFactory { Consumer<String, byte[]> create(Properties properties); }
}
