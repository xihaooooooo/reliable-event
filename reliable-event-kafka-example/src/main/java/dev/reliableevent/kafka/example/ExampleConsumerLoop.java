package dev.reliableevent.kafka.example;

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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** One poll thread owns the Consumer and commits each record only after handler transaction returns. */
final class ExampleConsumerLoop implements SmartLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleConsumerLoop.class);
    private final Map<String, Object> configuration;
    private final String topic;
    private final Duration pollTimeout;
    private final OrderMessageHandler handler;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Consumer<String, byte[]> consumer;
    private volatile long lastFailedOffset = -1;
    private Thread thread;

    ExampleConsumerLoop(Map<String, Object> configuration, String topic, Duration pollTimeout,
                        OrderMessageHandler handler) {
        this.configuration = Map.copyOf(configuration);
        this.topic = Objects.requireNonNull(topic);
        this.pollTimeout = Objects.requireNonNull(pollTimeout);
        this.handler = Objects.requireNonNull(handler);
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
            owned = new KafkaConsumer<>(properties);
            consumer = owned;
            owned.subscribe(java.util.List.of(topic));
            while (running.get()) {
                var records = owned.poll(pollTimeout);
                int index = 0;
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (!running.get()) return;
                    try {
                        // Spring's @Transactional proxy commits before handle returns.
                        boolean applied = handler.handle(record);
                        owned.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)), Duration.ofSeconds(5));
                        LOG.info("event=example.order.consumed eventId={} applied={} topic={} partition={} offset={}",
                                header(record, "reliable_event_id"), applied,
                                record.topic(), record.partition(), record.offset());
                    } catch (Exception failure) {
                        lastFailedOffset = record.offset();
                        rewindUnprocessed(owned, records, index);
                        LOG.warn("event=example.order.consume_failed topic={} partition={} offset={} exceptionType={} message={}",
                                record.topic(), record.partition(), record.offset(),
                                failure.getClass().getName(), failure.getMessage());
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        break;
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
}
