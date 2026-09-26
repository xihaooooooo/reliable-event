package dev.reliableevent.benchmark;

import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
final class BenchmarkRunner implements ApplicationRunner {

    private static final String EVENT_TYPE = "benchmark-event";
    private static final String TOPIC = "reliable-event-benchmark";

    private final Environment env;
    private final JdbcTemplate jdbc;
    private final PlatformTransactionManager transactions;
    private final ObjectProvider<ReliableEventPublisher> publisher;
    private final ObjectProvider<MeterRegistry> meters;

    BenchmarkRunner(Environment env, JdbcTemplate jdbc, PlatformTransactionManager transactions,
                    ObjectProvider<ReliableEventPublisher> publisher, ObjectProvider<MeterRegistry> meters) {
        this.env = env;
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.publisher = publisher;
        this.meters = meters;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String mode = value("benchmark.mode", "report");
        if (!mode.equals("publisher") && !mode.equals("receiver")) {
            requireDedicatedDatabase();
        }
        switch (mode) {
            case "reset" -> reset();
            case "seed" -> seed();
            case "load" -> load();
            case "report" -> report();
            case "publisher" -> publisher();
            case "receiver" -> receiver();
            default -> throw new IllegalArgumentException("Unknown benchmark.mode: " + mode);
        }
    }

    private void requireDedicatedDatabase() {
        String database = jdbc.queryForObject("SELECT DATABASE()", String.class);
        if (!"reliable_event_benchmark".equals(database)) {
            throw new IllegalStateException("Benchmark data operations require reliable_event_benchmark database");
        }
    }

    private void reset() {
        jdbc.execute("TRUNCATE TABLE reliable_event_outbox");
        System.out.println("BENCHMARK_RESET_OK");
    }

    private void seed() {
        String runId = runId();
        int count = nonNegative("benchmark.count", 0);
        int batchSize = positive("benchmark.batch-size", 1000);
        for (int offset = 0; offset < count; offset += batchSize) {
            int size = Math.min(batchSize, count - offset);
            String tuple = """
                    (?, ?, '{"history":true}', '{}', 2, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3),
                     1, 8, 2, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))
                    """.trim();
            String sql = """
                    INSERT INTO reliable_event_outbox
                        (event_type, event_key, payload, headers, status, next_attempt_at,
                         first_available_at, attempt_count, max_attempts, version,
                         created_at, updated_at, published_at)
                    VALUES
                    """ + String.join(",", java.util.Collections.nCopies(size, tuple));
            Object[] parameters = new Object[size * 2];
            for (int index = 0; index < size; index++) {
                parameters[index * 2] = EVENT_TYPE;
                parameters[index * 2 + 1] = "history-" + runId + "-" + (offset + index);
            }
            jdbc.update(sql, parameters);
        }
        System.out.println("BENCHMARK_SEEDED=" + count);
    }

    private void load() throws Exception {
        ReliableEventPublisher eventPublisher = publisher.getObject();
        String runId = runId();
        int count = positive("benchmark.count", 100);
        int batchSize = positive("benchmark.batch-size", 100);
        int threads = positive("benchmark.load-threads", 4);
        String padding = "x".repeat(nonNegative("benchmark.payload-padding", 256));
        int batches = (count + batchSize - 1) / batchSize;
        TransactionTemplate transaction = new TransactionTemplate(transactions);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<BatchResult>> futures = new ArrayList<>();
        long startedNanos = System.nanoTime();
        try {
            for (int batch = 0; batch < batches; batch++) {
                int start = batch * batchSize;
                int size = Math.min(batchSize, count - start);
                futures.add(executor.submit((Callable<BatchResult>) () -> {
                    long batchStart = System.nanoTime();
                    transaction.executeWithoutResult(status -> {
                        for (int index = start; index < start + size; index++) {
                            eventPublisher.publish(new ReliableEvent<>(
                                    EVENT_TYPE, runId + "-" + index,
                                    Map.of("runId", runId, "sequence", index, "padding", padding),
                                    Instant.now(), Map.of("source", "reliable-event-benchmark")
                            ));
                        }
                    });
                    return new BatchResult(start, size, System.currentTimeMillis(),
                            System.nanoTime() - batchStart);
                }));
            }
            Path output = output();
            try (BufferedWriter writer = Files.newBufferedWriter(output)) {
                writer.write("start_sequence,count,commit_return_epoch_ms,batch_duration_ns\n");
                for (Future<BatchResult> future : futures) {
                    BatchResult result = future.get();
                    writer.write(result.start() + "," + result.count() + "," + result.committedAt()
                            + "," + result.durationNanos() + "\n");
                }
            }
        } finally {
            executor.shutdownNow();
        }
        System.out.println("BENCHMARK_LOAD_RUN_ID=" + runId + " COUNT=" + count
                + " DURATION_NS=" + (System.nanoTime() - startedNanos));
    }

    private void report() throws IOException {
        String runId = runId();
        Path output = output();
        try (BufferedWriter writer = Files.newBufferedWriter(output)) {
            writer.write("event_id,event_key,status,attempt_count,first_available_epoch_ms,"
                    + "published_epoch_ms,lag_ms\n");
            try {
                jdbc.query("""
                        SELECT id, event_key, status, attempt_count, first_available_at, published_at
                        FROM reliable_event_outbox
                        WHERE event_type = ? AND event_key LIKE ?
                        ORDER BY id
                        """, resultSet -> {
                    Timestamp first = resultSet.getTimestamp("first_available_at");
                    Timestamp published = resultSet.getTimestamp("published_at");
                    long firstMillis = first == null ? -1 : first.getTime();
                    long publishedMillis = published == null ? -1 : published.getTime();
                    try {
                        writer.write(resultSet.getLong("id") + "," + resultSet.getString("event_key")
                                + "," + resultSet.getInt("status") + ","
                                + resultSet.getInt("attempt_count") + "," + firstMillis + ","
                                + publishedMillis + ","
                                + (published == null || first == null ? "" : publishedMillis - firstMillis)
                                + "\n");
                    } catch (IOException failure) {
                        throw new UncheckedIOException(failure);
                    }
                }, EVENT_TYPE, runId + "-%");
            } catch (UncheckedIOException failure) {
                throw failure.getCause();
            }
        }
        System.out.println("BENCHMARK_REPORT=" + output);
    }

    private void publisher() throws Exception {
        publisher.getObject();
        MeterRegistry registry = meters.getObject();
        try (BufferedWriter writer = Files.newBufferedWriter(output())) {
            writer.write("epoch_ms,meter,outcome,count,total_ms,max_ms,value\n");
            writer.flush();
            ready();
            while (!Thread.currentThread().isInterrupted()) {
                long at = System.currentTimeMillis();
                for (Timer timer : registry.find("reliable_event.publish.duration").timers()) {
                    writeTimer(writer, at, "publish.duration", timer.getId().getTag("outcome"), timer);
                }
                for (Timer timer : registry.find("reliable_event.publish.lag").timers()) {
                    writeTimer(writer, at, "publish.lag", "", timer);
                }
                writeGauge(writer, at, "backlog", registry.find("reliable_event.backlog").gauge());
                writeGauge(writer, at, "dead", registry.find("reliable_event.dead").gauge());
                writer.flush();
                Thread.sleep(1000);
            }
        }
    }

    private void writeTimer(BufferedWriter writer, long at, String name, String outcome, Timer timer)
            throws IOException {
        writer.write(at + "," + name + "," + (outcome == null ? "" : outcome) + ","
                + timer.count() + "," + timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)
                + "," + timer.max(java.util.concurrent.TimeUnit.MILLISECONDS) + ",\n");
    }

    private void writeGauge(BufferedWriter writer, long at, String name, Gauge gauge) throws IOException {
        if (gauge != null) {
            writer.write(at + "," + name + ",,,,," + gauge.value() + "\n");
        }
    }

    private void receiver() throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints(value("reliable-event.rocketmq.endpoints", "localhost:8081"))
                .setRequestTimeout(Duration.ofSeconds(5)).enableSsl(false).build();
        Path progress = Path.of(value("benchmark.progress-file", "benchmark-received-count.txt")).toAbsolutePath();
        if (progress.getParent() != null) {
            Files.createDirectories(progress.getParent());
        }
        Files.writeString(progress, "0");
        int received = 0;
        try (SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(value("benchmark.receiver-group", "reliable-event-benchmark-consumer"))
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(Map.of(TOPIC, FilterExpression.SUB_ALL)).build();
             BufferedWriter writer = Files.newBufferedWriter(output())) {
            writer.write("received_epoch_ms,event_id,event_key,broker_message_id\n");
            writer.flush();
            ready();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    int before = received;
                    for (MessageView message : consumer.receive(32, Duration.ofSeconds(10))) {
                        Map<String, String> properties = message.getProperties();
                        String key = properties.get("reliable_event_key");
                        if (key != null && key.startsWith(runId() + "-")) {
                            writer.write(System.currentTimeMillis() + ","
                                    + properties.get("reliable_event_id") + "," + key + ","
                                    + message.getMessageId() + "\n");
                            writer.flush();
                            received++;
                        }
                        consumer.ack(message);
                    }
                    if (received != before) {
                        Files.writeString(progress, Integer.toString(received));
                    }
                } catch (Exception failure) {
                    System.err.println("BENCHMARK_RECEIVER_ERROR=" + failure.getClass().getName());
                    Thread.sleep(500);
                }
            }
        }
    }

    private void ready() throws IOException {
        Path path = Path.of(value("benchmark.ready-file", "benchmark-ready.txt")).toAbsolutePath();
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.writeString(path, "READY " + Instant.now() + System.lineSeparator());
        System.out.println("BENCHMARK_READY=" + path);
    }

    private Path output() throws IOException {
        Path path = Path.of(value("benchmark.output", "benchmark-output.csv")).toAbsolutePath();
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        return path;
    }

    private String runId() {
        String id = value("benchmark.run-id", "local");
        if (!id.matches("[A-Za-z0-9-]{1,48}")) {
            throw new IllegalArgumentException("benchmark.run-id must use 1-48 ASCII letters, digits or hyphens");
        }
        return id;
    }

    private int positive(String name, int fallback) {
        int value = Integer.parseInt(value(name, Integer.toString(fallback)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private int nonNegative(String name, int fallback) {
        int value = Integer.parseInt(value(name, Integer.toString(fallback)));
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private String value(String name, String fallback) {
        return Objects.requireNonNullElse(env.getProperty(name), fallback);
    }

    private record BatchResult(int start, int count, long committedAt, long durationNanos) { }
}
