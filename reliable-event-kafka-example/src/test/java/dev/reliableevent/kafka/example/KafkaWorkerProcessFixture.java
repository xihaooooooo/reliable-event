package dev.reliableevent.kafka.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class KafkaWorkerProcessFixture implements AutoCloseable {
    private final String workerId;
    private final Process process;
    private final Path ready;
    private final Path result;
    private final Path log;
    private final Path timeline;

    private KafkaWorkerProcessFixture(String workerId, Process process, Path ready, Path result, Path log, Path timeline) {
        this.workerId = workerId; this.process = process; this.ready = ready; this.result = result;
        this.log = log; this.timeline = timeline;
        append("worker_started worker=" + workerId + " pid=" + process.pid());
    }

    static KafkaWorkerProcessFixture start(String workerId, String jdbcUrl, String username, String password,
                                           String bootstrap, String topic, Path evidence, Path barrier) throws IOException {
        Path java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name", "").toLowerCase().contains("win") ? "java.exe" : "java");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        if (classpath == null || classpath.isBlank()) throw new IllegalStateException("test classpath is unavailable");
        Files.createDirectories(evidence);
        Path ready = evidence.resolve(workerId + ".ready");
        Path result = evidence.resolve(workerId + ".result");
        Path log = evidence.resolve(workerId + ".log");
        ProcessBuilder builder = new ProcessBuilder(java.toString(), "-cp", classpath,
                KafkaPublicationWorkerProcess.class.getName(), workerId);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("KAFKA_CRASH_JDBC_URL", jdbcUrl);
        builder.environment().put("KAFKA_CRASH_JDBC_USER", username);
        builder.environment().put("KAFKA_CRASH_JDBC_PASSWORD", password);
        builder.environment().put("KAFKA_CRASH_BOOTSTRAP", bootstrap);
        builder.environment().put("KAFKA_CRASH_TOPIC", topic);
        builder.environment().put("KAFKA_CRASH_READY_FILE", ready.toString());
        builder.environment().put("KAFKA_CRASH_RESULT_FILE", result.toString());
        builder.environment().put("KAFKA_CRASH_START_BARRIER", barrier.toString());
        return new KafkaWorkerProcessFixture(workerId, builder.start(), ready, result, log,
                evidence.resolve("timeline.log"));
    }

    void awaitReady(Duration timeout) throws InterruptedException {
        awaitFile(ready, timeout);
        String marker = read(ready).strip();
        if (!marker.equals("READY " + workerId)) throw new IllegalStateException("Invalid ready marker: " + marker);
        append("worker_ready worker=" + workerId + " pid=" + process.pid());
    }

    int awaitResult(Duration timeout) throws InterruptedException {
        awaitFile(result, timeout);
        String marker = read(result).strip();
        String[] fields = marker.split(" ", -1);
        if (fields.length != 3 || !fields[0].equals("RESULT") || !fields[1].equals(workerId)
                || !fields[2].matches("[0-9]+")) throw new IllegalStateException("Invalid worker result: " + marker);
        int count = Integer.parseInt(fields[2]);
        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("Worker process did not exit, log=" + log);
        }
        int exit = process.exitValue();
        append("worker_result worker=" + workerId + " count=" + count + " exit_code=" + exit);
        if (exit != 0) throw new IllegalStateException("Worker failed with exit=" + exit + ", log=" + log);
        return count;
    }

    private void awaitFile(Path path, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(path)) return;
            if (!process.isAlive()) throw new IllegalStateException("Worker exited before marker, exit="
                    + process.exitValue() + ", log=" + log);
            Thread.sleep(25);
        }
        throw new IllegalStateException("Timed out waiting for " + path + ", log=" + log);
    }

    private static String read(Path path) {
        try { return Files.readString(path, StandardCharsets.UTF_8); }
        catch (IOException failure) { throw new IllegalStateException("Unable to read " + path, failure); }
    }
    private void append(String line) {
        try { Files.writeString(timeline, java.time.Instant.now() + " " + line + System.lineSeparator(),
                StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND); }
        catch (IOException failure) { throw new IllegalStateException("Unable to write timeline", failure); }
    }
    @Override public void close() {
        try {
            if (process.isAlive()) {
                process.destroyForcibly();
                if (!process.waitFor(5, TimeUnit.SECONDS)) throw new IllegalStateException("Worker remained alive");
                append("worker_forced_exit worker=" + workerId + " pid=" + process.pid()
                        + " exit_code=" + process.exitValue());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while cleaning worker", interrupted);
        }
    }
}
