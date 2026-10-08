package dev.reliableevent.kafka.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Child output is redirected to an evidence file, so a blocked child can never fill a pipe. */
final class KafkaCrashProcessFixture implements AutoCloseable {
    private final Process process;
    private final Path readyFile;
    private final Path childLog;
    private final Path timeline;
    private Integer exitCode;
    private boolean destroyed;

    private KafkaCrashProcessFixture(Process process, Path readyFile, Path childLog, Path timeline) {
        this.process = process;
        this.readyFile = readyFile;
        this.childLog = childLog;
        this.timeline = timeline;
        append("child_started pid=" + process.pid() + " log=" + childLog.getFileName());
    }

    static KafkaCrashProcessFixture start(KafkaCrashMode mode, String workerId,
                                          String jdbcUrl, String username, String password,
                                          String bootstrap, String topic, Path evidenceDirectory) throws IOException {
        Objects.requireNonNull(mode);
        Path java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name", "").toLowerCase().contains("win") ? "java.exe" : "java");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        if (classpath == null || classpath.isBlank()) throw new IllegalStateException("test classpath is unavailable");
        Files.createDirectories(evidenceDirectory);
        Path readyFile = evidenceDirectory.resolve("child-ready.txt");
        Path childLog = evidenceDirectory.resolve("child.log");
        Path timeline = evidenceDirectory.resolve("timeline.log");
        ProcessBuilder builder = new ProcessBuilder(java.toString(), "-cp", classpath,
                KafkaPublicationCrashProcess.class.getName(), mode.name(), workerId);
        builder.redirectErrorStream(true);
        builder.redirectOutput(childLog.toFile());
        builder.environment().put("KAFKA_CRASH_JDBC_URL", jdbcUrl);
        builder.environment().put("KAFKA_CRASH_JDBC_USER", username);
        builder.environment().put("KAFKA_CRASH_JDBC_PASSWORD", password);
        builder.environment().put("KAFKA_CRASH_BOOTSTRAP", bootstrap);
        builder.environment().put("KAFKA_CRASH_TOPIC", topic);
        builder.environment().put("KAFKA_CRASH_READY_FILE", readyFile.toString());
        return new KafkaCrashProcessFixture(builder.start(), readyFile, childLog, timeline);
    }

    Signal awaitReady(KafkaCrashMode expectedMode, String expectedWorker, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(readyFile)) {
                try {
                    String line = Files.readString(readyFile, StandardCharsets.UTF_8).strip();
                    String[] fields = line.split(" ", -1);
                    if (fields.length != 4 || !fields[0].equals("READY")
                            || !fields[1].equals(expectedMode.name()) || !fields[2].matches("[1-9][0-9]*")
                            || !fields[3].equals(expectedWorker)) {
                        throw new IllegalStateException("Invalid child readiness marker: " + line);
                    }
                    Signal signal = new Signal(expectedMode, Long.parseLong(fields[2]), fields[3], line);
                    append("child_ready " + line);
                    return signal;
                } catch (IOException failure) {
                    throw new IllegalStateException("Unable to read readiness marker " + readyFile, failure);
                }
            }
            if (!process.isAlive()) {
                throw new IllegalStateException("Child exited before READY, exit=" + process.exitValue()
                        + ", childLog=" + childLog);
            }
            Thread.sleep(50);
        }
        destroyForcibly(Duration.ofSeconds(5));
        throw new IllegalStateException("Timed out waiting for Kafka child READY, childLog=" + childLog);
    }

    int destroyForcibly(Duration timeout) throws InterruptedException {
        if (process.isAlive()) process.destroyForcibly();
        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("Kafka crash child did not terminate");
        }
        exitCode = process.exitValue();
        destroyed = true;
        append("child_forced_exit pid=" + process.pid() + " exit_code=" + exitCode);
        return exitCode;
    }

    void append(String line) {
        try {
            Files.writeString(timeline, java.time.Instant.now() + " " + line + System.lineSeparator(),
                    StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE,
                    java.nio.file.StandardOpenOption.APPEND);
        } catch (IOException failure) { throw new IllegalStateException("Unable to append evidence timeline", failure); }
    }

    @Override public void close() {
        RuntimeException failure = null;
        try {
            if (!destroyed && process.isAlive()) {
                process.destroyForcibly();
                if (!process.waitFor(5, TimeUnit.SECONDS)) failure = new IllegalStateException("Kafka child remained alive");
                else {
                    exitCode = process.exitValue();
                    append("child_forced_exit pid=" + process.pid() + " exit_code=" + exitCode + " cleanup=true");
                }
            } else if (exitCode == null && !process.isAlive()) {
                exitCode = process.exitValue();
                append("child_exit pid=" + process.pid() + " exit_code=" + exitCode);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            failure = new IllegalStateException("Interrupted while cleaning Kafka child", interrupted);
        }
        if (failure != null) throw failure;
    }

    Path childLog() { return childLog; }
    record Signal(KafkaCrashMode mode, long eventId, String workerId, String line) { }
}
