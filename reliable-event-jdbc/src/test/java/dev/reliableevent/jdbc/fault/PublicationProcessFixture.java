package dev.reliableevent.jdbc.fault;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class PublicationProcessFixture implements AutoCloseable {

    private static final String MAIN_CLASS = PublicationCrashProcess.class.getName();
    private static final int MAX_DIAGNOSTIC_LINES = 20;

    private final Process process;
    private final BufferedReader output;

    private PublicationProcessFixture(Process process) {
        this.process = process;
        this.output = new BufferedReader(new InputStreamReader(
                process.getInputStream(),
                StandardCharsets.UTF_8
        ));
    }

    static PublicationProcessFixture start(
            PublicationCrashMode mode,
            long eventId,
            PublicationProcessConfig config
    ) throws IOException {
        Objects.requireNonNull(mode, "mode must not be null");
        Objects.requireNonNull(config, "config must not be null");
        if (eventId <= 0) {
            throw new IllegalArgumentException("eventId must be positive");
        }

        ProcessBuilder builder = new ProcessBuilder(commandFor(
                javaExecutable(),
                testClasspath(),
                mode,
                eventId
        ));
        builder.redirectErrorStream(true);
        config.applyTo(builder.environment());
        return new PublicationProcessFixture(builder.start());
    }

    PublicationReadySignal awaitReady(Duration timeout)
            throws InterruptedException, TimeoutException {
        long timeoutMillis = positiveTimeoutMillis(timeout);
        ExecutorService readerExecutor = Executors.newSingleThreadExecutor();
        Future<PublicationReadySignal> result = readerExecutor.submit(this::readReadySignal);
        try {
            return result.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException("unable to read child process output", cause);
        } catch (TimeoutException exception) {
            destroyForcibly(Duration.ofSeconds(5));
            throw exception;
        } finally {
            result.cancel(true);
            readerExecutor.shutdownNow();
        }
    }

    void destroyForcibly(Duration timeout) throws InterruptedException {
        long timeoutMillis = positiveTimeoutMillis(timeout);
        if (process.isAlive()) {
            process.destroyForcibly();
        }
        if (!process.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("child process did not terminate within timeout");
        }
    }

    boolean isAlive() {
        return process.isAlive();
    }

    static List<String> commandFor(
            Path javaExecutable,
            String classpath,
            PublicationCrashMode mode,
            long eventId
    ) {
        Objects.requireNonNull(javaExecutable, "javaExecutable must not be null");
        if (classpath == null || classpath.isBlank()) {
            throw new IllegalArgumentException("classpath must not be blank");
        }
        Objects.requireNonNull(mode, "mode must not be null");
        if (eventId <= 0) {
            throw new IllegalArgumentException("eventId must be positive");
        }
        return List.of(
                javaExecutable.toString(),
                "-cp",
                classpath,
                MAIN_CLASS,
                mode.name(),
                Long.toString(eventId)
        );
    }

    private PublicationReadySignal readReadySignal() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        String line;
        while ((line = output.readLine()) != null) {
            if (line.startsWith("READY ")) {
                return PublicationReadySignal.parse(line);
            }
            if (diagnostics.size() < MAX_DIAGNOSTIC_LINES) {
                diagnostics.add(line);
            }
        }
        String exit = process.isAlive() ? "still running" : Integer.toString(process.exitValue());
        throw new IllegalStateException(
                "child process exited before READY; exit=" + exit + ", output=" + diagnostics
        );
    }

    private static Path javaExecutable() {
        String executable = System.getProperty("os.name", "")
                .toLowerCase()
                .contains("win") ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", executable);
    }

    private static String testClasspath() {
        String surefireClasspath = System.getProperty("surefire.test.class.path");
        if (surefireClasspath != null && !surefireClasspath.isBlank()) {
            return surefireClasspath;
        }
        String javaClasspath = System.getProperty("java.class.path");
        if (javaClasspath == null || javaClasspath.isBlank()) {
            throw new IllegalStateException("test classpath is unavailable");
        }
        return javaClasspath;
    }

    private static long positiveTimeoutMillis(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        long timeoutMillis = timeout.toMillis();
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        return timeoutMillis;
    }

    @Override
    public void close() {
        RuntimeException cleanupFailure = null;
        try {
            if (process.isAlive()) {
                process.destroyForcibly();
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    cleanupFailure = new IllegalStateException(
                            "child process remained alive during cleanup"
                    );
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            cleanupFailure = new IllegalStateException(
                    "interrupted while cleaning up child process",
                    exception
            );
        } finally {
            try {
                process.getOutputStream().close();
                output.close();
            } catch (IOException exception) {
                if (cleanupFailure == null) {
                    cleanupFailure = new IllegalStateException(
                            "unable to close child process streams",
                            exception
                    );
                } else {
                    cleanupFailure.addSuppressed(exception);
                }
            }
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }
}
