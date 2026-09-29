package dev.reliableevent.autoconfigure;

import dev.reliableevent.jdbc.JdbcPublishedEventRetention;
import dev.reliableevent.jdbc.PublishedRetentionResult;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Independent cleanup loop; no cleanup work is performed unless explicitly enabled. */
public final class PublishedRetentionScheduler implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(PublishedRetentionScheduler.class);

    private final JdbcPublishedEventRetention retention;
    private final PublicationObserver observer;
    private final Duration keepFor;
    private final int batchSize;
    private final Duration stopTimeout;
    private final long intervalMillis;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "reliable-event-retention");
        thread.setDaemon(true);
        return thread;
    });
    private volatile boolean running;

    public PublishedRetentionScheduler(JdbcPublishedEventRetention retention,
                                       PublicationObserver observer, Duration keepFor,
                                       int batchSize, Duration interval, Duration stopTimeout) {
        this.retention = Objects.requireNonNull(retention);
        this.observer = Objects.requireNonNull(observer);
        this.keepFor = Objects.requireNonNull(keepFor);
        this.batchSize = batchSize;
        this.intervalMillis = Objects.requireNonNull(interval).toMillis();
        this.stopTimeout = Objects.requireNonNull(stopTimeout);
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("interval must be at least one millisecond");
        }
    }

    private void runSafely() {
        long started = System.nanoTime();
        try {
            PublishedRetentionResult result = retention.runOnce(keepFor, batchSize);
            long elapsed = System.nanoTime() - started;
            if (result.scanned() > 0 || result.deleted() > 0) {
                LOG.info("event=reliable_event.retention.completed scanned={} deleted={} oldestEligibleAgeSeconds={} durationNanos={}",
                        result.scanned(), result.deleted(), result.oldestEligibleAgeSeconds(), elapsed);
            }
            observe(() -> observer.cleanupCompleted(result, elapsed));
        } catch (RuntimeException failure) {
            LOG.error("event=reliable_event.retention.failed exceptionType={}", failure.getClass().getName());
            observe(observer::cleanupFailed);
        }
    }

    private void observe(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException failure) {
            LOG.warn("event=reliable_event.observation.failed exceptionType={}",
                    failure.getClass().getName());
        }
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (executor.isShutdown()) {
            throw new IllegalStateException("Retention scheduler cannot restart after stop");
        }
        executor.scheduleWithFixedDelay(this::runSafely, 0, intervalMillis, TimeUnit.MILLISECONDS);
        running = true;
    }

    @Override
    public synchronized void stop() {
        running = false;
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(stopTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warn("event=reliable_event.retention.shutdown_timed_out");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
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
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 90;
    }
}
