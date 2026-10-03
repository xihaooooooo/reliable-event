package dev.reliableevent.autoconfigure;

import dev.reliableevent.jdbc.internal.model.OutboxMetricsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Owns the bounded, independent periodic snapshot resources. */
final class MetricsSnapshotSampler implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsSnapshotSampler.class);
    private final MicrometerPublicationObserver observer;
    private final boolean enabled;
    private final long intervalMillis;
    private final long totalTimeoutNanos;
    private final long shutdownTimeoutNanos;
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong activeGeneration = new AtomicLong(-1);
    private volatile ScheduledExecutorService ticker;
    private volatile ThreadPoolExecutor queryExecutor;
    private volatile CompletableFuture<Void> stopped;

    MetricsSnapshotSampler(MicrometerPublicationObserver observer, boolean enabled,
                           Duration interval, Duration timeout, Duration shutdownTimeout) {
        this.observer = Objects.requireNonNull(observer);
        this.enabled = enabled;
        this.intervalMillis = interval.toMillis();
        this.totalTimeoutNanos = timeout.toNanos();
        this.shutdownTimeoutNanos = shutdownTimeout.toNanos();
        if (intervalMillis <= 0 || totalTimeoutNanos <= 0 || shutdownTimeoutNanos <= 0) {
            throw new IllegalArgumentException("Snapshot sampler durations must be positive");
        }
        observer.periodicEnabled(enabled);
    }

    @Override
    public synchronized void start() {
        if (!enabled || running.get()) return;
        if (stopping.get()) throw new IllegalStateException("Snapshot sampler cannot restart after stop");
        queryExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(), daemonFactory("reliable-event-snapshot-query"),
                new ThreadPoolExecutor.AbortPolicy());
        ticker = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                daemonFactory("reliable-event-snapshot-scheduler"));
        running.set(true);
        ticker.scheduleWithFixedDelay(this::sampleSafely, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void sampleSafely() {
        if (stopping.get()) return;
        long started = System.nanoTime();
        long generation = observer.beginSnapshot();
        if (generation < 0) return;
        activeGeneration.set(generation);
        FutureTask<OutboxMetricsSnapshot> query = new FutureTask<>(observer::readSnapshot);
        try {
            synchronized (this) {
                if (stopping.get()) {
                    observer.invalidateSnapshot(generation);
                    observer.finishSnapshot(generation);
                    activeGeneration.compareAndSet(generation, -1);
                    return;
                }
                queryExecutor.execute(() -> {
                    try { query.run(); }
                    finally {
                        observer.finishSnapshot(generation);
                        activeGeneration.compareAndSet(generation, -1);
                    }
                });
            }
        } catch (RejectedExecutionException rejected) {
            observer.snapshotFailure(generation);
            observer.finishSnapshot(generation);
            activeGeneration.compareAndSet(generation, -1);
            return;
        }
        long remaining = totalTimeoutNanos - (System.nanoTime() - started);
        try {
            OutboxMetricsSnapshot snapshot = query.get(Math.max(0, remaining), TimeUnit.NANOSECONDS);
            if (System.nanoTime() - started < totalTimeoutNanos && !stopping.get()) {
                observer.publishSnapshot(generation, snapshot);
            } else {
                observer.snapshotFailure(generation);
                observer.invalidateSnapshot(generation);
            }
        } catch (java.util.concurrent.TimeoutException timeout) {
            observer.snapshotFailure(generation);
            observer.invalidateSnapshot(generation);
            query.cancel(true);
        } catch (InterruptedException interrupted) {
            observer.invalidateSnapshot(generation);
            query.cancel(true);
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException failure) {
            observer.snapshotFailure(generation);
            Throwable cause = failure.getCause();
            LOG.warn("event=reliable_event.metrics.snapshot_failed exceptionType={}",
                    cause == null ? failure.getClass().getName() : cause.getClass().getName());
        } catch (java.util.concurrent.CancellationException stoppedQuery) {
            // Shutdown invalidated this attempt; the query worker retains its slot until exit.
        }
    }

    @Override public void stop() { stopAsync().join(); }

    @Override
    public void stop(Runnable callback) {
        Objects.requireNonNull(callback);
        stopAsync().whenComplete((ignored, failure) -> {
            try { callback.run(); }
            catch (RuntimeException callbackFailure) {
                LOG.warn("event=reliable_event.snapshot.stop_callback_failed exceptionType={}",
                        callbackFailure.getClass().getName());
            }
        });
    }

    private CompletableFuture<Void> stopAsync() {
        synchronized (this) {
            if (stopped != null) return stopped;
            stopping.set(true);
            running.set(false);
            observer.stopSnapshotAdmission();
            stopped = new CompletableFuture<>();
        }
        long started = System.nanoTime();
        Thread shutdown = new Thread(() -> finishStop(started, stopped), "reliable-event-snapshot-stop");
        shutdown.setDaemon(true);
        try { shutdown.start(); }
        catch (RuntimeException failure) {
            LOG.error("event=reliable_event.snapshot.stop_thread_failed exceptionType={}",
                    failure.getClass().getName());
            finishStop(started, stopped);
        }
        return stopped;
    }

    private void finishStop(long started, CompletableFuture<Void> completion) {
        try {
            ScheduledExecutorService scheduled = ticker;
            ThreadPoolExecutor queries = queryExecutor;
            if (scheduled != null) scheduled.shutdownNow();
            if (queries != null) queries.shutdown();
            long remaining = Math.max(0, shutdownTimeoutNanos - (System.nanoTime() - started));
            if (scheduled != null) scheduled.awaitTermination(remaining, TimeUnit.NANOSECONDS);
            remaining = Math.max(0, shutdownTimeoutNanos - (System.nanoTime() - started));
            if (queries != null && !queries.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                long generation = activeGeneration.get();
                if (generation >= 0) observer.invalidateSnapshot(generation);
                queries.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            ThreadPoolExecutor queries = queryExecutor;
            if (queries != null) queries.shutdownNow();
        } finally {
            completion.complete(null);
        }
    }

    @Override public boolean isRunning() { return running.get(); }
    @Override public boolean isAutoStartup() { return enabled; }
    @Override public int getPhase() { return Integer.MAX_VALUE - 100; }

    private static ThreadFactory daemonFactory(String name) {
        return task -> { Thread thread = new Thread(task, name); thread.setDaemon(true); return thread; };
    }
}
