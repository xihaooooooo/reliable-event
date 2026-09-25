package dev.reliableevent.autoconfigure;

import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Schedules bounded publication work. A queued candidate has not claimed a database lease.
 */
public final class ReliableEventScheduler implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(ReliableEventScheduler.class);

    private final JdbcExpiredLeaseRecovery recovery;
    private final JdbcEventPublicationWorker worker;
    private final int claimBatchSize;
    private final long pollIntervalMillis;
    private final Semaphore slots;
    private final Set<Long> outstandingIds = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scanner;
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ReentrantReadWriteLock claimGate = new ReentrantReadWriteLock(true);
    private volatile boolean running;

    public ReliableEventScheduler(
            JdbcExpiredLeaseRecovery recovery,
            JdbcEventPublicationWorker worker,
            int claimBatchSize,
            int workerThreads,
            int workerQueueCapacity,
            Duration pollInterval
    ) {
        this.recovery = Objects.requireNonNull(recovery, "recovery must not be null");
        this.worker = Objects.requireNonNull(worker, "worker must not be null");
        if (claimBatchSize <= 0 || workerThreads <= 0 || workerQueueCapacity < 0) {
            throw new IllegalArgumentException("Batch size and worker threads must be positive; queue capacity must not be negative");
        }
        int capacity = Math.addExact(workerThreads, workerQueueCapacity);
        Objects.requireNonNull(pollInterval, "pollInterval must not be null");
        this.pollIntervalMillis = pollInterval.toMillis();
        if (pollIntervalMillis <= 0) {
            throw new IllegalArgumentException("pollInterval must be at least one millisecond");
        }
        this.claimBatchSize = claimBatchSize;
        this.slots = new Semaphore(capacity);
        this.scanner = Executors.newSingleThreadScheduledExecutor(namedDaemonThreads("reliable-event-scan"));
        BlockingQueue<Runnable> queue = workerQueueCapacity == 0
                ? new SynchronousQueue<>()
                : new ArrayBlockingQueue<>(workerQueueCapacity);
        this.executor = new ThreadPoolExecutor(
                workerThreads, workerThreads, 0L, TimeUnit.MILLISECONDS, queue,
                namedDaemonThreads("reliable-event-send"), new ThreadPoolExecutor.AbortPolicy()
        );
    }

    /** Executes one recovery and dispatch pass without waiting for submitted sends. */
    AsyncPublicationCycleResult dispatchOnce() {
        if (stopped.get()) {
            return new AsyncPublicationCycleResult(0, 0);
        }
        int recoveredCount = recovery.recoverExpiredLeases();
        int dispatchLimit = Math.min(claimBatchSize, slots.availablePermits());
        if (dispatchLimit == 0 || stopped.get()) {
            return new AsyncPublicationCycleResult(recoveredCount, 0);
        }

        int queryLimit = (int) Math.min(Integer.MAX_VALUE,
                (long) dispatchLimit + outstandingIds.size());
        List<EventCandidate> candidates = worker.findDueEventCandidates(queryLimit);
        int submittedCount = 0;
        for (EventCandidate candidate : candidates) {
            if (submittedCount == dispatchLimit || stopped.get()) {
                break;
            }
            if (!slots.tryAcquire()) {
                break;
            }
            long eventId = candidate.id().value();
            if (!outstandingIds.add(eventId)) {
                slots.release();
                continue;
            }
            CandidateTask task = new CandidateTask(candidate);
            try {
                executor.execute(task);
                submittedCount++;
            } catch (RejectedExecutionException exception) {
                task.release();
                if (!stopped.get()) {
                    LOG.warn("Reliable event executor rejected a candidate before claim; eventId={}", eventId);
                }
                break;
            }
        }
        return new AsyncPublicationCycleResult(recoveredCount, submittedCount);
    }

    int outstandingCount() {
        return outstandingIds.size();
    }

    private void runSafely() {
        try {
            dispatchOnce();
        } catch (RuntimeException exception) {
            LOG.error("Reliable event scan failed; type={}", exception.getClass().getName());
        }
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (stopped.get()) {
            throw new IllegalStateException("Reliable event scheduler cannot restart after stop");
        }
        scanner.scheduleWithFixedDelay(this::runSafely, 0, pollIntervalMillis, TimeUnit.MILLISECONDS);
        running = true;
    }

    @Override
    public synchronized void stop() {
        claimGate.writeLock().lock();
        try {
            if (!stopped.compareAndSet(false, true)) {
                return;
            }
        } finally {
            claimGate.writeLock().unlock();
        }
        running = false;
        scanner.shutdownNow();
        for (Runnable task : executor.shutdownNow()) {
            ((CandidateTask) task).release();
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    private static ThreadFactory namedDaemonThreads(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, prefix + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private final class CandidateTask implements Runnable {
        private final EventCandidate candidate;
        private final AtomicBoolean released = new AtomicBoolean();

        private CandidateTask(EventCandidate candidate) {
            this.candidate = candidate;
        }

        @Override
        public void run() {
            try {
                ClaimedEvent claimed;
                claimGate.readLock().lock();
                try {
                    if (stopped.get()) {
                        return;
                    }
                    claimed = worker.claimCandidate(candidate).orElse(null);
                } finally {
                    claimGate.readLock().unlock();
                }
                if (claimed != null) {
                    worker.publishClaimedEvent(claimed);
                }
            } catch (RuntimeException exception) {
                LOG.error("Reliable event task failed; eventId={}, type={}",
                        candidate.id().value(), exception.getClass().getName());
            } finally {
                release();
            }
        }

        private void release() {
            if (released.compareAndSet(false, true)) {
                outstandingIds.remove(candidate.id().value());
                slots.release();
            }
        }
    }
}
