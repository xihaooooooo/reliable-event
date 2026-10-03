package dev.reliableevent.autoconfigure;

import dev.reliableevent.jdbc.JdbcPublishedEventRetention;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;

@ConfigurationProperties(prefix = "reliable-event", ignoreUnknownFields = true)
public class ReliableEventProperties {

    private boolean enabled = true;
    private boolean deadOperationsEnabled = false;
    private boolean publishedRetentionEnabled = false;
    private boolean tracingEnabled = true;
    private Duration publishedRetention;
    private int cleanupBatchSize = 100;
    private Duration cleanupInterval = Duration.ofHours(1);
    private int claimBatchSize = 50;
    private int recoveryBatchSize = 50;
    private boolean schedulingEnabled = true;
    private Duration pollInterval = Duration.ofSeconds(1);
    private boolean adaptivePollingEnabled = false;
    private Duration activePollInterval = Duration.ofMillis(100);
    private int workerThreads = 8;
    private int workerQueueCapacity = 200;
    private Duration shutdownTimeout = Duration.ofSeconds(20);
    private boolean metricsSnapshotEnabled = true;
    private Duration metricsSnapshotInterval = Duration.ofSeconds(15);
    private Duration metricsSnapshotQueryTimeout = Duration.ofSeconds(2);
    private Duration metricsSnapshotTimeout = Duration.ofSeconds(5);
    private Duration metricsSnapshotShutdownTimeout = Duration.ofSeconds(2);
    private Duration leaseDuration = Duration.ofSeconds(30);
    private int maxAttempts = 8;
    private Duration initialRetryDelay = Duration.ofSeconds(1);
    private Duration maxRetryDelay = Duration.ofMinutes(5);

    private String transport;
    public String getTransport() { return transport; }
    public void setTransport(String transport) { this.transport = transport; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isDeadOperationsEnabled() { return deadOperationsEnabled; }
    public void setDeadOperationsEnabled(boolean deadOperationsEnabled) {
        this.deadOperationsEnabled = deadOperationsEnabled;
    }
    public boolean isPublishedRetentionEnabled() { return publishedRetentionEnabled; }
    public void setPublishedRetentionEnabled(boolean publishedRetentionEnabled) {
        this.publishedRetentionEnabled = publishedRetentionEnabled;
    }
    public boolean isTracingEnabled() { return tracingEnabled; }
    public void setTracingEnabled(boolean tracingEnabled) { this.tracingEnabled = tracingEnabled; }
    public Duration getPublishedRetention() { return publishedRetention; }
    public void setPublishedRetention(Duration publishedRetention) { this.publishedRetention = publishedRetention; }
    public int getCleanupBatchSize() { return cleanupBatchSize; }
    public void setCleanupBatchSize(int cleanupBatchSize) { this.cleanupBatchSize = cleanupBatchSize; }
    public Duration getCleanupInterval() { return cleanupInterval; }
    public void setCleanupInterval(Duration cleanupInterval) { this.cleanupInterval = cleanupInterval; }
    public int getClaimBatchSize() { return claimBatchSize; }
    public void setClaimBatchSize(int claimBatchSize) { this.claimBatchSize = claimBatchSize; }
    public int getRecoveryBatchSize() { return recoveryBatchSize; }
    public void setRecoveryBatchSize(int recoveryBatchSize) { this.recoveryBatchSize = recoveryBatchSize; }
    public boolean isSchedulingEnabled() { return schedulingEnabled; }
    public void setSchedulingEnabled(boolean schedulingEnabled) { this.schedulingEnabled = schedulingEnabled; }
    public Duration getPollInterval() { return pollInterval; }
    public void setPollInterval(Duration pollInterval) { this.pollInterval = pollInterval; }
    public boolean isAdaptivePollingEnabled() { return adaptivePollingEnabled; }
    public void setAdaptivePollingEnabled(boolean adaptivePollingEnabled) {
        this.adaptivePollingEnabled = adaptivePollingEnabled;
    }
    public Duration getActivePollInterval() { return activePollInterval; }
    public void setActivePollInterval(Duration activePollInterval) {
        this.activePollInterval = activePollInterval;
    }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public int getWorkerQueueCapacity() { return workerQueueCapacity; }
    public void setWorkerQueueCapacity(int workerQueueCapacity) { this.workerQueueCapacity = workerQueueCapacity; }
    public Duration getShutdownTimeout() { return shutdownTimeout; }
    public void setShutdownTimeout(Duration shutdownTimeout) { this.shutdownTimeout = shutdownTimeout; }
    public boolean isMetricsSnapshotEnabled() { return metricsSnapshotEnabled; }
    public void setMetricsSnapshotEnabled(boolean enabled) { this.metricsSnapshotEnabled = enabled; }
    public Duration getMetricsSnapshotInterval() { return metricsSnapshotInterval; }
    public void setMetricsSnapshotInterval(Duration interval) { this.metricsSnapshotInterval = interval; }
    public Duration getMetricsSnapshotQueryTimeout() { return metricsSnapshotQueryTimeout; }
    public void setMetricsSnapshotQueryTimeout(Duration timeout) { this.metricsSnapshotQueryTimeout = timeout; }
    public Duration getMetricsSnapshotTimeout() { return metricsSnapshotTimeout; }
    public void setMetricsSnapshotTimeout(Duration timeout) { this.metricsSnapshotTimeout = timeout; }
    public Duration getMetricsSnapshotShutdownTimeout() { return metricsSnapshotShutdownTimeout; }
    public void setMetricsSnapshotShutdownTimeout(Duration timeout) { this.metricsSnapshotShutdownTimeout = timeout; }
    public Duration getLeaseDuration() { return leaseDuration; }
    public void setLeaseDuration(Duration leaseDuration) { this.leaseDuration = leaseDuration; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Duration getInitialRetryDelay() { return initialRetryDelay; }
    public void setInitialRetryDelay(Duration initialRetryDelay) { this.initialRetryDelay = initialRetryDelay; }
    public Duration getMaxRetryDelay() { return maxRetryDelay; }
    public void setMaxRetryDelay(Duration maxRetryDelay) { this.maxRetryDelay = maxRetryDelay; }

    public void validateCore() {
        positive(claimBatchSize, "claim-batch-size");
        positive(recoveryBatchSize, "recovery-batch-size");
        positiveMillis(pollInterval, "poll-interval");
        positiveMillis(activePollInterval, "active-poll-interval");
        positive(workerThreads, "worker-threads");
        if (workerQueueCapacity < 0) {
            throw invalid("worker-queue-capacity", "must not be negative");
        }
        if ((long) workerThreads + workerQueueCapacity > Integer.MAX_VALUE) {
            throw invalid("worker-threads/worker-queue-capacity", "combined capacity is too large");
        }
        positiveMillis(shutdownTimeout, "shutdown-timeout");
        try {
            shutdownTimeout.toNanos();
        } catch (ArithmeticException exception) {
            throw invalid("shutdown-timeout", "is too large");
        }
        positive(maxAttempts, "max-attempts");
        positiveMillis(leaseDuration, "lease-duration");
        try {
            Math.multiplyExact(leaseDuration.toMillis(), 1_000L);
        } catch (ArithmeticException exception) {
            throw invalid("lease-duration", "is too large");
        }
        long initial = positiveMillis(initialRetryDelay, "initial-retry-delay");
        long maximum = positiveMillis(maxRetryDelay, "max-retry-delay");
        if (maximum < initial) {
            throw invalid("max-retry-delay", "must be at least initial-retry-delay");
        }
    }

    public void validateRetention() {
        if (!publishedRetentionEnabled) {
            return;
        }
        positiveMillis(publishedRetention, "published-retention");
        if (cleanupBatchSize < 1 || cleanupBatchSize > JdbcPublishedEventRetention.MAX_BATCH_SIZE) {
            throw invalid("cleanup-batch-size", "must be between 1 and "
                    + JdbcPublishedEventRetention.MAX_BATCH_SIZE);
        }
        positiveMillis(cleanupInterval, "cleanup-interval");
    }

    public void validateMetricsSnapshot() {
        positiveMillis(metricsSnapshotInterval, "metrics-snapshot-interval");
        long queryMillis = positiveMillis(metricsSnapshotQueryTimeout, "metrics-snapshot-query-timeout");
        long totalMillis = positiveMillis(metricsSnapshotTimeout, "metrics-snapshot-timeout");
        positiveMillis(metricsSnapshotShutdownTimeout, "metrics-snapshot-shutdown-timeout");
        if (totalMillis < queryMillis) {
            throw invalid("metrics-snapshot-timeout", "must be at least metrics-snapshot-query-timeout");
        }
        try {
            metricsSnapshotInterval.toNanos();
            metricsSnapshotTimeout.toNanos();
            metricsSnapshotShutdownTimeout.toNanos();
        } catch (ArithmeticException exception) {
            throw invalid("metrics-snapshot-timeout", "is too large");
        }
    }

    public void validateMetricsSnapshotShutdown() {
        validateMetricsSnapshot();
        if (metricsSnapshotShutdownTimeout.compareTo(shutdownTimeout) > 0) {
            throw invalid("metrics-snapshot-shutdown-timeout", "must not exceed shutdown-timeout");
        }
    }

    private static void positive(int value, String path) {
        if (value <= 0) { throw invalid(path, "must be positive"); }
    }

    private static long positiveMillis(Duration value, String path) {
        if (value == null) {
            throw invalid(path, "must be at least 1 ms");
        }
        try {
            long millis = value.toMillis();
            if (millis <= 0) {
                throw invalid(path, "must be at least 1 ms");
            }
            return millis;
        } catch (ArithmeticException exception) {
            throw invalid(path, "is too large");
        }
    }

    private static IllegalArgumentException invalid(String path, String message) {
        return new IllegalArgumentException("reliable-event." + path + " " + message);
    }

}
