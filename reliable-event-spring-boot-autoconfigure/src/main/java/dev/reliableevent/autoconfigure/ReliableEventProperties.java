package dev.reliableevent.autoconfigure;

import dev.reliableevent.rocketmq.RocketMqDestination;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;

@ConfigurationProperties(prefix = "reliable-event", ignoreUnknownFields = false)
public class ReliableEventProperties {

    private boolean enabled = true;
    private int claimBatchSize = 50;
    private int recoveryBatchSize = 50;
    private boolean schedulingEnabled = true;
    private Duration pollInterval = Duration.ofSeconds(1);
    private int workerThreads = 8;
    private int workerQueueCapacity = 200;
    private Duration leaseDuration = Duration.ofSeconds(30);
    private int maxAttempts = 8;
    private Duration initialRetryDelay = Duration.ofSeconds(1);
    private Duration maxRetryDelay = Duration.ofMinutes(5);
    private RocketMq rocketmq = new RocketMq();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public int getClaimBatchSize() { return claimBatchSize; }
    public void setClaimBatchSize(int claimBatchSize) { this.claimBatchSize = claimBatchSize; }
    public int getRecoveryBatchSize() { return recoveryBatchSize; }
    public void setRecoveryBatchSize(int recoveryBatchSize) { this.recoveryBatchSize = recoveryBatchSize; }
    public boolean isSchedulingEnabled() { return schedulingEnabled; }
    public void setSchedulingEnabled(boolean schedulingEnabled) { this.schedulingEnabled = schedulingEnabled; }
    public Duration getPollInterval() { return pollInterval; }
    public void setPollInterval(Duration pollInterval) { this.pollInterval = pollInterval; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public int getWorkerQueueCapacity() { return workerQueueCapacity; }
    public void setWorkerQueueCapacity(int workerQueueCapacity) { this.workerQueueCapacity = workerQueueCapacity; }
    public Duration getLeaseDuration() { return leaseDuration; }
    public void setLeaseDuration(Duration leaseDuration) { this.leaseDuration = leaseDuration; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Duration getInitialRetryDelay() { return initialRetryDelay; }
    public void setInitialRetryDelay(Duration initialRetryDelay) { this.initialRetryDelay = initialRetryDelay; }
    public Duration getMaxRetryDelay() { return maxRetryDelay; }
    public void setMaxRetryDelay(Duration maxRetryDelay) { this.maxRetryDelay = maxRetryDelay; }
    public RocketMq getRocketmq() { return rocketmq; }
    public void setRocketmq(RocketMq rocketmq) { this.rocketmq = rocketmq; }

    public void validateCore() {
        positive(claimBatchSize, "claim-batch-size");
        positive(recoveryBatchSize, "recovery-batch-size");
        positiveMillis(pollInterval, "poll-interval");
        positive(workerThreads, "worker-threads");
        if (workerQueueCapacity < 0) {
            throw invalid("worker-queue-capacity", "must not be negative");
        }
        if ((long) workerThreads + workerQueueCapacity > Integer.MAX_VALUE) {
            throw invalid("worker-threads/worker-queue-capacity", "combined capacity is too large");
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

    public void validateRocketMq(boolean requireEndpoint, boolean requireMappings) {
        if (rocketmq == null) {
            throw invalid("rocketmq", "must not be null");
        }
        positiveMillis(rocketmq.requestTimeout, "rocketmq.request-timeout");
        positive(rocketmq.maxBodyBytes, "rocketmq.max-body-bytes");
        if (requireEndpoint && (rocketmq.endpoints == null || rocketmq.endpoints.isBlank())) {
            throw invalid("rocketmq.endpoints", "must not be blank");
        }
        if (requireEndpoint && rocketmq.endpoints.chars().anyMatch(Character::isWhitespace)) {
            throw invalid("rocketmq.endpoints", "must not contain whitespace");
        }
        if (requireEndpoint && leaseDuration.compareTo(rocketmq.requestTimeout) <= 0) {
            throw invalid("lease-duration", "must exceed rocketmq.request-timeout");
        }
        if (requireMappings) {
            destinations();
        }
        boolean accessPresent = rocketmq.accessKey != null && !rocketmq.accessKey.isBlank();
        boolean secretPresent = rocketmq.secretKey != null && !rocketmq.secretKey.isBlank();
        if (accessPresent != secretPresent) {
            throw invalid("rocketmq.access-key/secret-key", "must be configured together");
        }
    }

    public Map<String, RocketMqDestination> destinations() {
        if (rocketmq == null || rocketmq.mappings == null || rocketmq.mappings.isEmpty()) {
            throw invalid("rocketmq.mappings", "must not be empty");
        }
        Map<String, RocketMqDestination> parsed = new LinkedHashMap<>();
        rocketmq.mappings.forEach((eventType, mapping) -> {
            if (eventType == null || eventType.isBlank()) {
                throw invalid("rocketmq.mappings", "event type must not be blank");
            }
            String destination = mapping == null ? null : mapping.destination;
            if (destination == null || destination.isBlank()) {
                throw invalid("rocketmq.mappings." + eventType + ".destination", "must not be blank");
            }
            int colon = destination.indexOf(':');
            if (colon != destination.lastIndexOf(':')) {
                throw invalid("rocketmq.mappings." + eventType + ".destination", "must use topic[:tag]");
            }
            String topic = colon < 0 ? destination : destination.substring(0, colon);
            String tag = colon < 0 ? null : destination.substring(colon + 1);
            try {
                parsed.put(eventType, new RocketMqDestination(topic, tag));
            } catch (IllegalArgumentException exception) {
                throw invalid("rocketmq.mappings." + eventType + ".destination", exception.getMessage());
            }
        });
        return Collections.unmodifiableMap(parsed);
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

    public static class RocketMq {
        private String endpoints;
        private Duration requestTimeout = Duration.ofSeconds(5);
        private int maxBodyBytes = 4 * 1024 * 1024;
        private String accessKey;
        private String secretKey;
        private boolean sslEnabled;
        private Map<String, Mapping> mappings = new LinkedHashMap<>();

        public String getEndpoints() { return endpoints; }
        public void setEndpoints(String endpoints) { this.endpoints = endpoints; }
        public Duration getRequestTimeout() { return requestTimeout; }
        public void setRequestTimeout(Duration requestTimeout) { this.requestTimeout = requestTimeout; }
        public int getMaxBodyBytes() { return maxBodyBytes; }
        public void setMaxBodyBytes(int maxBodyBytes) { this.maxBodyBytes = maxBodyBytes; }
        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }
        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
        public boolean isSslEnabled() { return sslEnabled; }
        public void setSslEnabled(boolean sslEnabled) { this.sslEnabled = sslEnabled; }
        public Map<String, Mapping> getMappings() { return mappings; }
        public void setMappings(Map<String, Mapping> mappings) { this.mappings = mappings; }
    }

    public static class Mapping {
        private String destination;
        public String getDestination() { return destination; }
        public void setDestination(String destination) { this.destination = destination; }
    }
}
