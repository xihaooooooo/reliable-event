package dev.reliableevent.kafka.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "reliable-event.kafka", ignoreUnknownFields = false)
public class KafkaProperties {
    private String bootstrapServers;
    private Duration maxBlock = Duration.ofSeconds(1);
    private Duration requestTimeout = Duration.ofSeconds(3);
    private Duration deliveryTimeout = Duration.ofSeconds(5);
    private Duration linger = Duration.ZERO;
    private Duration customProducerSendBudget;
    private Duration shutdownTimeout = Duration.ofSeconds(5);
    private int maxBodyBytes = 4 * 1024 * 1024;
    private Map<String, Mapping> mappings = new LinkedHashMap<>();
    private Map<String, String> producerProperties = new LinkedHashMap<>();

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
    public Duration getMaxBlock() { return maxBlock; }
    public void setMaxBlock(Duration maxBlock) { this.maxBlock = maxBlock; }
    public Duration getRequestTimeout() { return requestTimeout; }
    public void setRequestTimeout(Duration requestTimeout) { this.requestTimeout = requestTimeout; }
    public Duration getDeliveryTimeout() { return deliveryTimeout; }
    public void setDeliveryTimeout(Duration deliveryTimeout) { this.deliveryTimeout = deliveryTimeout; }
    public Duration getLinger() { return linger; }
    public void setLinger(Duration linger) { this.linger = linger; }
    public Duration getCustomProducerSendBudget() { return customProducerSendBudget; }
    public void setCustomProducerSendBudget(Duration budget) { this.customProducerSendBudget = budget; }
    public Duration getShutdownTimeout() { return shutdownTimeout; }
    public void setShutdownTimeout(Duration shutdownTimeout) { this.shutdownTimeout = shutdownTimeout; }
    public int getMaxBodyBytes() { return maxBodyBytes; }
    public void setMaxBodyBytes(int maxBodyBytes) { this.maxBodyBytes = maxBodyBytes; }
    public Map<String, Mapping> getMappings() { return mappings; }
    public void setMappings(Map<String, Mapping> mappings) { this.mappings = mappings; }
    public Map<String, String> getProducerProperties() { return producerProperties; }
    public void setProducerProperties(Map<String, String> producerProperties) { this.producerProperties = producerProperties; }

    public static class Mapping {
        private String topic;
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }
}
