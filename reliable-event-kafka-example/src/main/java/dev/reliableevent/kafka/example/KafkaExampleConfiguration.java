package dev.reliableevent.kafka.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Configuration(proxyBeanMethods = false)
class KafkaExampleConfiguration {
    @Bean
    @ConditionalOnProperty(prefix = "example.kafka", name = "create-topic", havingValue = "true")
    TopicProvisioner kafkaExampleTopicProvisioner(
            @Value("${reliable-event.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${example.kafka.topic}") String topic,
            @Value("${example.kafka.partitions:1}") int partitions,
            @Value("${example.kafka.replication-factor:1}") short replicationFactor) {
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrapServers);
        config.put(org.apache.kafka.clients.CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
        config.put(org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        AdminClient admin = AdminClient.create(config);
        try {
            try {
                admin.createTopics(java.util.List.of(new NewTopic(topic, partitions, replicationFactor)))
                        .all().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException failure) {
                if (!(failure.getCause() instanceof TopicExistsException)) throw new IllegalStateException(
                        "Unable to create configured example Kafka topic", failure.getCause());
            } catch (TimeoutException timeout) {
                throw new IllegalStateException("Timed out creating configured example Kafka topic", timeout);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while creating example Kafka topic", interrupted);
            }
        } finally {
            admin.close(java.time.Duration.ofSeconds(5));
        }
        return new TopicProvisioner(topic);
    }

    @Bean
    @ConditionalOnProperty(prefix = "example.consumer", name = "enabled", havingValue = "true", matchIfMissing = true)
    ExampleConsumerLoop kafkaExampleConsumerLoop(
            @Value("${reliable-event.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${example.kafka.topic}") String topic,
            @Value("${example.kafka.consumer-group}") String group,
            OrderMessageHandler handler,
            @Value("${example.kafka.poll-timeout:500ms}") java.time.Duration pollTimeout) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new ExampleConsumerLoop(config, topic, pollTimeout, handler);
    }

    record TopicProvisioner(String topic) { }
}
