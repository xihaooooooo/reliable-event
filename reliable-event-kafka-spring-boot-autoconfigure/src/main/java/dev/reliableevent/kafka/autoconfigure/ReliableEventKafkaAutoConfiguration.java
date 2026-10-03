package dev.reliableevent.kafka.autoconfigure;

import dev.reliableevent.autoconfigure.ReliableEventAutoConfiguration;
import dev.reliableevent.autoconfigure.ReliableEventProperties;
import dev.reliableevent.autoconfigure.ReliableEventTransportSelection;
import dev.reliableevent.autoconfigure.ReliableEventTransportSelectionAutoConfiguration;
import dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.kafka.KafkaDestinationResolver;
import dev.reliableevent.kafka.KafkaEventTransport;
import dev.reliableevent.kafka.MapKafkaDestinationResolver;
import dev.reliableevent.spi.EventTransport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@AutoConfiguration(after = {
        ReliableEventAutoConfiguration.class,
        ReliableEventTransportSelectionAutoConfiguration.class
}, beforeName = {
        "dev.reliableevent.autoconfigure.ReliableEventTransportBridgeAutoConfiguration",
        "dev.reliableevent.autoconfigure.ReliableEventPublicationAutoConfiguration"
})
@ConditionalOnClass(Producer.class)
@ConditionalOnProperty(prefix = "reliable-event", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(KafkaProperties.class)
public class ReliableEventKafkaAutoConfiguration {
    private static final Duration DATABASE_UPDATE_RESERVE = Duration.ofSeconds(1);
    private static final Duration FUTURE_COMPLETION_SLACK = Duration.ofSeconds(1);
    private static final long RECORD_OVERHEAD_RESERVE_BYTES = 32L * 1024;
    private static final Set<String> GUARDED_PROPERTIES = Set.of(
            ProducerConfig.ACKS_CONFIG,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            ProducerConfig.RETRIES_CONFIG,
            ProducerConfig.MAX_BLOCK_MS_CONFIG,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
            ProducerConfig.LINGER_MS_CONFIG,
            ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            ProducerConfig.PARTITIONER_CLASS_CONFIG,
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);

    @Bean
    @ConditionalOnMissingBean(name = "reliableEventKafkaTransportAdapter")
    TransportAdapterDescriptor reliableEventKafkaTransportAdapter() {
        return new KafkaTransportAdapter();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(value = ReliableEventTransportSelection.class, type = {
            "com.fasterxml.jackson.databind.ObjectMapper",
            "org.springframework.jdbc.core.JdbcTemplate",
            "org.springframework.transaction.PlatformTransactionManager"
    })
    @ConditionalOnMissingBean({EventSender.class, EventTransport.class})
    @ConditionalOnProperty(prefix = "reliable-event", name = "transport", havingValue = "kafka", matchIfMissing = true)
    @ConditionalOnClass(name = {
            "org.springframework.jdbc.core.JdbcTemplate",
            "org.springframework.transaction.PlatformTransactionManager"
    })
    @ConditionalOnSingleCandidate(type = "javax.sql.DataSource")
    static class SelectedKafkaConfiguration {

        @Bean
        @ConditionalOnMissingBean(KafkaDestinationResolver.class)
        KafkaDestinationResolver reliableEventKafkaDestinationResolver(
                ReliableEventTransportSelection selection,
                KafkaProperties properties) {
            requireKafka(selection);
            return new MapKafkaDestinationResolver(validatedMappings(properties));
        }

        @Bean(destroyMethod = "")
        @ConditionalOnMissingBean(Producer.class)
        ReliableEventKafkaProducer reliableEventKafkaProducer(
                ReliableEventTransportSelection selection,
                KafkaProperties properties,
                ReliableEventProperties commonProperties) {
            requireKafka(selection);
            commonProperties.validateCore();
            Map<String, Object> configuration = producerConfiguration(properties);
            validateDefaultBudget(properties, commonProperties);
            return new ReliableEventKafkaProducer(configuration);
        }

        @Bean
        @ConditionalOnMissingBean(KafkaProducerOwner.class)
        @ConditionalOnBean(ReliableEventKafkaProducer.class)
        KafkaProducerOwner reliableEventKafkaProducerOwner(
                ReliableEventTransportSelection selection,
                ReliableEventKafkaProducer reliableEventKafkaProducer,
                KafkaProperties properties) {
            requireKafka(selection);
            return new KafkaProducerOwner(reliableEventKafkaProducer, properties.getShutdownTimeout());
        }

        @Bean
        @ConditionalOnMissingBean(EventTransport.class)
        @Role(org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE)
        EventTransport reliableEventKafkaTransport(
                ReliableEventTransportSelection selection,
                ObjectProvider<Producer<String, byte[]>> producers,
                ObjectProvider<KafkaDestinationResolver> resolvers,
                KafkaProperties properties,
                ReliableEventProperties commonProperties) {
            requireKafka(selection);
            commonProperties.validateCore();
            java.util.List<Producer<String, byte[]>> candidates = producers.stream().toList();
            if (candidates.size() != 1) {
                throw new IllegalStateException(
                        "Selected Kafka transport requires exactly one Producer<String, byte[]> bean; found "
                                + candidates.size());
            }
            Producer<String, byte[]> producer = candidates.get(0);
            boolean frameworkProducer = producer instanceof ReliableEventKafkaProducer;
            Duration sendBudget = frameworkProducer
                    ? defaultSendBudget(properties)
                    : requiredCustomBudget(properties);
            validateLease(commonProperties.getLeaseDuration(), sendBudget);
            validateTransportSettings(properties);
            return new KafkaEventTransport(producer, resolvers.getObject(), sendBudget,
                    properties.getMaxBodyBytes());
        }
    }

    private static void requireKafka(ReliableEventTransportSelection selection) {
        if (!"kafka".equals(selection.transport())) {
            throw new IllegalStateException("Kafka client creation requires selected transport=kafka");
        }
    }

    static Map<String, String> validatedMappings(KafkaProperties properties) {
        if (properties.getMappings() == null || properties.getMappings().isEmpty()) {
            throw invalid("mappings", "must not be empty");
        }
        Map<String, String> result = new LinkedHashMap<>();
        properties.getMappings().forEach((eventType, mapping) -> {
            String topic = mapping == null ? null : mapping.getTopic();
            if (eventType == null || eventType.isBlank() || topic == null || topic.isBlank()) {
                throw invalid("mappings", "event type and topic must not be blank");
            }
            result.put(eventType, topic);
        });
        return result;
    }

    static Map<String, Object> producerConfiguration(KafkaProperties properties) {
        validateProducerProperties(properties);
        requireDuration(properties.getMaxBlock(), "max-block");
        requireDuration(properties.getRequestTimeout(), "request-timeout");
        requireDuration(properties.getDeliveryTimeout(), "delivery-timeout");
        requireNonNegative(properties.getLinger(), "linger");
        validateTransportSettings(properties);
        if (properties.getMaxBodyBytes() < 1) {
            throw invalid("max-body-bytes", "must be positive");
        }
        long maxRequestSize = (long) properties.getMaxBodyBytes() + RECORD_OVERHEAD_RESERVE_BYTES;
        if (maxRequestSize > Integer.MAX_VALUE) {
            throw invalid("max-body-bytes", "plus Kafka record overhead must fit in max-request-size");
        }
        if (properties.getBootstrapServers() == null || properties.getBootstrapServers().isBlank()) {
            throw invalid("bootstrap-servers", "must be configured");
        }
        if (properties.getDeliveryTimeout().compareTo(properties.getRequestTimeout().plus(properties.getLinger())) < 0) {
            throw invalid("delivery-timeout", "must be at least request-timeout plus linger");
        }
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, durationMillis(properties.getMaxBlock(), "max-block"));
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                durationMillis(properties.getRequestTimeout(), "request-timeout"));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                durationMillis(properties.getDeliveryTimeout(), "delivery-timeout"));
        config.put(ProducerConfig.LINGER_MS_CONFIG, durationMillis(properties.getLinger(), "linger"));
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, (int) maxRequestSize);
        properties.getProducerProperties().forEach(config::put);
        try {
            ProducerConfig.configDef().parse(config);
        } catch (RuntimeException invalidConfig) {
            throw invalid("producer-properties", "contains an invalid Kafka client configuration");
        }
        return config;
    }

    private static void validateProducerProperties(KafkaProperties properties) {
        if (properties.getProducerProperties() == null) {
            throw invalid("producer-properties", "must not be null");
        }
        Set<String> known = ProducerConfig.configDef().names();
        for (String key : properties.getProducerProperties().keySet()) {
            if (!known.contains(key)) {
                throw invalid("producer-properties." + key, "is not a Kafka producer setting");
            }
            if (GUARDED_PROPERTIES.contains(key)) {
                throw invalid("producer-properties." + key, "is controlled by the ReliableEvent send budget");
            }
        }
    }

    private static void validateDefaultBudget(KafkaProperties properties, ReliableEventProperties common) {
        validateLease(common.getLeaseDuration(), defaultSendBudget(properties));
    }

    private static Duration defaultSendBudget(KafkaProperties properties) {
        try {
            return properties.getMaxBlock().plus(properties.getDeliveryTimeout()).plus(FUTURE_COMPLETION_SLACK);
        } catch (ArithmeticException overflow) {
            throw invalid("max-block/delivery-timeout", "sum is too large");
        }
    }

    private static Duration requiredCustomBudget(KafkaProperties properties) {
        Duration budget = properties.getCustomProducerSendBudget();
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw invalid("custom-producer-send-budget",
                    "must be configured when a custom Producer<String, byte[]> is used");
        }
        return budget;
    }

    private static void validateLease(Duration lease, Duration sendBudget) {
        if (lease == null || lease.compareTo(sendBudget.plus(DATABASE_UPDATE_RESERVE)) <= 0) {
            throw new IllegalArgumentException(
                    "reliable-event.lease-duration must strictly exceed Kafka send budget plus one-second state-update reserve");
        }
    }

    private static void requireDuration(Duration duration, String property) {
        if (duration == null || duration.isZero() || duration.isNegative() || durationMillis(duration, property) < 1) {
            throw invalid(property, "must be at least 1 ms");
        }
    }

    private static void requireNonNegative(Duration duration, String property) {
        if (duration == null || duration.isNegative() || durationMillis(duration, property) < 0) {
            throw invalid(property, "must not be negative");
        }
    }

    private static int durationMillis(Duration duration, String property) {
        try {
            long millis = duration.toMillis();
            if (millis > Integer.MAX_VALUE) {
                throw invalid(property, "must fit in Kafka's integer millisecond setting");
            }
            return (int) millis;
        } catch (ArithmeticException overflow) {
            throw invalid(property, "is too large");
        }
    }

    private static void validateTransportSettings(KafkaProperties properties) {
        if (properties.getMaxBodyBytes() < 1) {
            throw invalid("max-body-bytes", "must be positive");
        }
        requireDuration(properties.getShutdownTimeout(), "shutdown-timeout");
    }

    private static IllegalArgumentException invalid(String property, String message) {
        return new IllegalArgumentException("reliable-event.kafka." + property + " " + message);
    }
}
