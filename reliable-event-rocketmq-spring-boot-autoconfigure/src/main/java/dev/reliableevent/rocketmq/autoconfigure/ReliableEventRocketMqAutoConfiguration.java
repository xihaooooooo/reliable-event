package dev.reliableevent.rocketmq.autoconfigure;

import dev.reliableevent.autoconfigure.ReliableEventAutoConfiguration;
import dev.reliableevent.autoconfigure.ReliableEventProperties;
import dev.reliableevent.autoconfigure.ReliableEventTransportSelection;
import dev.reliableevent.autoconfigure.ReliableEventTransportSelectionAutoConfiguration;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.rocketmq.EventDestinationResolver;
import dev.reliableevent.rocketmq.MapEventDestinationResolver;
import dev.reliableevent.rocketmq.RocketMqDestination;
import dev.reliableevent.rocketmq.RocketMqEventTransport;
import dev.reliableevent.spi.EventTransport;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentials;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
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

@AutoConfiguration(after = {
        ReliableEventAutoConfiguration.class,
        ReliableEventTransportSelectionAutoConfiguration.class
}, beforeName = {
        "dev.reliableevent.autoconfigure.ReliableEventTransportBridgeAutoConfiguration",
        "dev.reliableevent.autoconfigure.ReliableEventPublicationAutoConfiguration"
})
@ConditionalOnClass(name = {
        "org.apache.rocketmq.client.apis.ClientServiceProvider",
        "org.apache.rocketmq.client.apis.producer.Producer"
})
@ConditionalOnProperty(prefix = "reliable-event", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RocketMqProperties.class)
public class ReliableEventRocketMqAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RocketMqTransportAdapter.class)
    RocketMqTransportAdapter reliableEventRocketMqAdapter() {
        return new RocketMqTransportAdapter();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = {
            "org.springframework.jdbc.core.JdbcTemplate",
            "org.springframework.transaction.PlatformTransactionManager"
    })
    @ConditionalOnBean(type = {
            "com.fasterxml.jackson.databind.ObjectMapper",
            "org.springframework.jdbc.core.JdbcTemplate",
            "org.springframework.transaction.PlatformTransactionManager"
    })
    @ConditionalOnSingleCandidate(type = "javax.sql.DataSource")
    @ConditionalOnMissingBean({EventSender.class, EventTransport.class})
    @ConditionalOnProperty(prefix = "reliable-event", name = "transport", havingValue = "rocketmq", matchIfMissing = true)
    static class SelectedRocketMqConfiguration {

        @Bean
        @ConditionalOnMissingBean(EventDestinationResolver.class)
        EventDestinationResolver reliableEventDestinationResolver(
                RocketMqProperties properties,
                ReliableEventTransportSelection selection) {
            requireRocket(selection);
            return new MapEventDestinationResolver(validatedMappings(properties));
        }

        @Bean
        @ConditionalOnMissingBean(ClientServiceProvider.class)
        ClientServiceProvider reliableEventClientServiceProvider(ReliableEventTransportSelection selection) {
            requireRocket(selection);
            return ClientServiceProvider.loadService();
        }

        @Bean
        @ConditionalOnMissingBean({ClientConfiguration.class, Producer.class})
        ClientConfiguration reliableEventClientConfiguration(
                RocketMqProperties properties,
                ObjectProvider<SessionCredentialsProvider> credentials,
                ReliableEventTransportSelection selection) {
            requireRocket(selection);
            validatePositiveTimeout(properties.getRequestTimeout());
            validateEndpoints(properties.getEndpoints());
            validateCredentialPair(properties);

            ClientConfigurationBuilder builder = ClientConfiguration.newBuilder()
                    .setEndpoints(properties.getEndpoints())
                    .setRequestTimeout(properties.getRequestTimeout())
                    .enableSsl(properties.isSslEnabled());
            SessionCredentialsProvider providedCredentials = credentials.getIfAvailable();
            if (providedCredentials != null) {
                builder.setCredentialProvider(providedCredentials);
            } else if (hasStaticCredentials(properties)) {
                builder.setCredentialProvider(() -> new SessionCredentials(
                        properties.getAccessKey(), properties.getSecretKey()));
            }
            return builder.build();
        }

        @Bean
        @ConditionalOnMissingBean({Producer.class, EventSender.class, EventTransport.class})
        Producer reliableEventProducer(
                ClientServiceProvider provider,
                ClientConfiguration configuration,
                EventDestinationResolver resolver,
                RocketMqProperties properties,
                ReliableEventProperties commonProperties,
                ReliableEventTransportSelection selection) throws ClientException {
            requireRocket(selection);
            commonProperties.validateCore();
            validatePositiveTimeout(configuration.getRequestTimeout());
            if (commonProperties.getLeaseDuration().compareTo(configuration.getRequestTimeout()) <= 0) {
                throw new IllegalArgumentException(
                        "reliable-event.lease-duration must exceed RocketMQ client request timeout");
            }

            Map<String, RocketMqDestination> destinations;
            try {
                destinations = validatedMappings(properties);
            } catch (IllegalArgumentException exception) {
                if (!(resolver instanceof MapEventDestinationResolver)) {
                    throw new IllegalArgumentException(
                            "Custom EventDestinationResolver requires a custom Producer", exception);
                }
                throw exception;
            }
            String[] topics = destinations.values().stream()
                    .map(RocketMqDestination::topic)
                    .distinct()
                    .sorted()
                    .toArray(String[]::new);
            return provider.newProducerBuilder()
                    .setClientConfiguration(configuration)
                    .setTopics(topics)
                    .setMaxAttempts(1)
                    .build();
        }

        @Bean
        @ConditionalOnMissingBean({EventSender.class, EventTransport.class})
        @Role(org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE)
        @ConditionalOnBean(Producer.class)
        EventTransport reliableEventRocketMqTransport(
                ClientServiceProvider provider,
                Producer producer,
                EventDestinationResolver resolver,
                RocketMqProperties properties,
                ReliableEventTransportSelection selection) {
            requireRocket(selection);
            validatePositive(properties.getMaxBodyBytes(), "max-body-bytes");
            return new RocketMqEventTransport(provider, producer, resolver, properties.getMaxBodyBytes());
        }
    }

    static Map<String, RocketMqDestination> validatedMappings(RocketMqProperties properties) {
        if (properties.getMappings() == null || properties.getMappings().isEmpty()) {
            throw invalid("mappings", "must not be empty");
        }

        Map<String, RocketMqDestination> destinations = new LinkedHashMap<>();
        properties.getMappings().forEach((eventType, mapping) -> {
            String destination = mapping == null ? null : mapping.getDestination();
            if (eventType == null || eventType.isBlank() || destination == null || destination.isBlank()) {
                throw invalid("mappings", "event type and destination must not be blank");
            }
            int separator = destination.indexOf(':');
            if (separator != destination.lastIndexOf(':')) {
                throw invalid("mappings." + eventType + ".destination", "must use topic[:tag]");
            }
            try {
                String topic = separator < 0 ? destination : destination.substring(0, separator);
                String tag = separator < 0 ? null : destination.substring(separator + 1);
                destinations.put(eventType, new RocketMqDestination(topic, tag));
            } catch (IllegalArgumentException exception) {
                throw invalid("mappings." + eventType + ".destination", exception.getMessage());
            }
        });
        return destinations;
    }

    private static void requireRocket(ReliableEventTransportSelection selection) {
        if (!selection.transport().equals("rocketmq")) {
            throw new IllegalStateException("RocketMQ client creation requires selected transport=rocketmq");
        }
    }

    private static void validatePositiveTimeout(Duration timeout) {
        if (timeout == null || timeout.toMillis() < 1) {
            throw invalid("request-timeout", "must be at least 1 ms");
        }
    }

    private static void validateEndpoints(String endpoints) {
        if (endpoints == null || endpoints.isBlank()
                || endpoints.chars().anyMatch(Character::isWhitespace)) {
            throw invalid("endpoints", "must be configured without whitespace");
        }
    }

    private static void validateCredentialPair(RocketMqProperties properties) {
        boolean hasAccessKey = properties.getAccessKey() != null && !properties.getAccessKey().isBlank();
        boolean hasSecretKey = properties.getSecretKey() != null && !properties.getSecretKey().isBlank();
        if (hasAccessKey != hasSecretKey) {
            throw invalid("access-key/secret-key", "must be configured together");
        }
    }

    private static boolean hasStaticCredentials(RocketMqProperties properties) {
        return properties.getAccessKey() != null && !properties.getAccessKey().isBlank();
    }

    private static void validatePositive(int value, String property) {
        if (value <= 0) {
            throw invalid(property, "must be positive");
        }
    }

    private static IllegalArgumentException invalid(String property, String message) {
        return new IllegalArgumentException("reliable-event.rocketmq." + property + " " + message);
    }
}
