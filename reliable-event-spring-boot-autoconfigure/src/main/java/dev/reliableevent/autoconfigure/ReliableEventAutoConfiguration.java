package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.JdbcReliableEventPublisher;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.rocketmq.EventDestinationResolver;
import dev.reliableevent.rocketmq.MapEventDestinationResolver;
import dev.reliableevent.rocketmq.RocketMqDestination;
import dev.reliableevent.rocketmq.RocketMqEventSender;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration",
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration",
        "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration"
})
@ConditionalOnProperty(prefix = "reliable-event", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(ReliableEventProperties.class)
public class ReliableEventAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({JdbcTemplate.class, JdbcReliableEventPublisher.class})
    @ConditionalOnBean({JdbcTemplate.class, ObjectMapper.class, PlatformTransactionManager.class})
    @ConditionalOnSingleCandidate(DataSource.class)
    static class JdbcConfiguration {

        @Bean
        @ConditionalOnMissingBean(ReliableEventPublisher.class)
        ReliableEventPublisher reliableEventPublisher(
                JdbcTemplate jdbcTemplate,
                ObjectMapper objectMapper,
                ReliableEventProperties properties
        ) {
            properties.validateCore();
            return new JdbcReliableEventPublisher(jdbcTemplate, objectMapper, properties.getMaxAttempts());
        }

        @Bean
        @ConditionalOnMissingBean(ExponentialBackoff.class)
        ExponentialBackoff reliableEventBackoff(ReliableEventProperties properties) {
            properties.validateCore();
            return new ExponentialBackoff(
                    properties.getInitialRetryDelay(),
                    properties.getMaxRetryDelay(),
                    0.2,
                    () -> ThreadLocalRandom.current().nextDouble()
            );
        }

        @Bean
        @ConditionalOnMissingBean(JdbcExpiredLeaseRecovery.class)
        JdbcExpiredLeaseRecovery reliableEventRecovery(
                JdbcTemplate jdbcTemplate,
                PlatformTransactionManager transactionManager,
                ExponentialBackoff backoff,
                ReliableEventProperties properties,
                ObjectProvider<PublicationObserver> observers
        ) {
            properties.validateCore();
            return new JdbcExpiredLeaseRecovery(
                    jdbcTemplate, transactionManager, properties.getRecoveryBatchSize(), backoff,
                    observers.getIfAvailable(() -> PublicationObserver.NOOP)
            );
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({RocketMqEventSender.class, ClientServiceProvider.class, JdbcTemplate.class})
    @ConditionalOnBean({ObjectMapper.class, JdbcTemplate.class, PlatformTransactionManager.class})
    @ConditionalOnSingleCandidate(DataSource.class)
    @ConditionalOnMissingBean(EventSender.class)
    static class RocketMqConfiguration {

        @Bean
        @ConditionalOnMissingBean(EventDestinationResolver.class)
        EventDestinationResolver reliableEventDestinationResolver(ReliableEventProperties properties) {
            return new MapEventDestinationResolver(properties.destinations());
        }

        @Bean
        @ConditionalOnMissingBean(ClientServiceProvider.class)
        ClientServiceProvider reliableEventClientServiceProvider() {
            return ClientServiceProvider.loadService();
        }

        @Bean
        @ConditionalOnMissingBean(EventSender.class)
        EventSender reliableEventSender(
                ClientServiceProvider provider,
                Producer producer,
                EventDestinationResolver resolver,
                ObjectMapper objectMapper,
                ReliableEventProperties properties
        ) {
            properties.validateRocketMq(false, false);
            return new RocketMqEventSender(
                    provider, producer, resolver, objectMapper, properties.getRocketmq().getMaxBodyBytes()
            );
        }

        @Configuration(proxyBeanMethods = false)
        @ConditionalOnMissingBean(Producer.class)
        static class DefaultProducerConfiguration {

            @Bean
            @ConditionalOnMissingBean(ClientConfiguration.class)
            ClientConfiguration reliableEventClientConfiguration(
                    ReliableEventProperties properties,
                    ObjectProvider<SessionCredentialsProvider> credentials
            ) {
                properties.validateCore();
                properties.validateRocketMq(true, false);
                ReliableEventProperties.RocketMq settings = properties.getRocketmq();
                var builder = ClientConfiguration.newBuilder()
                        .setEndpoints(settings.getEndpoints())
                        .setRequestTimeout(settings.getRequestTimeout())
                        .enableSsl(settings.isSslEnabled());
                SessionCredentialsProvider supplied = credentials.getIfAvailable();
                if (supplied != null) {
                    builder.setCredentialProvider(supplied);
                } else if (settings.getAccessKey() != null && !settings.getAccessKey().isBlank()) {
                    builder.setCredentialProvider(new StaticSessionCredentialsProvider(
                            settings.getAccessKey(), settings.getSecretKey()
                    ));
                }
                return builder.build();
            }

            @Bean(destroyMethod = "close")
            Producer reliableEventProducer(
                    ClientServiceProvider provider,
                    ClientConfiguration configuration,
                    EventDestinationResolver resolver,
                    ReliableEventProperties properties
            ) throws org.apache.rocketmq.client.apis.ClientException {
                properties.validateCore();
                properties.validateRocketMq(false, false);
                if (configuration.getRequestTimeout() == null
                        || properties.getLeaseDuration().compareTo(configuration.getRequestTimeout()) <= 0) {
                    throw new IllegalArgumentException(
                            "reliable-event.lease-duration must exceed RocketMQ client request timeout"
                    );
                }
                Map<String, RocketMqDestination> destinations;
                try {
                    destinations = properties.destinations();
                } catch (IllegalArgumentException exception) {
                    if (!(resolver instanceof MapEventDestinationResolver)) {
                        throw new IllegalArgumentException(
                                "Custom EventDestinationResolver requires a custom Producer", exception
                        );
                    }
                    throw exception;
                }
                String[] topics = destinations.values().stream()
                        .map(RocketMqDestination::topic).distinct().sorted().toArray(String[]::new);
                return provider.newProducerBuilder()
                        .setClientConfiguration(configuration)
                        .setTopics(topics)
                        .setMaxAttempts(1)
                        .build();
            }
        }
    }
}
