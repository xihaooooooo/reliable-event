package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.JdbcReliableEventPublisher;
import dev.reliableevent.jdbc.DeadEventOperations;
import dev.reliableevent.jdbc.JdbcDeadEventOperations;
import dev.reliableevent.jdbc.JdbcPublishedEventRetention;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.tracing.RegistrationTracer;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
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
                ReliableEventProperties properties,
                ObjectProvider<RegistrationTracer> registrationTracers
        ) {
            properties.validateCore();
            RegistrationTracer registrationTracer = properties.isTracingEnabled()
                    ? registrationTracers.getIfAvailable(() -> RegistrationTracer.NOOP)
                    : RegistrationTracer.NOOP;
            return new JdbcReliableEventPublisher(jdbcTemplate, objectMapper, properties.getMaxAttempts(),
                    registrationTracer);
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
    @ConditionalOnClass({JdbcTemplate.class, JdbcDeadEventOperations.class})
    @ConditionalOnBean({JdbcTemplate.class, PlatformTransactionManager.class})
    @ConditionalOnSingleCandidate(DataSource.class)
    @ConditionalOnProperty(prefix = "reliable-event", name = "dead-operations-enabled", havingValue = "true")
    static class DeadEventOperationsConfiguration {

        @Bean
        @ConditionalOnMissingBean(DeadEventOperations.class)
        DeadEventOperations deadEventOperations(JdbcTemplate jdbcTemplate,
                                                 PlatformTransactionManager transactionManager) {
            return new JdbcDeadEventOperations(jdbcTemplate, transactionManager);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({JdbcTemplate.class, JdbcPublishedEventRetention.class})
    @ConditionalOnBean({JdbcTemplate.class, PlatformTransactionManager.class})
    @ConditionalOnSingleCandidate(DataSource.class)
    @ConditionalOnProperty(prefix = "reliable-event", name = "published-retention-enabled", havingValue = "true")
    static class PublishedRetentionConfiguration {

        @Bean
        @ConditionalOnMissingBean(JdbcPublishedEventRetention.class)
        JdbcPublishedEventRetention publishedEventRetention(JdbcTemplate jdbcTemplate,
                                                            PlatformTransactionManager manager,
                                                            ReliableEventProperties properties) {
            properties.validateRetention();
            return new JdbcPublishedEventRetention(jdbcTemplate, manager);
        }

        @Bean
        @ConditionalOnMissingBean(PublishedRetentionScheduler.class)
        PublishedRetentionScheduler publishedRetentionScheduler(JdbcPublishedEventRetention retention,
                                                                 ReliableEventProperties properties,
                                                                 ObjectProvider<PublicationObserver> observers) {
            properties.validateRetention();
            return new PublishedRetentionScheduler(retention,
                    observers.getIfAvailable(() -> PublicationObserver.NOOP),
                    properties.getPublishedRetention(), properties.getCleanupBatchSize(),
                    properties.getCleanupInterval(), properties.getShutdownTimeout());
        }
    }


}
