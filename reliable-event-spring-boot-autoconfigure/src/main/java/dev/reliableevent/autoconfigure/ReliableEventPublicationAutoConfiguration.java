package dev.reliableevent.autoconfigure;

import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.JdbcPublicationStore;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Configuration;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.UUID;

@AutoConfiguration(after = ReliableEventAutoConfiguration.class, afterName = {
        "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration"
})
@ConditionalOnProperty(prefix = "reliable-event", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({JdbcEventPublicationWorker.class, JdbcTemplate.class, EventTransport.class})
@ConditionalOnBean({JdbcTemplate.class, EventTransport.class, JdbcExpiredLeaseRecovery.class,
        PlatformTransactionManager.class})
@ConditionalOnSingleCandidate(DataSource.class)
public class ReliableEventPublicationAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(JdbcPublicationStore.class)
    JdbcPublicationStore reliableEventPublicationStore(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            ReliableEventProperties properties,
            ObjectProvider<PublicationObserver> observers
    ) {
        properties.validateCore();
        return new JdbcPublicationStore(jdbcTemplate, transactionManager, Clock.systemUTC(),
                "reliable-event-" + UUID.randomUUID(), properties.getLeaseDuration(),
                observers.getIfAvailable(() -> PublicationObserver.NOOP));
    }

    @Bean
    @ConditionalOnMissingBean(JdbcEventPublicationWorker.class)
    JdbcEventPublicationWorker reliableEventWorker(
            JdbcPublicationStore store,
            EventTransport transport,
            ExponentialBackoff backoff,
            ReliableEventProperties properties,
            ObjectProvider<PublicationObserver> observers,
            ObjectProvider<PublicationTracer> tracers
    ) {
        properties.validateCore();
        return new JdbcEventPublicationWorker(store, transport, Clock.systemUTC(), backoff,
                observers.getIfAvailable(() -> PublicationObserver.NOOP),
                properties.isTracingEnabled()
                        ? tracers.getIfAvailable(() -> PublicationTracer.NOOP)
                        : PublicationTracer.NOOP);
    }

    @Bean
    @ConditionalOnProperty(prefix = "reliable-event", name = "scheduling-enabled",
            havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(ReliableEventScheduler.class)
    ReliableEventScheduler reliableEventScheduler(
            JdbcExpiredLeaseRecovery recovery,
            JdbcPublicationStore store,
            JdbcEventPublicationWorker worker,
            ReliableEventProperties properties,
            ObjectProvider<PublicationObserver> observers
    ) {
        properties.validateCore();
        return new ReliableEventScheduler(
                recovery, store, worker, properties.getClaimBatchSize(),
                properties.getWorkerThreads(), properties.getWorkerQueueCapacity(),
                properties.getPollInterval(), properties.getShutdownTimeout(),
                properties.isAdaptivePollingEnabled(), properties.getActivePollInterval(),
                observers.getIfAvailable(() -> PublicationObserver.NOOP)
        );
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class MetricsConfiguration {

        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean(PublicationObserver.class)
        MicrometerPublicationObserver reliableEventMetrics(MeterRegistry registry, JdbcTemplate jdbcTemplate,
                                                            ReliableEventProperties properties) {
            properties.validateMetricsSnapshot();
            MicrometerPublicationObserver observer = new MicrometerPublicationObserver(
                    registry, new dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository(jdbcTemplate),
                    properties.getMetricsSnapshotQueryTimeout());
            observer.automaticSnapshotInterval(properties.getMetricsSnapshotInterval());
            return observer;
        }

    }
}
