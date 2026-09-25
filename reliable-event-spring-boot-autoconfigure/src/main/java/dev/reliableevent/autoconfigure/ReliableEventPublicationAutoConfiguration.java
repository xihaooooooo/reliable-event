package dev.reliableevent.autoconfigure;

import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.internal.cycle.JdbcEventPublicationCycle;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.UUID;

@AutoConfiguration(after = ReliableEventAutoConfiguration.class)
@ConditionalOnProperty(prefix = "reliable-event", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({JdbcEventPublicationWorker.class, JdbcTemplate.class, EventSender.class})
@ConditionalOnBean({JdbcTemplate.class, EventSender.class, JdbcExpiredLeaseRecovery.class,
        PlatformTransactionManager.class})
@ConditionalOnSingleCandidate(DataSource.class)
public class ReliableEventPublicationAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(JdbcEventPublicationWorker.class)
    JdbcEventPublicationWorker reliableEventWorker(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            EventSender sender,
            ExponentialBackoff backoff,
            ReliableEventProperties properties
    ) {
        properties.validateCore();
        return new JdbcEventPublicationWorker(
                jdbcTemplate,
                transactionManager,
                sender,
                Clock.systemUTC(),
                properties.getClaimBatchSize(),
                "reliable-event-" + UUID.randomUUID(),
                properties.getLeaseDuration(),
                backoff
        );
    }

    @Bean
    @ConditionalOnMissingBean(JdbcEventPublicationCycle.class)
    JdbcEventPublicationCycle reliableEventPublicationCycle(
            JdbcExpiredLeaseRecovery recovery,
            JdbcEventPublicationWorker worker
    ) {
        return new JdbcEventPublicationCycle(recovery, worker);
    }

    @Bean
    @ConditionalOnProperty(prefix = "reliable-event", name = "scheduling-enabled",
            havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(ReliableEventScheduler.class)
    ReliableEventScheduler reliableEventScheduler(
            JdbcExpiredLeaseRecovery recovery,
            JdbcEventPublicationWorker worker,
            ReliableEventProperties properties
    ) {
        properties.validateCore();
        return new ReliableEventScheduler(
                recovery, worker, properties.getClaimBatchSize(),
                properties.getWorkerThreads(), properties.getWorkerQueueCapacity(),
                properties.getPollInterval()
        );
    }
}
