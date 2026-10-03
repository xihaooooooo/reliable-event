package dev.reliableevent.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

@AutoConfiguration(after = ReliableEventPublicationAutoConfiguration.class)
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(prefix = "reliable-event", name = {"enabled", "scheduling-enabled",
        "metrics-snapshot-enabled"}, havingValue = "true", matchIfMissing = true)
@ConditionalOnBean(MeterRegistry.class)
@Conditional(DefaultMetricsRuntimeCondition.class)
public class ReliableEventMetricsSnapshotAutoConfiguration {

    @Bean
    MetricsSnapshotSampler reliableEventMetricsSnapshotSampler(
            ObjectProvider<MicrometerPublicationObserver> observers,
            ReliableEventProperties properties) {
        MicrometerPublicationObserver observer = observers.getIfAvailable();
        if (observer == null) throw new IllegalStateException("Default metrics observer is missing");
        properties.validateMetricsSnapshotShutdown();
        return new MetricsSnapshotSampler(observer, true, properties.getMetricsSnapshotInterval(),
                properties.getMetricsSnapshotTimeout(), properties.getMetricsSnapshotShutdownTimeout());
    }
}
