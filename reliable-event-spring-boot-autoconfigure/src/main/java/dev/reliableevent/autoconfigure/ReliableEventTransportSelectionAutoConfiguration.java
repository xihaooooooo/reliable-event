package dev.reliableevent.autoconfigure;

import dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.util.List;

@AutoConfiguration(beforeName = {
        "dev.reliableevent.autoconfigure.ReliableEventPublicationAutoConfiguration",
        "dev.reliableevent.autoconfigure.ReliableEventMetricsSnapshotAutoConfiguration"
})
@ConditionalOnProperty(prefix = "reliable-event", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ReliableEventTransportSelectionAutoConfiguration {

    @Bean
    ReliableEventTransportSelection reliableEventTransportSelection(
            Environment environment,
            List<TransportAdapterDescriptor> adapters,
            ListableBeanFactory beanFactory) {
        return ReliableEventTransportSelection.resolve(environment, adapters, beanFactory);
    }
}
