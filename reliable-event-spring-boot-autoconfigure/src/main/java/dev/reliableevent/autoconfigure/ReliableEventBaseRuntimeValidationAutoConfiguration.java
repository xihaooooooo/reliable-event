package dev.reliableevent.autoconfigure;

import dev.reliableevent.internal.publication.EventSender;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = ReliableEventTransportBridgeAutoConfiguration.class)
@ConditionalOnProperty(prefix = "reliable-event", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ReliableEventBaseRuntimeValidationAutoConfiguration {

    @Bean
    SmartInitializingSingleton reliableEventBaseSenderRequirement(
            ReliableEventProperties properties,
            ListableBeanFactory beans) {
        return () -> {
            if (properties.isSchedulingEnabled()
                    && beans.getBeanNamesForType(EventSender.class).length == 0) {
                throw new IllegalStateException(
                        "ReliableEvent scheduling is enabled but no EventSender or EventTransport is configured; "
                                + "install a transport starter, provide a custom sender, or set "
                                + "reliable-event.scheduling-enabled=false for an ingress-only instance");
            }
        };
    }
}
