package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.spi.EventTransport;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(beforeName="dev.reliableevent.autoconfigure.ReliableEventPublicationAutoConfiguration")
@ConditionalOnProperty(prefix="reliable-event",name="enabled",havingValue="true",matchIfMissing=true)
public class ReliableEventTransportBridgeAutoConfiguration {
    @Bean @ConditionalOnBean(EventTransport.class) @ConditionalOnMissingBean(EventSender.class)
    @org.springframework.context.annotation.Role(org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE)
    EventSender reliableEventSender(ListableBeanFactory beanFactory,ObjectMapper mapper){
        var transports=beanFactory.getBeansOfType(EventTransport.class);
        if(transports.size()!=1) throw new IllegalStateException("Exactly one EventTransport bean is required; found "+transports.size());
        return new EventTransportSenderBridge(transports.values().iterator().next(),mapper);
    }
    @Bean @ConditionalOnBean(EventTransport.class)
    SmartInitializingSingleton reliableEventTransportEntryPointValidator(ListableBeanFactory factory){
        return ()->{boolean legacy=factory.getBeansOfType(EventSender.class).values().stream().anyMatch(sender->!(sender instanceof EventTransportSenderBridge));
            if(legacy)throw new IllegalStateException("Both EventTransport and legacy EventSender are configured; choose exactly one sending extension point");};
    }
}
