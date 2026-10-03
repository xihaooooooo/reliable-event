package dev.reliableevent.autoconfigure;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.ConfigurationCondition;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Enables periodic sampling only when the complete default runtime owns its beans. */
final class DefaultMetricsRuntimeCondition implements ConfigurationCondition {

    private static final String AUTOCONFIG =
            "dev.reliableevent.autoconfigure.ReliableEventPublicationAutoConfiguration";
    private static final String METRICS_CONFIG = AUTOCONFIG + "$MetricsConfiguration";

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return matches(context.getRegistry());
    }

    @Override
    public ConfigurationPhase getConfigurationPhase() {
        return ConfigurationPhase.REGISTER_BEAN;
    }

    static boolean matches(BeanDefinitionRegistry registry) {
        boolean metrics = owned(registry, "reliableEventMetrics", "reliableEventMetrics", METRICS_CONFIG);
        boolean worker = owned(registry, "reliableEventWorker", "reliableEventWorker", AUTOCONFIG);
        boolean cycle = owned(registry, "reliableEventPublicationCycle", "reliableEventPublicationCycle", AUTOCONFIG);
        boolean scheduler = owned(registry, "reliableEventScheduler", "reliableEventScheduler", AUTOCONFIG);
        boolean matched = metrics && worker && cycle && scheduler;
        return matched;
    }

    private static boolean owned(BeanDefinitionRegistry registry, String beanName,
                                 String method, String configurationClass) {
        if (!registry.containsBeanDefinition(beanName)) return false;
        BeanDefinition bean = registry.getBeanDefinition(beanName);
        if (!method.equals(bean.getFactoryMethodName())) return false;
        String factoryBeanName = bean.getFactoryBeanName();
        return configurationClass.equals(factoryBeanName);
    }
}
