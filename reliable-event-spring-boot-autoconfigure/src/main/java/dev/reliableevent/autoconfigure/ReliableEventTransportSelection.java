package dev.reliableevent.autoconfigure;

import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.core.env.Environment;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/** Resolves the configured transport before any adapter creates its client. */
public record ReliableEventTransportSelection(String transport) {

    private static final Logger LOG = Logger.getLogger(ReliableEventTransportSelection.class.getName());

    public static ReliableEventTransportSelection resolve(
            Environment environment,
            List<TransportAdapterDescriptor> adapters,
            ListableBeanFactory beans) {
        String requested = Binder.get(environment)
                .bind("reliable-event.transport", String.class)
                .orElse(null);
        Set<ConfigurationPropertyName> publicProperties = publicPropertyNames();
        Set<String> transportNames = new HashSet<>();
        Set<ConfigurationPropertyName> namespaces = new HashSet<>();

        validateAdapters(adapters, publicProperties, transportNames, namespaces);
        Set<String> builtInRuntimeNames = runtimeBeanNames(adapters);
        validateRootProperties(environment, publicProperties, namespaces);

        long customSenderCount = Arrays.stream(beans.getBeanNamesForType(EventSender.class, false, false))
                .filter(name -> !isFrameworkBean(beans, name, builtInRuntimeNames))
                .count();
        long customTransportCount = Arrays.stream(beans.getBeanNamesForType(EventTransport.class, false, false))
                .filter(name -> !isFrameworkBean(beans, name, builtInRuntimeNames))
                .count();
        boolean customSender = customSenderCount > 0;
        boolean customTransport = customTransportCount > 0;

        if (customSenderCount > 1 || customTransportCount > 1) {
            throw new IllegalStateException(
                    "Multiple custom EventSender/EventTransport beans are configured; register exactly one entry point");
        }
        if (customSender && customTransport) {
            throw new IllegalStateException(
                    "Both EventSender and EventTransport are configured; choose exactly one custom extension point");
        }

        String selected = requested;
        if (selected == null) {
            if (customSender || customTransport) {
                selected = "custom";
            } else if (adapters.size() == 1) {
                selected = adapters.get(0).transportName();
            } else if (adapters.size() > 1) {
                throw new IllegalStateException(
                        "Multiple built-in transports are installed; set reliable-event.transport explicitly");
            } else {
                selected = "custom";
            }
        }

        validateSelection(selected, requested, transportNames, customSender, customTransport);
        logUnusedAdapterSettings(environment, adapters, selected);
        LOG.info("ReliableEvent selected transport=" + selected);
        return new ReliableEventTransportSelection(selected);
    }

    private static void validateAdapters(
            List<TransportAdapterDescriptor> adapters,
            Set<ConfigurationPropertyName> publicProperties,
            Set<String> transportNames,
            Set<ConfigurationPropertyName> namespaces) {
        for (TransportAdapterDescriptor adapter : adapters) {
            String transportName = adapter.transportName();
            String namespaceText = adapter.configurationNamespace();
            if (transportName == null
                    || !transportName.matches("[a-z][a-z0-9-]*")
                    || transportName.equals("custom")
                    || !transportNames.add(transportName)) {
                throw new IllegalStateException("Duplicate or invalid transport adapter registration");
            }

            ConfigurationPropertyName namespace = namespaceText == null
                    ? null
                    : ConfigurationPropertyName.adapt(namespaceText, '.');
            boolean shadowsPublicProperty = namespace != null && publicProperties.stream()
                    .anyMatch(property -> property.getLastElement(ConfigurationPropertyName.Form.UNIFORM)
                            .equals(namespace.getLastElement(ConfigurationPropertyName.Form.UNIFORM)));
            if (namespace == null
                    || namespace.getNumberOfElements() != 1
                    || !namespaceText.matches("[a-z][a-z0-9-]*")
                    || shadowsPublicProperty
                    || !namespaces.add(namespace)) {
                throw new IllegalStateException("Duplicate or invalid adapter configuration namespace");
            }
        }
    }

    private static Set<String> runtimeBeanNames(List<TransportAdapterDescriptor> adapters) {
        Set<String> names = new HashSet<>();
        for (TransportAdapterDescriptor adapter : adapters) {
            for (String beanName : adapter.runtimeBeanNames()) {
                if (beanName == null || beanName.isBlank() || !names.add(beanName)) {
                    throw new IllegalStateException("Duplicate or invalid adapter runtime bean registration");
                }
            }
        }
        return names;
    }

    private static void validateSelection(
            String selected,
            String requested,
            Set<String> installedTransports,
            boolean customSender,
            boolean customTransport) {
        if (selected.equals("custom")) {
            if (requested != null && !customSender && !customTransport) {
                throw new IllegalStateException(
                        "reliable-event.transport=custom requires one custom EventTransport or legacy EventSender");
            }
            return;
        }
        if (!installedTransports.contains(selected)) {
            throw new IllegalStateException(
                    "Selected transport '" + selected + "' is not installed; add its starter");
        }
        if (customSender || customTransport) {
            throw new IllegalStateException(
                    "A custom EventSender/EventTransport conflicts with selected transport '" + selected + "'");
        }
    }

    private static void logUnusedAdapterSettings(
            Environment environment,
            List<TransportAdapterDescriptor> adapters,
            String selected) {
        for (TransportAdapterDescriptor adapter : adapters) {
            String transport = adapter.transportName();
            if (!transport.equals(selected)
                    && Binder.get(environment)
                    .bind("reliable-event." + adapter.configurationNamespace(), Bindable.mapOf(String.class, Object.class))
                    .map(values -> !values.isEmpty())
                    .orElse(false)) {
                LOG.info("ReliableEvent selected transport=" + selected
                        + "; settings for transport '" + transport + "' are present but not used");
            }
        }
    }

    private static void validateRootProperties(
            Environment environment,
            Set<ConfigurationPropertyName> publicProperties,
            Set<ConfigurationPropertyName> namespaces) {
        Map<String, Object> root = Binder.get(environment)
                .bind("reliable-event", Bindable.mapOf(String.class, Object.class))
                .orElse(Map.of());
        Set<String> knownElements = publicProperties.stream()
                .map(property -> property.getLastElement(ConfigurationPropertyName.Form.UNIFORM))
                .collect(Collectors.toSet());
        Set<String> adapterElements = namespaces.stream()
                .map(namespace -> namespace.getLastElement(ConfigurationPropertyName.Form.UNIFORM))
                .collect(Collectors.toSet());

        for (Map.Entry<String, Object> entry : root.entrySet()) {
            ConfigurationPropertyName key = ConfigurationPropertyName.adapt(entry.getKey(), '.');
            if (key.getNumberOfElements() != 1) {
                throw unknownProperty(entry.getKey());
            }
            String element = key.getLastElement(ConfigurationPropertyName.Form.UNIFORM);
            if (adapterElements.contains(element)) {
                continue;
            }
            if (!knownElements.contains(element)) {
                throw unknownProperty(entry.getKey());
            }
            if (entry.getValue() instanceof Map<?, ?>) {
                throw new IllegalStateException(
                        "Property reliable-event." + entry.getKey() + " does not accept nested fields");
            }
        }
    }

    private static IllegalStateException unknownProperty(String property) {
        return new IllegalStateException("Unknown reliable-event property '" + property + "'");
    }

    private static Set<ConfigurationPropertyName> publicPropertyNames() {
        Set<ConfigurationPropertyName> names = new HashSet<>();
        try {
            for (PropertyDescriptor property : Introspector
                    .getBeanInfo(ReliableEventProperties.class, Object.class)
                    .getPropertyDescriptors()) {
                names.add(ConfigurationPropertyName.of(toKebabCase(property.getName())));
            }
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Cannot inspect ReliableEventProperties for strict configuration validation", exception);
        }
        return names;
    }

    private static String toKebabCase(String value) {
        return value.replaceAll("([a-z0-9])([A-Z])", "$1-$2").toLowerCase(Locale.ROOT);
    }

    private static boolean isFrameworkBean(
            ListableBeanFactory factory,
            String name,
            Set<String> builtInRuntimeNames) {
        if (builtInRuntimeNames.contains(name)) {
            return true;
        }
        if (factory instanceof org.springframework.beans.factory.config.ConfigurableListableBeanFactory configurable
                && configurable.containsBeanDefinition(name)) {
            String factoryBean = configurable.getBeanDefinition(name).getFactoryBeanName();
            if (factoryBean == null || !configurable.containsBeanDefinition(factoryBean)) {
                return false;
            }
            String owner = configurable.getBeanDefinition(factoryBean).getBeanClassName();
            return "dev.reliableevent.autoconfigure.ReliableEventTransportBridgeAutoConfiguration".equals(owner);
        }
        return false;
    }
}
