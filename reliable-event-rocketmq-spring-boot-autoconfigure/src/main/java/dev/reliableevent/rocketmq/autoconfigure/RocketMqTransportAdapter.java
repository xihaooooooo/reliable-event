package dev.reliableevent.rocketmq.autoconfigure;

import dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor;

import java.util.Set;

public final class RocketMqTransportAdapter implements TransportAdapterDescriptor {
    @Override
    public String transportName() {
        return "rocketmq";
    }

    @Override
    public String configurationNamespace() {
        return "rocketmq";
    }

    @Override
    public Set<String> runtimeBeanNames() {
        return Set.of("reliableEventRocketMqTransport");
    }
}
