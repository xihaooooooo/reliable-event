package dev.reliableevent.kafka.autoconfigure;

import dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor;

import java.util.Set;

public final class KafkaTransportAdapter implements TransportAdapterDescriptor {
    @Override public String transportName() { return "kafka"; }
    @Override public String configurationNamespace() { return "kafka"; }
    @Override public Set<String> runtimeBeanNames() { return Set.of("reliableEventKafkaTransport"); }
}
