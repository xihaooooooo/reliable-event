package dev.reliableevent.autoconfigure.spi;

import java.util.Set;

/** Spring auto-configuration metadata for one installed built-in transport. */
public interface TransportAdapterDescriptor {
    String transportName();

    String configurationNamespace();

    /** Bean names owned by this adapter runtime, used to distinguish framework beans from user SPI beans. */
    default Set<String> runtimeBeanNames() {
        return Set.of();
    }
}
