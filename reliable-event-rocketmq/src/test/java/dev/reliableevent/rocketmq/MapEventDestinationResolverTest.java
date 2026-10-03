package dev.reliableevent.rocketmq;

import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MapEventDestinationResolverTest {

    @Test
    void resolvesExactEventTypesFromAnImmutableSnapshot() {
        RocketMqDestination destination = new RocketMqDestination("coupon-task-topic", "execute");
        Map<String, RocketMqDestination> mappings = new HashMap<>();
        mappings.put("coupon-task-execute", destination);
        MapEventDestinationResolver resolver = new MapEventDestinationResolver(mappings);

        mappings.clear();

        assertThat(resolver.resolve("coupon-task-execute")).isEqualTo(destination);
        assertThatThrownBy(() -> resolver.resolve("Coupon-Task-Execute"))
                .isInstanceOfSatisfying(TransportException.class, exception ->
                        assertThat(exception.failureType())
                                .isEqualTo(TransportFailureType.NON_RETRYABLE)
                );
    }

    @Test
    void rejectsMissingMappingsAndInvalidConfiguration() {
        MapEventDestinationResolver resolver = new MapEventDestinationResolver(Map.of());

        assertThatThrownBy(() -> resolver.resolve("missing"))
                .isInstanceOfSatisfying(TransportException.class, exception -> {
                    assertThat(exception.failureType())
                            .isEqualTo(TransportFailureType.NON_RETRYABLE);
                    assertThat(exception).hasMessageContaining("missing");
                });
        assertThatThrownBy(() -> new MapEventDestinationResolver(Map.of(
                " ", new RocketMqDestination("topic", null)
        ))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("event type");
    }
}
