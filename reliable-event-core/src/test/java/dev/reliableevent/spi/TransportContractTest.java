package dev.reliableevent.spi;

import dev.reliableevent.EventId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransportContractTest {
    @Test
    void outboundEventCopiesHeadersAndPreservesJsonText() {
        Map<String, String> headers = new HashMap<>();
        headers.put("traceparent", "00-abc");
        OutboundEvent event = new OutboundEvent(new EventId(7), "order", "key", "{ \"x\" : 1 }", headers);
        headers.put("later", "value");
        assertThat(event.headers()).containsOnlyKeys("traceparent");
        assertThat(event.payloadJson()).isEqualTo("{ \"x\" : 1 }");
        assertThatThrownBy(() -> event.headers().put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void receiptAllowsMissingIdAndDefensivelyCopiesAllowlistedMetadata() {
        Map<String, String> metadata = new HashMap<>(Map.of("topic", "orders", "partition", "2"));
        TransportReceipt receipt = new TransportReceipt(Optional.empty(), metadata);
        metadata.clear();
        assertThat(receipt.brokerMessageId()).isEmpty();
        assertThat(receipt.metadata()).containsEntry("topic", "orders");
        assertThatThrownBy(() -> new TransportReceipt(Optional.empty(), Map.of("password", "secret")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> receipt.metadata().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
