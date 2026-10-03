package dev.reliableevent.rocketmq;

import dev.reliableevent.EventId;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RocketMqMessageFactoryTest {

    private static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();
    private static final RocketMqDestination DESTINATION =
            new RocketMqDestination("coupon-task-topic", "execute");

    @Test
    void mapsPublicOutboundEventsToOrdinaryRocketMqMessages() {
        String payload = "{\"name\":\"测试券\"}";
        OutboundEvent event = event(payload, Map.of("traceparent", "00-aabbcc", "tenant.id", "tenant-1"));
        RocketMqMessageFactory factory = new RocketMqMessageFactory(
                PROVIDER, RocketMqMessageFactory.DEFAULT_MAX_BODY_BYTES);

        Message message = factory.create(event, DESTINATION);

        assertThat(message.getTopic()).isEqualTo("coupon-task-topic");
        assertThat(message.getTag()).contains("execute");
        assertThat(message.getKeys()).containsExactly("task-42");
        assertThat(readBody(message.getBody())).isEqualTo(payload);
        assertThat(message.getProperties())
                .containsEntry("traceparent", "00-aabbcc")
                .containsEntry("tenant.id", "tenant-1")
                .containsEntry(RocketMqMessageFactory.EVENT_ID_PROPERTY, "42")
                .containsEntry(RocketMqMessageFactory.EVENT_TYPE_PROPERTY, "coupon-task-execute")
                .containsEntry(RocketMqMessageFactory.EVENT_KEY_PROPERTY, "task-42");
        assertThat(message.getMessageGroup()).isEmpty();
        assertThat(message.getDeliveryTimestamp()).isEmpty();
    }

    @Test
    void leavesTheTagUnsetWhenTheDestinationHasNoTag() {
        RocketMqMessageFactory factory = new RocketMqMessageFactory(PROVIDER, 100);

        Message message = factory.create(event("{}", Map.of()),
                new RocketMqDestination("coupon-task-topic", null));

        assertThat(message.getTag()).isEmpty();
    }

    @Test
    void measuresTheBodyAsUtf8BytesBeforeCallingTheClientBuilder() {
        RocketMqMessageFactory factory = new RocketMqMessageFactory(PROVIDER, 2);

        assertThatThrownBy(() -> factory.create(event("券", Map.of()), DESTINATION))
                .isInstanceOfSatisfying(TransportException.class, exception -> {
                    assertThat(exception.failureType()).isEqualTo(TransportFailureType.NON_RETRYABLE);
                    assertThat(exception).hasMessageContaining("3 bytes");
                });
    }

    @Test
    void rejectsReservedHeadersWithoutLeakingValues() {
        RocketMqMessageFactory factory = new RocketMqMessageFactory(PROVIDER, 100);

        assertThatThrownBy(() -> factory.create(
                event("{}", Map.of("reliable_event_secret", "do-not-print")), DESTINATION))
                .isInstanceOfSatisfying(TransportException.class, exception -> {
                    assertThat(exception.failureType()).isEqualTo(TransportFailureType.NON_RETRYABLE);
                    assertThat(exception).hasMessageContaining("reliable_event_secret")
                            .hasMessageNotContaining("do-not-print");
                });
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void rejectsHeadersThatDoNotSatisfyThePublicStringMapContract() {
        Map nonStringHeaders = Map.of("attempt", 1);
        Map<String, String> blankHeaders = Map.of("tenant", " ");
        RocketMqMessageFactory factory = new RocketMqMessageFactory(PROVIDER, 100);

        assertThatThrownBy(() -> factory.create(event("{}", nonStringHeaders), DESTINATION))
                .isInstanceOfSatisfying(TransportException.class, exception ->
                        assertThat(exception.failureType()).isEqualTo(TransportFailureType.NON_RETRYABLE));
        assertThatThrownBy(() -> factory.create(event("{}", blankHeaders), DESTINATION))
                .isInstanceOfSatisfying(TransportException.class, exception -> {
                    assertThat(exception.failureType()).isEqualTo(TransportFailureType.NON_RETRYABLE);
                    assertThat(exception).hasMessageContaining("tenant");
                });
    }

    private OutboundEvent event(String payload, Map<String, String> headers) {
        return new OutboundEvent(new EventId(42), "coupon-task-execute", "task-42", payload, headers);
    }

    private String readBody(ByteBuffer body) {
        byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
