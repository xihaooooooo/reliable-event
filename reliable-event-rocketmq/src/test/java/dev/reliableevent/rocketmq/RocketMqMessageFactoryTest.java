package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSendFailureType;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RocketMqMessageFactoryTest {

    private static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();
    private static final RocketMqDestination DESTINATION =
            new RocketMqDestination("coupon-task-topic", "execute");

    @Test
    void mapsStoredEventsToOrdinaryRocketMqMessages() {
        String payload = "{\"name\":\"测试券\"}";
        StoredEvent event = new StoredEvent(
                new EventId(41),
                "coupon-task-execute",
                "task-41",
                payload,
                "{\"traceparent\":\"00-aabbcc\",\"tenant.id\":\"tenant-1\"}"
        );
        RocketMqMessageFactory factory = new RocketMqMessageFactory(
                PROVIDER,
                new ObjectMapper(),
                RocketMqMessageFactory.DEFAULT_MAX_BODY_BYTES
        );

        Message message = factory.create(event, DESTINATION);

        assertThat(message.getTopic()).isEqualTo("coupon-task-topic");
        assertThat(message.getTag()).contains("execute");
        assertThat(message.getKeys()).containsExactly("task-41");
        assertThat(readBody(message.getBody())).isEqualTo(payload);
        assertThat(message.getProperties())
                .containsEntry("traceparent", "00-aabbcc")
                .containsEntry("tenant.id", "tenant-1")
                .containsEntry(RocketMqMessageFactory.EVENT_ID_PROPERTY, "41")
                .containsEntry(RocketMqMessageFactory.EVENT_TYPE_PROPERTY, "coupon-task-execute")
                .containsEntry(RocketMqMessageFactory.EVENT_KEY_PROPERTY, "task-41");
        assertThat(message.getMessageGroup()).isEmpty();
        assertThat(message.getDeliveryTimestamp()).isEmpty();
    }

    @Test
    void leavesTheTagUnsetWhenTheDestinationHasNoTag() {
        RocketMqMessageFactory factory = new RocketMqMessageFactory(
                PROVIDER,
                new ObjectMapper(),
                100
        );

        Message message = factory.create(
                event("{}", "{}"),
                new RocketMqDestination("coupon-task-topic", null)
        );

        assertThat(message.getTag()).isEmpty();
    }

    @Test
    void measuresTheBodyAsUtf8BytesBeforeCallingTheClientBuilder() {
        String payload = "券";
        RocketMqMessageFactory factory = new RocketMqMessageFactory(
                PROVIDER,
                new ObjectMapper(),
                2
        );

        assertThatThrownBy(() -> factory.create(event(payload, "{}"), DESTINATION))
                .isInstanceOfSatisfying(EventSendException.class, exception -> {
                    assertThat(exception.failureType())
                            .isEqualTo(EventSendFailureType.NON_RETRYABLE);
                    assertThat(exception).hasMessageContaining("3 bytes");
                });
    }

    @Test
    void rejectsReservedInvalidAndNonStringHeadersWithoutLeakingValues() {
        RocketMqMessageFactory factory = new RocketMqMessageFactory(
                PROVIDER,
                new ObjectMapper(),
                100
        );

        assertThatThrownBy(() -> factory.create(
                event("{}", "{\"reliable_event_secret\":\"do-not-print\"}"),
                DESTINATION
        )).isInstanceOfSatisfying(EventSendException.class, exception -> {
            assertThat(exception.failureType()).isEqualTo(EventSendFailureType.NON_RETRYABLE);
            assertThat(exception).hasMessageContaining("reliable_event_secret")
                    .hasMessageNotContaining("do-not-print");
        });
        assertThatThrownBy(() -> factory.create(
                event("{}", "{\"attempt\":1}"),
                DESTINATION
        )).isInstanceOfSatisfying(EventSendException.class, exception ->
                assertThat(exception).hasMessageContaining("string value")
        );
        assertThatThrownBy(() -> factory.create(
                event("{}", "[]"),
                DESTINATION
        )).isInstanceOfSatisfying(EventSendException.class, exception ->
                assertThat(exception).hasMessageContaining("JSON object")
        );
        assertThatThrownBy(() -> factory.create(
                event("{}", "{\"bad\\nname\":\"do-not-print\"}"),
                DESTINATION
        )).isInstanceOfSatisfying(EventSendException.class, exception ->
                assertThat(exception).hasMessageContaining("bad?name")
                        .hasMessageNotContaining("\n")
                        .hasMessageNotContaining("do-not-print")
        );
    }

    private StoredEvent event(String payload, String headers) {
        return new StoredEvent(
                new EventId(42),
                "coupon-task-execute",
                "task-42",
                payload,
                headers
        );
    }

    private String readBody(ByteBuffer body) {
        byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
