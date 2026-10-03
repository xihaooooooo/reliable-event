package dev.reliableevent.rocketmq;

import dev.reliableevent.EventId;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportReceipt;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RocketMqEventTransportTest {

    private Producer producer;
    private RocketMqEventTransport transport;

    @BeforeEach
    void setUp() {
        producer = mock(Producer.class);
        transport = new RocketMqEventTransport(
                ClientServiceProvider.loadService(),
                producer,
                new MapEventDestinationResolver(Map.of(
                        "coupon-task-execute", new RocketMqDestination("coupon-task-topic", "execute"))));
    }

    @Test
    void sendsPublicOutboundEventAndReturnsBrokerReceiptWithoutLegacyConversion() throws Exception {
        org.apache.rocketmq.client.apis.producer.SendReceipt brokerReceipt =
                mock(org.apache.rocketmq.client.apis.producer.SendReceipt.class);
        MessageId messageId = mock(MessageId.class);
        when(messageId.toString()).thenReturn("broker-message-1");
        when(brokerReceipt.getMessageId()).thenReturn(messageId);
        when(producer.send(any(Message.class))).thenReturn(brokerReceipt);

        TransportReceipt receipt = transport.send(event());

        assertThat(receipt.brokerMessageId()).contains("broker-message-1");
        assertThat(receipt.metadata()).isEmpty();
        verify(producer).send(any(Message.class));
    }

    @Test
    void acceptsConfirmedReceiptWithoutBrokerMessageId() throws Exception {
        org.apache.rocketmq.client.apis.producer.SendReceipt brokerReceipt =
                mock(org.apache.rocketmq.client.apis.producer.SendReceipt.class);
        when(brokerReceipt.getMessageId()).thenReturn(null);
        when(producer.send(any(Message.class))).thenReturn(brokerReceipt);

        assertThat(transport.send(event()).brokerMessageId()).isEmpty();
    }

    @Test
    void treatsMissingMappingAsNonRetryableBeforeSending() throws org.apache.rocketmq.client.apis.ClientException {
        assertThatThrownBy(() -> new RocketMqEventTransport(
                ClientServiceProvider.loadService(),
                producer,
                new MapEventDestinationResolver(Map.of())).send(event()))
                .isInstanceOfSatisfying(TransportException.class, exception ->
                        assertThat(exception.failureType()).isEqualTo(TransportFailureType.NON_RETRYABLE));
        verify(producer, never()).send(any(Message.class));
    }

    @Test
    void treatsNullReceiptAndPostSendRuntimeFailureAsResultUnknown() throws Exception {
        when(producer.send(any(Message.class))).thenReturn(null);
        assertUnknown(() -> transport.send(event()));

        when(producer.send(any(Message.class))).thenThrow(new IllegalStateException("private detail"));
        assertThatThrownBy(() -> transport.send(event()))
                .isInstanceOfSatisfying(TransportException.class, exception -> {
                    assertThat(exception.failureType()).isEqualTo(TransportFailureType.RESULT_UNKNOWN);
                    assertThat(exception).hasCauseInstanceOf(IllegalStateException.class)
                            .hasMessageNotContaining("private detail");
                });
    }

    private void assertUnknown(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(TransportException.class, exception ->
                        assertThat(exception.failureType()).isEqualTo(TransportFailureType.RESULT_UNKNOWN));
    }

    private OutboundEvent event() {
        return new OutboundEvent(new EventId(1), "coupon-task-execute", "task-1", "{\"id\":1}",
                Map.of("traceparent", "00-aabbcc"));
    }
}
