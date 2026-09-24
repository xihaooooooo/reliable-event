package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.internal.publication.SendReceipt;
import org.apache.rocketmq.client.apis.ClientException;
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

class RocketMqEventSenderTest {

    private Producer producer;
    private RocketMqEventSender sender;

    @BeforeEach
    void setUp() {
        producer = mock(Producer.class);
        sender = new RocketMqEventSender(
                ClientServiceProvider.loadService(),
                producer,
                new MapEventDestinationResolver(Map.of(
                        "coupon-task-execute",
                        new RocketMqDestination("coupon-task-topic", "execute")
                )),
                new ObjectMapper(),
                1024
        );
    }

    @Test
    void returnsTheBrokerMessageIdAfterSynchronousSend() throws Exception {
        org.apache.rocketmq.client.apis.producer.SendReceipt brokerReceipt =
                mock(org.apache.rocketmq.client.apis.producer.SendReceipt.class);
        MessageId messageId = mock(MessageId.class);
        when(messageId.toString()).thenReturn("broker-message-1");
        when(brokerReceipt.getMessageId()).thenReturn(messageId);
        when(producer.send(any(Message.class))).thenReturn(brokerReceipt);

        SendReceipt result = sender.send(event());

        assertThat(result.messageId()).isEqualTo("broker-message-1");
        verify(producer).send(any(Message.class));
    }

    @Test
    void treatsMissingReceiptsAsResultUnknown() throws Exception {
        when(producer.send(any(Message.class))).thenReturn(null);

        assertThatThrownBy(() -> sender.send(event()))
                .isInstanceOfSatisfying(EventSendException.class, exception ->
                        assertThat(exception.failureType())
                                .isEqualTo(EventSendFailureType.RESULT_UNKNOWN)
                );
    }

    @Test
    void wrapsUnknownRuntimeFailuresAfterSendAsResultUnknown() throws Exception {
        when(producer.send(any(Message.class))).thenThrow(new IllegalStateException("secret"));

        assertThatThrownBy(() -> sender.send(event()))
                .isInstanceOfSatisfying(EventSendException.class, exception -> {
                    assertThat(exception.failureType())
                            .isEqualTo(EventSendFailureType.RESULT_UNKNOWN);
                    assertThat(exception).hasMessageNotContaining("secret")
                            .hasCauseInstanceOf(IllegalStateException.class);
                });
    }

    @Test
    void doesNotCallProducerWhenTheDestinationIsMissing() throws ClientException {
        RocketMqEventSender missingMappingSender = new RocketMqEventSender(
                ClientServiceProvider.loadService(),
                producer,
                new MapEventDestinationResolver(Map.of()),
                new ObjectMapper()
        );

        assertThatThrownBy(() -> missingMappingSender.send(event()))
                .isInstanceOfSatisfying(EventSendException.class, exception ->
                        assertThat(exception.failureType())
                                .isEqualTo(EventSendFailureType.NON_RETRYABLE)
                );
        verify(producer, never()).send(any(Message.class));
    }

    private StoredEvent event() {
        return new StoredEvent(
                new EventId(1),
                "coupon-task-execute",
                "task-1",
                "{\"taskId\":1}",
                "{}"
        );
    }
}
