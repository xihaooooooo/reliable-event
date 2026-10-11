package dev.reliableevent.benchmark;

import dev.reliableevent.EventId;
import dev.reliableevent.rocketmq.MapEventDestinationResolver;
import dev.reliableevent.rocketmq.RocketMqDestination;
import dev.reliableevent.rocketmq.RocketMqEventTransport;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BenchmarkApplicationTest {
    @Test
    void losesExactlyOneSuccessfulReceiptFromTheCurrentRocketMqTransport() throws Exception {
        Producer producer = mock(Producer.class);
        SendReceipt receipt = mock(SendReceipt.class);
        MessageId id = mock(MessageId.class);
        when(id.toString()).thenReturn("broker-message");
        when(receipt.getMessageId()).thenReturn(id);
        when(producer.send(any(Message.class))).thenReturn(null, receipt, receipt);
        var transport = new RocketMqEventTransport(ClientServiceProvider.loadService(), producer,
                new MapEventDestinationResolver(Map.of("test.event", new RocketMqDestination("events", "test"))));
        var postProcessor = BenchmarkApplication.dropFirstRealReceipt();
        var wrapped = (EventTransport) postProcessor.postProcessAfterInitialization(transport, "transport");
        var event = new OutboundEvent(new EventId(1), "test.event", "key", "{}", Map.of());

        assertThatThrownBy(() -> wrapped.send(event)).isInstanceOf(TransportException.class)
                .hasMessageContaining("without a receipt");
        assertThatThrownBy(() -> wrapped.send(event))
                .isInstanceOfSatisfying(TransportException.class, failure -> {
                    assertThat(failure.failureType()).isEqualTo(TransportFailureType.RESULT_UNKNOWN);
                    assertThat(failure).hasMessageContaining("Benchmark discarded");
                });
        assertThat(wrapped.send(event).brokerMessageId()).contains("broker-message");
        verify(producer, times(3)).send(any(Message.class));
        Object unrelated = new Object();
        assertThat(postProcessor.postProcessAfterInitialization(unrelated, "unrelated")).isSameAs(unrelated);
    }
}
