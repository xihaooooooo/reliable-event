package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSendException;
import dev.reliableevent.internal.publication.EventSendFailureType;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportReceipt;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventTransportSenderBridgeTest {
    private final StoredEvent event = new StoredEvent(new EventId(41), "test.event", "key",
            "{ \"n\" : 1 }", "{\"traceparent\":\"00-abc\"}");

    @Test
    void forwardsImmutableViewAndAcceptsConfirmedReceiptWithoutMessageId() {
        EventTransport transport = mock(EventTransport.class);
        when(transport.send(org.mockito.ArgumentMatchers.any())).thenReturn(
                new TransportReceipt(Optional.empty(), Map.of("topic", "events")));
        var sender = new EventTransportSenderBridge(transport, new ObjectMapper());
        var receipt = sender.send(event);
        assertThat(receipt.messageId()).isNull();
        assertThat(receipt.metadata()).containsEntry("topic", "events");
        org.mockito.ArgumentCaptor<dev.reliableevent.spi.OutboundEvent> sent =
                org.mockito.ArgumentCaptor.forClass(dev.reliableevent.spi.OutboundEvent.class);
        org.mockito.Mockito.verify(transport).send(sent.capture());
        assertThat(sent.getValue().payloadJson()).isEqualTo(event.payloadJson());
        assertThat(sent.getValue().headers()).containsEntry("traceparent", "00-abc");
    }

    @Test
    void mapsClassifiedAndUnexpectedExceptionsAndRejectsNullReceipt() {
        for (TransportFailureType publicType : TransportFailureType.values()) {
            EventTransport transport = ignored -> { throw new TransportException(publicType, "classified"); };
            assertThatThrownBy(() -> new EventTransportSenderBridge(transport, new ObjectMapper()).send(event))
                    .isInstanceOf(EventSendException.class)
                    .extracting(failure -> ((EventSendException) failure).failureType())
                    .isEqualTo(EventSendFailureType.valueOf(publicType.name()));
        }
        EventTransport unexpected = ignored -> { throw new IllegalStateException("unexpected"); };
        assertFailure(new EventTransportSenderBridge(unexpected, new ObjectMapper()), EventSendFailureType.RESULT_UNKNOWN);
        EventTransport nullReceipt = ignored -> null;
        assertFailure(new EventTransportSenderBridge(nullReceipt, new ObjectMapper()), EventSendFailureType.RESULT_UNKNOWN);
    }

    @Test
    void invalidPersistedHeadersFailBeforeTransportAndAreNotSwallowed() {
        EventTransport transport = mock(EventTransport.class);
        var sender = new EventTransportSenderBridge(transport, new ObjectMapper());
        for (String headers : new String[]{"not-json", "[]", "{\"x\":1}",
                "{\"reliable_event_id\":\"spoof\"}",
                "{\"oversized\":\"" + "x".repeat(4097) + "\"}"}) {
            StoredEvent bad = new StoredEvent(event.id(), event.eventType(), event.eventKey(), event.payloadJson(),
                    headers);
            assertFailure(sender, bad, EventSendFailureType.NON_RETRYABLE);
        }
        org.mockito.Mockito.verifyNoInteractions(transport);
    }

    private static void assertFailure(EventTransportSenderBridge sender, EventSendFailureType expected) {
        assertFailure(sender, new StoredEvent(new EventId(2), "type", "key", "{}", "{}"), expected);
    }

    private static void assertFailure(EventTransportSenderBridge sender, StoredEvent event,
                                      EventSendFailureType expected) {
        assertThatThrownBy(() -> sender.send(event)).isInstanceOf(EventSendException.class)
                .extracting(failure -> ((EventSendException) failure).failureType()).isEqualTo(expected);
    }
}
