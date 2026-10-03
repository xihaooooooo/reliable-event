package dev.reliableevent.kafka;

import dev.reliableevent.EventId;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaEventTransportTest {
    private static final OutboundEvent EVENT = new OutboundEvent(
            new EventId(42), "order-created", "order-7", "{\"name\":\"雪\"}", Map.of("trace-id", "abc"));

    @Test
    void sendsExactUtf8PayloadKeyAndIdentityHeadersAndReturnsOnlyDiagnosticPosition() {
        Producer<String, byte[]> producer = mock(Producer.class);
        RecordMetadata metadata = metadata("orders", 3, 19);
        when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(metadata));
        KafkaEventTransport transport = transport(producer, event -> "orders", Duration.ofSeconds(1), 1024);

        var receipt = transport.send(EVENT);

        ArgumentCaptor<ProducerRecord<String, byte[]>> record = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(record.capture());
        assertEquals("order-7", record.getValue().key());
        assertArrayEquals(EVENT.payloadJson().getBytes(StandardCharsets.UTF_8), record.getValue().value());
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), record.getValue().headers().lastHeader("trace-id").value());
        assertEquals("42", new String(record.getValue().headers().lastHeader("reliable_event_id").value(), StandardCharsets.UTF_8));
        assertEquals("order-created", new String(record.getValue().headers().lastHeader("reliable_event_type").value(), StandardCharsets.UTF_8));
        assertEquals("order-7", new String(record.getValue().headers().lastHeader("reliable_event_key").value(), StandardCharsets.UTF_8));
        assertFalse(receipt.brokerMessageId().isPresent());
        assertEquals(Map.of("topic", "orders", "partition", "3", "offset", "19"), receipt.metadata());
    }

    @Test
    void missingMappingAndInvalidHeadersFailBeforeProducerInvocation() {
        Producer<String, byte[]> producer = mock(Producer.class);
        KafkaEventTransport transport = transport(producer, new MapKafkaDestinationResolver(Map.of()),
                Duration.ofSeconds(1), 1024);
        TransportException missing = assertThrows(TransportException.class, () -> transport.send(EVENT));
        assertEquals(TransportFailureType.NON_RETRYABLE, missing.failureType());

        KafkaEventTransport invalidHeaderTransport = transport(producer, event -> "orders", Duration.ofSeconds(1), 1024);
        OutboundEvent invalid = new OutboundEvent(EVENT.id(), EVENT.eventType(), EVENT.eventKey(), EVENT.payloadJson(),
                Map.of("reliable_event_id", "spoof"));
        TransportException invalidFailure = assertThrows(TransportException.class,
                () -> invalidHeaderTransport.send(invalid));
        assertEquals(TransportFailureType.NON_RETRYABLE, invalidFailure.failureType());
        verify(producer, never()).send(any());
    }

    @Test
    void oversizedBodyAndInvalidResolverTopicFailBeforeProducerInvocation() {
        Producer<String, byte[]> producer = mock(Producer.class);
        TransportException tooLarge = assertThrows(TransportException.class,
                () -> transport(producer, event -> "orders", Duration.ofSeconds(1), 1).send(EVENT));
        assertEquals(TransportFailureType.NON_RETRYABLE, tooLarge.failureType());

        TransportException invalidTopic = assertThrows(TransportException.class,
                () -> transport(producer, event -> "bad topic", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.NON_RETRYABLE, invalidTopic.failureType());
        verify(producer, never()).send(any());
    }

    @Test
    void classifiesSynchronousLocalPermanentAndKnownTransientErrorsSeparately() {
        Producer<String, byte[]> permanent = mock(Producer.class);
        when(permanent.send(any())).thenThrow(new SerializationException("sensitive SDK detail"));
        TransportException local = assertThrows(TransportException.class,
                () -> transport(permanent, event -> "orders", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.NON_RETRYABLE, local.failureType());
        assertFalse(local.getMessage().contains("sensitive"));
        assertEquals(null, local.getCause());

        Producer<String, byte[]> transientProducer = mock(Producer.class);
        when(transientProducer.send(any())).thenThrow(new NetworkException("temporary"));
        TransportException transientFailure = assertThrows(TransportException.class,
                () -> transport(transientProducer, event -> "orders", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.RETRYABLE, transientFailure.failureType());
        assertEquals(null, transientFailure.getCause());

        Producer<String, byte[]> saslDenied = mock(Producer.class);
        when(saslDenied.send(any())).thenThrow(new SaslAuthenticationException("private SASL principal detail",
                new IllegalStateException("private nested cause")));
        TransportException saslFailure = assertThrows(TransportException.class,
                () -> transport(saslDenied, event -> "orders", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.NON_RETRYABLE, saslFailure.failureType());
        assertFalse(saslFailure.getMessage().contains("private"));
        assertEquals(null, saslFailure.getCause());
    }

    @Test
    void completedAsyncPermanentAndUnknownResultsRemainDistinctWithoutSdkMessages() {
        Producer<String, byte[]> permanent = mock(Producer.class);
        when(permanent.send(any())).thenReturn(CompletableFuture.failedFuture(
                new AuthorizationException("private principal detail")));
        TransportException denied = assertThrows(TransportException.class,
                () -> transport(permanent, event -> "orders", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.NON_RETRYABLE, denied.failureType());
        assertEquals(null, denied.getCause());

        Producer<String, byte[]> sslDenied = mock(Producer.class);
        when(sslDenied.send(any())).thenReturn(CompletableFuture.failedFuture(
                new SslAuthenticationException("private SSL credential detail",
                        new IllegalStateException("private nested cause"))));
        TransportException sslFailure = assertThrows(TransportException.class,
                () -> transport(sslDenied, event -> "orders", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.NON_RETRYABLE, sslFailure.failureType());
        assertFalse(sslFailure.getMessage().contains("private"));
        assertEquals(null, sslFailure.getCause());

        Producer<String, byte[]> unknown = mock(Producer.class);
        when(unknown.send(any())).thenReturn(CompletableFuture.failedFuture(new KafkaException("private detail")));
        TransportException uncertain = assertThrows(TransportException.class,
                () -> transport(unknown, event -> "orders", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.RESULT_UNKNOWN, uncertain.failureType());
        assertEquals(null, uncertain.getCause());

        Producer<String, byte[]> network = mock(Producer.class);
        when(network.send(any())).thenReturn(CompletableFuture.failedFuture(new NetworkException("late network failure")));
        TransportException networkUnknown = assertThrows(TransportException.class,
                () -> transport(network, event -> "orders", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.RESULT_UNKNOWN, networkUnknown.failureType());
        assertEquals(null, networkUnknown.getCause());
    }

    @Test
    void confirmationTimeoutAndInvalidMetadataAreUnknown() {
        Producer<String, byte[]> pending = mock(Producer.class);
        when(pending.send(any())).thenReturn(new CompletableFuture<>());
        TransportException timedOut = assertThrows(TransportException.class,
                () -> transport(pending, event -> "orders", Duration.ofMillis(5), 1024).send(EVENT));
        assertEquals(TransportFailureType.RESULT_UNKNOWN, timedOut.failureType());

        Producer<String, byte[]> wrongTopic = mock(Producer.class);
        RecordMetadata otherTopicMetadata = metadata("other", 0, 1);
        when(wrongTopic.send(any())).thenReturn(CompletableFuture.completedFuture(otherTopicMetadata));
        TransportException invalidMetadata = assertThrows(TransportException.class,
                () -> transport(wrongTopic, event -> "orders", Duration.ofSeconds(1), 1024).send(EVENT));
        assertEquals(TransportFailureType.RESULT_UNKNOWN, invalidMetadata.failureType());
    }

    @Test
    void synchronousSendTimeAndLateFutureResultShareOneDeadline() throws Exception {
        Producer<String, byte[]> slowSend = mock(Producer.class);
        RecordMetadata confirmation = metadata("orders", 0, 4);
        when(slowSend.send(any())).thenAnswer(invocation -> {
            Thread.sleep(30);
            return CompletableFuture.completedFuture(confirmation);
        });
        TransportException sendBudgetExpired = assertThrows(TransportException.class,
                () -> transport(slowSend, event -> "orders", Duration.ofMillis(10), 1024).send(EVENT));
        assertEquals(TransportFailureType.RESULT_UNKNOWN, sendBudgetExpired.failureType());

        Producer<String, byte[]> lateFuture = mock(Producer.class);
        Future<RecordMetadata> ignoresTimeout = new CompletableFuture<>() {
            @Override
            public RecordMetadata get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                Thread.sleep(30);
                return confirmation;
            }
        };
        when(lateFuture.send(any())).thenReturn(ignoresTimeout);
        TransportException lateConfirmation = assertThrows(TransportException.class,
                () -> transport(lateFuture, event -> "orders", Duration.ofMillis(10), 1024).send(EVENT));
        assertEquals(TransportFailureType.RESULT_UNKNOWN, lateConfirmation.failureType());
    }

    private static KafkaEventTransport transport(Producer<String, byte[]> producer,
                                                  KafkaDestinationResolver resolver,
                                                  Duration budget,
                                                  int maxBodyBytes) {
        return new KafkaEventTransport(producer, resolver, budget, maxBodyBytes);
    }

    private static RecordMetadata metadata(String topic, int partition, long offset) {
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.topic()).thenReturn(topic);
        when(metadata.partition()).thenReturn(partition);
        when(metadata.offset()).thenReturn(offset);
        when(metadata.hasOffset()).thenReturn(true);
        return metadata;
    }
}
