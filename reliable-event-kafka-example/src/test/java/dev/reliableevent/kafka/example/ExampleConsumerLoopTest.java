package dev.reliableevent.kafka.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.assertj.core.api.Assertions.assertThat;

class ExampleConsumerLoopTest {
    @Test
    void failedRecordRewindsItAndEveryUnprocessedPartitionFromThatPoll() {
        Consumer<String, byte[]> consumer = mock(Consumer.class);
        List<ConsumerRecord<String, byte[]>> batch = List.of(
                record(0, 0), // committed before the failure
                record(1, 0), // failed
                record(0, 1), // same partition must not be skipped
                record(1, 1)); // later partition record must not be skipped

        ExampleConsumerLoop.rewindUnprocessed(consumer, batch, 1);

        verify(consumer).seek(new TopicPartition("orders", 0), 1);
        verify(consumer).seek(new TopicPartition("orders", 1), 0);
        verifyNoMoreInteractions(consumer);
    }

    @Test
    void handlerFailureRewindsWithoutOffsetCommitAndRecordsOnlyBusinessFailure() throws Exception {
        Consumer<String, byte[]> consumer = mock(Consumer.class);
        ConsumerRecord<String, byte[]> record = tracedRecord(0);
        stubbingOneRecordThenEmpty(consumer, record);
        OrderMessageHandler handler = mock(OrderMessageHandler.class);
        when(handler.handle(record)).thenThrow(new IllegalStateException("business transaction failed"));
        var tracing = new KafkaConsumerTracingTest.TracingFixture();
        ExampleConsumerLoop loop = new ExampleConsumerLoop(Map.of(), "orders", Duration.ofMillis(10), handler,
                tracing.tracer, tracing.propagator, properties -> consumer);
        loop.start();
        try {
            await(() -> loop.lastFailedOffset() == 0 && !tracing.ended.isEmpty());
            verify(consumer, never()).commitSync(anyMap(), any(Duration.class));
            verify(consumer).seek(new TopicPartition("orders", 0), 0);
            var span = tracing.ended.get(0);
            assertThat(span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                    "reliable_event.consume.result"))).isEqualTo("failed");
            assertThat(span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                    "reliable_event.offset_commit.result"))).isNull();
        } finally { loop.stop(); tracing.close(); }
    }

    @Test
    void offsetCommitFailureAfterIdempotentSkipRewindsAndRecordsCommitSeparately() throws Exception {
        Consumer<String, byte[]> consumer = mock(Consumer.class);
        ConsumerRecord<String, byte[]> record = tracedRecord(0);
        stubbingOneRecordThenEmpty(consumer, record);
        doThrow(new KafkaException("injected offset commit failure"))
                .when(consumer).commitSync(anyMap(), any(Duration.class));
        OrderMessageHandler handler = mock(OrderMessageHandler.class);
        when(handler.handle(record)).thenReturn(false);
        var tracing = new KafkaConsumerTracingTest.TracingFixture();
        ExampleConsumerLoop loop = new ExampleConsumerLoop(Map.of(), "orders", Duration.ofMillis(10), handler,
                tracing.tracer, tracing.propagator, properties -> consumer);
        loop.start();
        try {
            await(() -> loop.lastFailedOffset() == 0 && !tracing.ended.isEmpty());
            verify(consumer).commitSync(anyMap(), any(Duration.class));
            verify(consumer).seek(new TopicPartition("orders", 0), 0);
            var span = tracing.ended.get(0);
            assertThat(span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                    "reliable_event.consume.result"))).isEqualTo("idempotent_skip");
            assertThat(span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                    "reliable_event.offset_commit.result"))).isEqualTo("failed");
            assertThat(span.getEvents()).extracting(io.opentelemetry.sdk.trace.data.EventData::getName)
                    .contains("reliable_event.consumer.idempotent_skip", "reliable_event.consumer.offset_commit_failed");
        } finally { loop.stop(); tracing.close(); }
    }

    private static ConsumerRecord<String, byte[]> tracedRecord(long offset) {
        ConsumerRecord<String, byte[]> record = record(0, offset);
        RecordHeaders headers = (RecordHeaders) record.headers();
        headers.add("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        headers.add("reliable_event_id", "1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return record;
    }

    private static void stubbingOneRecordThenEmpty(Consumer<String, byte[]> consumer,
                                                    ConsumerRecord<String, byte[]> record) {
        AtomicInteger polls = new AtomicInteger();
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        ConsumerRecords<String, byte[]> batch = new ConsumerRecords<>(Map.of(partition, List.of(record)));
        when(consumer.poll(any(Duration.class))).thenAnswer(invocation ->
                polls.getAndIncrement() == 0 ? batch : ConsumerRecords.empty());
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static ConsumerRecord<String, byte[]> record(int partition, long offset) {
        return new ConsumerRecord<>("orders", partition, offset, "key", new byte[]{1});
    }
}
