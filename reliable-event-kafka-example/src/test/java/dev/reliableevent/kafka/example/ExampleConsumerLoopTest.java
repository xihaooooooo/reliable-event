package dev.reliableevent.kafka.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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

    private static ConsumerRecord<String, byte[]> record(int partition, long offset) {
        return new ConsumerRecord<>("orders", partition, offset, "key", new byte[]{1});
    }
}
