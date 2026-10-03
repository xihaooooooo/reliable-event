package dev.reliableevent.kafka.autoconfigure;

import dev.reliableevent.EventId;
import dev.reliableevent.kafka.KafkaEventTransport;
import dev.reliableevent.kafka.MapKafkaDestinationResolver;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaEventTransportBrokerTest {
    private static final String IMAGE = "apache/kafka:3.9.2";

    @Test
    void brokerConfirmationLossProducesUnknownAndSameIdentityCanBeObservedAsDuplicate() throws Exception {
        try (KafkaContainer broker = broker()) {
            broker.start();
            createTopic(broker, "orders", Map.of());

            KafkaProducer<String, byte[]> delegate = producer(broker);
            try {
                Producer<String, byte[]> dropConfirmation = mock(Producer.class);
                when(dropConfirmation.send(any())).thenAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    ProducerRecord<String, byte[]> record = invocation.getArgument(0);
                    delegate.send(record).get(10, TimeUnit.SECONDS);
                    return new CompletableFuture<org.apache.kafka.clients.producer.RecordMetadata>() {
                        @Override
                        public org.apache.kafka.clients.producer.RecordMetadata get(long timeout, TimeUnit unit)
                                throws ExecutionException, InterruptedException, TimeoutException {
                            throw new TimeoutException("Injected loss of broker confirmation");
                        }
                    };
                });
                KafkaEventTransport transport = new KafkaEventTransport(
                        dropConfirmation, new MapKafkaDestinationResolver(Map.of("order-created", "orders")),
                        Duration.ofSeconds(7), 4096);
                TransportException unknown = assertThrows(TransportException.class,
                        () -> transport.send(event(7001, "order-7", "order-created")));
                assertThat(unknown.failureType()).isEqualTo(TransportFailureType.RESULT_UNKNOWN);
            } finally {
                delegate.close(Duration.ofSeconds(2));
            }

            try (KafkaProducer<String, byte[]> retryProducer = producer(broker)) {
                KafkaEventTransport retryTransport = new KafkaEventTransport(retryProducer,
                        new MapKafkaDestinationResolver(Map.of("order-created", "orders")), Duration.ofSeconds(7), 4096);
                var receipt = retryTransport.send(event(7001, "order-7", "order-created"));
                assertThat(receipt.brokerMessageId()).isEmpty();
                assertThat(receipt.metadata()).containsEntry("topic", "orders").containsKeys("partition", "offset");
            }

            List<ObservedRecord> records = consume(broker, "orders", 2);
            assertThat(records).hasSize(2);
            assertThat(records).allSatisfy(record -> {
                assertThat(record.key()).isEqualTo("order-7");
                assertThat(record.eventId()).isEqualTo("7001");
                assertThat(record.eventType()).isEqualTo("order-created");
                assertThat(record.eventKey()).isEqualTo("order-7");
            });
        }
    }

    @Test
    void brokerRejectsOversizedRecordAsPermanentFailure() throws Exception {
        try (KafkaContainer broker = broker()) {
            broker.start();
            createTopic(broker, "small-records", Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1024"));
            try (KafkaProducer<String, byte[]> producer = producer(broker)) {
                KafkaEventTransport transport = new KafkaEventTransport(producer,
                        new MapKafkaDestinationResolver(Map.of("order-created", "small-records")), Duration.ofSeconds(7), 4096);
                String payload = "x".repeat(4096);
                TransportException rejection = assertThrows(TransportException.class,
                        () -> transport.send(new OutboundEvent(new EventId(7002), "order-created", "order-8",
                                payload, Map.of())));
                assertThat(rejection.failureType()).isEqualTo(TransportFailureType.NON_RETRYABLE);
                assertThat(rejection.getMessage()).doesNotContain(payload);
            }
        }
    }

    private static KafkaContainer broker() {
        return new KafkaContainer(DockerImageName.parse(IMAGE));
    }

    private static void createTopic(KafkaContainer broker, String name, Map<String, String> config) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", broker.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(new NewTopic(name, 1, (short) 1).configs(config))).all().get(30, TimeUnit.SECONDS);
        }
    }

    private static KafkaProducer<String, byte[]> producer(KafkaContainer broker) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        return new KafkaProducer<>(properties);
    }

    private static OutboundEvent event(long id, String key, String type) {
        return new OutboundEvent(new EventId(id), type, key, "{\"order\":\"" + key + "\"}", Map.of());
    }

    private static List<ObservedRecord> consume(KafkaContainer broker, String topic, int count) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "reliable-event-test-" + System.nanoTime());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(new TopicPartition(topic, 0)));
            var result = new java.util.ArrayList<ObservedRecord>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (result.size() < count && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(record -> result.add(new ObservedRecord(
                        record.key(), header(record, KafkaEventTransport.EVENT_ID_HEADER),
                        header(record, KafkaEventTransport.EVENT_TYPE_HEADER),
                        header(record, KafkaEventTransport.EVENT_KEY_HEADER))));
            }
            return result;
        }
    }

    private static String header(org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private record ObservedRecord(String key, String eventId, String eventType, String eventKey) { }

}
