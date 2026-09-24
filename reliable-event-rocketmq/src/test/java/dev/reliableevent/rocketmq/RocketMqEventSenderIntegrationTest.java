package dev.reliableevent.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.SendReceipt;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@Timeout(180)
class RocketMqEventSenderIntegrationTest {

    private static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();

    private static RocketMqTestEnvironment environment;
    private static Producer producer;
    private static SimpleConsumer consumer;
    private static RocketMqEventSender sender;

    @BeforeAll
    static void startRocketMq() throws Exception {
        environment = new RocketMqTestEnvironment();
        environment.start();
        ClientConfiguration configuration = environment.clientConfiguration(Duration.ofSeconds(5));
        producer = PROVIDER.newProducerBuilder()
                .setClientConfiguration(configuration)
                .setTopics(RocketMqTestEnvironment.TOPIC)
                .setMaxAttempts(1)
                .build();
        consumer = PROVIDER.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(RocketMqTestEnvironment.CONSUMER_GROUP)
                .setAwaitDuration(Duration.ofSeconds(5))
                .setSubscriptionExpressions(Map.of(
                        RocketMqTestEnvironment.TOPIC,
                        FilterExpression.SUB_ALL
                ))
                .build();
        sender = new RocketMqEventSender(
                PROVIDER,
                producer,
                new MapEventDestinationResolver(Map.of(
                        "coupon-task-execute",
                        new RocketMqDestination(
                                RocketMqTestEnvironment.TOPIC,
                                RocketMqTestEnvironment.TAG
                        )
                )),
                new ObjectMapper()
        );
    }

    @AfterAll
    static void stopRocketMq() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
        if (environment != null) {
            environment.close();
        }
    }

    @Test
    void sendsTheMappedMessageToARealBroker() throws Exception {
        String eventKey = "task-" + UUID.randomUUID();
        String payload = "{\"taskId\":\"" + eventKey + "\"}";
        StoredEvent event = new StoredEvent(
                new EventId(1001),
                "coupon-task-execute",
                eventKey,
                payload,
                "{\"traceparent\":\"00-aabbcc\"}"
        );

        SendReceipt receipt = sender.send(event);
        MessageView message = receiveByKey(eventKey);

        assertThat(receipt.messageId()).isNotBlank();
        assertThat(message.getMessageId().toString()).isEqualTo(receipt.messageId());
        assertThat(message.getTopic()).isEqualTo(RocketMqTestEnvironment.TOPIC);
        assertThat(message.getTag()).contains(RocketMqTestEnvironment.TAG);
        assertThat(message.getKeys()).contains(eventKey);
        assertThat(readBody(message.getBody())).isEqualTo(payload);
        assertThat(message.getProperties())
                .containsEntry("traceparent", "00-aabbcc")
                .containsEntry(RocketMqMessageFactory.EVENT_ID_PROPERTY, "1001")
                .containsEntry(RocketMqMessageFactory.EVENT_TYPE_PROPERTY, "coupon-task-execute")
                .containsEntry(RocketMqMessageFactory.EVENT_KEY_PROPERTY, eventKey);
        consumer.ack(message);
    }

    private MessageView receiveByKey(String eventKey) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            List<MessageView> messages = consumer.receive(16, Duration.ofSeconds(15));
            for (MessageView message : messages) {
                if (message.getKeys().contains(eventKey)) {
                    return message;
                }
                consumer.ack(message);
            }
        }
        throw new AssertionError("Did not receive RocketMQ message for key " + eventKey);
    }

    private String readBody(ByteBuffer body) {
        byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
