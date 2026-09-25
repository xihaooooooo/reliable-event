package dev.reliableevent.example;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "example.consumer", name = "enabled", havingValue = "true", matchIfMissing = true)
class ExampleConsumerConfiguration {

    @Bean(destroyMethod = "close")
    SimpleConsumer exampleConsumer(
            ClientServiceProvider provider,
            ClientConfiguration configuration,
            @Value("${example.rocketmq.topic}") String topic,
            @Value("${example.rocketmq.consumer-group}") String group
    ) throws ClientException {
        return provider.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(group)
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(Map.of(topic, FilterExpression.SUB_ALL))
                .build();
    }

    @Bean
    ExampleConsumerLoop exampleConsumerLoop(SimpleConsumer consumer, OrderMessageHandler handler) {
        return new ExampleConsumerLoop(consumer, handler);
    }
}
