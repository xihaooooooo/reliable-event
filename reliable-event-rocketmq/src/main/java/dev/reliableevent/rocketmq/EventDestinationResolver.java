package dev.reliableevent.rocketmq;

public interface EventDestinationResolver {

    RocketMqDestination resolve(String eventType);
}
