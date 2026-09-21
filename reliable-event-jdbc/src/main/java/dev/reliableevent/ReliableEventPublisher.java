package dev.reliableevent;

public interface ReliableEventPublisher {

    EventId publish(ReliableEvent<?> event);
}
