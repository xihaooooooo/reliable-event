package dev.reliableevent.kafka.autoconfigure;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.DisposableBean;

import java.time.Duration;

/** Closes only a Producer created by this auto-configuration, with a finite timeout. */
final class KafkaProducerOwner implements DisposableBean {
    private final Producer<?, ?> producer;
    private final Duration timeout;

    KafkaProducerOwner(Producer<?, ?> producer, Duration timeout) {
        this.producer = producer;
        this.timeout = timeout;
    }

    @Override
    public void destroy() {
        producer.close(timeout);
    }
}
