package dev.reliableevent.example;

import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Example-only consumer; all business writes happen before acknowledging the message. */
final class ExampleConsumerLoop implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleConsumerLoop.class);

    private final SimpleConsumer consumer;
    private final OrderMessageHandler handler;
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread thread;

    ExampleConsumerLoop(SimpleConsumer consumer, OrderMessageHandler handler) {
        this.consumer = Objects.requireNonNull(consumer);
        this.handler = Objects.requireNonNull(handler);
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        thread = new Thread(this::poll, "reliable-event-example-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void poll() {
        while (running.get()) {
            try {
                List<MessageView> messages = consumer.receive(8, Duration.ofSeconds(15));
                for (MessageView message : messages) {
                    if (!running.get()) {
                        return;
                    }
                    try {
                        boolean applied = handler.handle(message);
                        consumer.ack(message);
                        LOG.info("event=example.order.consumed eventId={} applied={}",
                                message.getProperties().get("reliable_event_id"), applied);
                    } catch (Exception failure) {
                        LOG.warn("event=example.order.consume_failed exceptionType={} message={}",
                                failure.getClass().getName(), failure.getMessage());
                    }
                }
            } catch (Exception failure) {
                if (running.get()) {
                    LOG.warn("event=example.order.receive_failed exceptionType={} message={}",
                            failure.getClass().getName(), failure.getMessage());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    @Override
    public synchronized void stop() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 200;
    }
}
