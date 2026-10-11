package dev.reliableevent.benchmark;

import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.rocketmq.RocketMqEventTransport;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class BenchmarkApplication {

    public static void main(String[] args) {
        SpringApplication.run(BenchmarkApplication.class, args);
    }

    /** The load process registers events but never sends them itself. */
    @Bean
    @ConditionalOnProperty(prefix = "benchmark", name = "mode", havingValue = "load")
    EventTransport benchmarkLoadOnlySender() {
        return event -> {
            throw new IllegalStateException("Benchmark load process must not publish events");
        };
    }

    @Bean
    @ConditionalOnProperty(prefix = "benchmark", name = "mode", havingValue = "publisher")
    MeterRegistry benchmarkMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    /** Fault injection still sends to the real Broker, then loses one successful receipt. */
    @Bean
    @ConditionalOnProperty(prefix = "benchmark", name = "drop-first-receipt", havingValue = "true")
    static BeanPostProcessor dropFirstRealReceipt() {
        AtomicBoolean drop = new AtomicBoolean(true);
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (!(bean instanceof RocketMqEventTransport sender)) {
                    return bean;
                }
                return (EventTransport) event -> {
                    var receipt = sender.send(event);
                    if (drop.compareAndSet(true, false)) {
                        throw new TransportException(TransportFailureType.RESULT_UNKNOWN, "Benchmark discarded one real success receipt");
                    }
                    return receipt;
                };
            }
        };
    }
}
