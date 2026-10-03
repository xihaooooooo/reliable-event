package dev.reliableevent.kafka.autoconfigure;

import dev.reliableevent.autoconfigure.ReliableEventProperties;
import dev.reliableevent.autoconfigure.ReliableEventTransportSelection;
import dev.reliableevent.autoconfigure.ReliableEventTransportSelectionAutoConfiguration;
import dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor;
import dev.reliableevent.kafka.KafkaEventTransport;
import dev.reliableevent.spi.EventTransport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ReliableEventKafkaAutoConfigurationTest {
    private ApplicationContextRunner runner() {
        return runner(Duration.ofSeconds(30));
    }

    private ApplicationContextRunner runner(Duration leaseDuration) {
        ReliableEventProperties commonProperties = new ReliableEventProperties();
        commonProperties.setLeaseDuration(leaseDuration);
        return new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ReliableEventTransportSelectionAutoConfiguration.class,
                    ReliableEventKafkaAutoConfiguration.class))
            .withUserConfiguration(RuntimePrerequisites.class)
            .withBean(com.fasterxml.jackson.databind.ObjectMapper.class,
                    com.fasterxml.jackson.databind.ObjectMapper::new)
            .withBean(ReliableEventProperties.class, () -> commonProperties)
            .withPropertyValues(
                    "reliable-event.enabled=true",
                    "reliable-event.scheduling-enabled=false",
                    "reliable-event.kafka.mappings.order-created.topic=orders");
    }

    @Test
    void selectedKafkaInstallsPublicTransportAndUsesCustomProducerBudget() {
        Producer<String, byte[]> producer = mock(Producer.class);
        runner().withBean("userProducer", Producer.class, () -> producer)
                .withPropertyValues("reliable-event.kafka.custom-producer-send-budget=5s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventTransportSelection.class);
                    assertThat(context.getBean(ReliableEventTransportSelection.class).transport()).isEqualTo("kafka");
                    assertThat(context).hasSingleBean(EventTransport.class);
                    assertThat(context.getBean(EventTransport.class)).isInstanceOf(KafkaEventTransport.class);
                    assertThat(context).doesNotHaveBean(ReliableEventKafkaProducer.class);
                    assertThat(context).doesNotHaveBean(KafkaProducerOwner.class);
                });
    }

    @Test
    void defaultKafkaProducerIsConstructedWithoutBrokerAndOwnedByBoundedCloser() {
        runner().withPropertyValues(
                        "reliable-event.kafka.bootstrap-servers=127.0.0.1:1",
                        "reliable-event.kafka.shutdown-timeout=1s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventKafkaProducer.class);
                    assertThat(context).hasSingleBean(KafkaProducerOwner.class);
                    assertThat(context.getBean(EventTransport.class)).isInstanceOf(KafkaEventTransport.class);
                    assertThat(context.getBean(KafkaProducerOwner.class)).isNotNull();
                });
    }

    @Test
    void secondTransportCanBeExplicitlySelectedWithoutCreatingKafkaRuntime() {
        runner().withUserConfiguration(OtherTransportConfiguration.class)
                .withPropertyValues("reliable-event.transport=other")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(ReliableEventTransportSelection.class).transport()).isEqualTo("other");
                    assertThat(context).doesNotHaveBean(KafkaEventTransport.class);
                    assertThat(context).doesNotHaveBean(Producer.class);
                });
    }

    @Test
    void installedAdaptersRequireAnExplicitChoiceBeforeAnyProducerIsCreated() {
        AtomicInteger producerCreations = new AtomicInteger();
        runner().withUserConfiguration(OtherTransportConfiguration.class)
                .withBean("producerCreations", AtomicInteger.class, () -> producerCreations)
                .withUserConfiguration(ProducerCreationCounter.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "Multiple built-in transports are installed; set reliable-event.transport explicitly");
                });
        assertThat(producerCreations).hasValue(0);
    }

    @Test
    void duplicateTypedProducersFailEvenWhenOneIsPrimary() {
        runner().withUserConfiguration(DualProducerConfiguration.class)
                .withPropertyValues("reliable-event.kafka.custom-producer-send-budget=5s")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "Selected Kafka transport requires exactly one Producer<String, byte[]> bean; found 2");
                });
    }

    @Test
    void customBudgetIsRequiredAndLeaseMustExceedItPlusDatabaseReserve() {
        Producer<String, byte[]> producer = mock(Producer.class);
        runner().withBean("userProducer", Producer.class, () -> producer)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "reliable-event.kafka.custom-producer-send-budget must be configured when a custom Producer<String, byte[]> is used");
                });

        runner(Duration.ofSeconds(6)).withBean("userProducer", Producer.class, () -> producer)
                .withPropertyValues("reliable-event.kafka.custom-producer-send-budget=5s")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "reliable-event.lease-duration must strictly exceed Kafka send budget plus one-second state-update reserve");
                });
    }

    @Test
    void aRealUserKafkaProducerUsesDeclaredBudgetAndIsNotLibraryOwned() {
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        try {
            runner().withBean("userProducer", Producer.class, () -> producer)
                    .withPropertyValues("reliable-event.kafka.custom-producer-send-budget=8s")
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        assertThat(context.getBean(EventTransport.class)).isInstanceOf(KafkaEventTransport.class);
                        assertThat(context).doesNotHaveBean(KafkaProducerOwner.class);
                        assertThat(context).doesNotHaveBean(ReliableEventKafkaProducer.class);
                    });
        } finally {
            producer.close(Duration.ofSeconds(1));
        }
    }

    @Test
    void aRealUserKafkaProducerMustRespectItsDeclaredBudgetInsteadOfTheDefaultBudget() {
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        try {
            runner().withBean("userProducer", Producer.class, () -> producer)
                    .withPropertyValues("reliable-event.kafka.custom-producer-send-budget=40s")
                    .run(context -> {
                        assertThat(context).hasFailed();
                        assertThat(context.getStartupFailure()).hasRootCauseMessage(
                                "reliable-event.lease-duration must strictly exceed Kafka send budget plus one-second state-update reserve");
                    });
        } finally {
            producer.close(Duration.ofSeconds(1));
        }
    }

    @Test
    void clientPropertyTypoAndBudgetOverridesAreRejectedBeforeOpeningClient() {
        runner().withPropertyValues(
                        "reliable-event.kafka.bootstrap-servers=127.0.0.1:1",
                        "reliable-event.kafka.producer-properties.acks=1")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "reliable-event.kafka.producer-properties.acks is controlled by the ReliableEvent send budget");
                });

        runner().withPropertyValues(
                        "reliable-event.kafka.bootstrap-servers=127.0.0.1:1",
                        "reliable-event.kafka.producer-properties.bootstrap.servers=evil:9092")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "reliable-event.kafka.producer-properties.bootstrap.servers is controlled by the ReliableEvent send budget");
                });

        runner().withPropertyValues(
                        "reliable-event.kafka.bootstrap-servers=127.0.0.1:1",
                        "reliable-event.kafka.producer-properties.max.blok.ms=3000")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "reliable-event.kafka.producer-properties.max.blok.ms is not a Kafka producer setting");
                });
    }

    @Test
    void defaultRequestLimitIncludesBodyAndHeaderAllowanceAndCannotBeOverridden() {
        KafkaProperties properties = new KafkaProperties();
        properties.setBootstrapServers("127.0.0.1:1");
        properties.getMappings().put("order-created", new KafkaProperties.Mapping());
        properties.getMappings().get("order-created").setTopic("orders");

        Map<String, Object> configuration = ReliableEventKafkaAutoConfiguration.producerConfiguration(properties);

        assertThat(configuration).containsEntry(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                KafkaEventTransport.DEFAULT_MAX_BODY_BYTES + 32 * 1024);
        properties.setMaxBodyBytes(Integer.MAX_VALUE);
        assertThat(org.assertj.core.api.Assertions.catchThrowable(
                () -> ReliableEventKafkaAutoConfiguration.producerConfiguration(properties)))
                .hasMessage("reliable-event.kafka.max-body-bytes plus Kafka record overhead must fit in max-request-size");
        properties.setMaxBodyBytes(KafkaEventTransport.DEFAULT_MAX_BODY_BYTES);
        properties.getProducerProperties().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1024");
        assertThat(org.assertj.core.api.Assertions.catchThrowable(
                () -> ReliableEventKafkaAutoConfiguration.producerConfiguration(properties)))
                .hasMessage("reliable-event.kafka.producer-properties.max.request.size is controlled by the ReliableEvent send budget");
    }

    @Test
    void kafkaNamespaceAndDynamicMappingObjectsRejectUnknownFields() {
        Producer<String, byte[]> producer = mock(Producer.class);
        runner().withBean("userProducer", Producer.class, () -> producer)
                .withPropertyValues(
                        "reliable-event.kafka.custom-producer-send-budget=5s",
                        "reliable-event.kafka.unknown-setting=true")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("unknown-setting");
                });

        runner().withBean("userProducer", Producer.class, () -> producer)
                .withPropertyValues(
                        "reliable-event.kafka.custom-producer-send-budget=5s",
                        "reliable-event.kafka.mappings.order-created.typo=unexpected")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("mappings.order-created.typo");
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class RuntimePrerequisites {
        @Bean DataSource dataSource() { return mock(DataSource.class); }
        @Bean JdbcTemplate jdbcTemplate() { return mock(JdbcTemplate.class); }
        @Bean PlatformTransactionManager transactionManager() { return mock(PlatformTransactionManager.class); }
    }

    @Configuration(proxyBeanMethods = false)
    static class OtherTransportConfiguration {
        @Bean
        TransportAdapterDescriptor otherDescriptor() {
            return new TransportAdapterDescriptor() {
                @Override public String transportName() { return "other"; }
                @Override public String configurationNamespace() { return "other"; }
                @Override public Set<String> runtimeBeanNames() { return Set.of("otherTransport"); }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DualProducerConfiguration {
        @Bean
        @Primary
        Producer<String, byte[]> primaryProducer() { return mock(Producer.class); }

        @Bean
        Producer<String, byte[]> secondaryProducer() { return mock(Producer.class); }
    }

    @Configuration(proxyBeanMethods = false)
    static class ProducerCreationCounter {
        @Bean
        static BeanPostProcessor producerCreationCounter(AtomicInteger producerCreations) {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (bean instanceof Producer<?, ?>) {
                        producerCreations.incrementAndGet();
                    }
                    return bean;
                }
            };
        }
    }
}
