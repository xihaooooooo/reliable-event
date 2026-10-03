package dev.reliableevent.rocketmq.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.autoconfigure.*;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.rocketmq.EventDestinationResolver;
import dev.reliableevent.rocketmq.RocketMqDestination;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor;
import dev.reliableevent.spi.TransportReceipt;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ReliableEventRocketMqAutoConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ReliableEventAutoConfiguration.class,
                    ReliableEventTransportSelectionAutoConfiguration.class,
                    ReliableEventRocketMqAutoConfiguration.class,
                    ReliableEventTransportBridgeAutoConfiguration.class,
                    ReliableEventPublicationAutoConfiguration.class,
                    ReliableEventMetricsSnapshotAutoConfiguration.class,
                    ReliableEventTracingAutoConfiguration.class));

    private ApplicationContextRunner database() {
        return runner.withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(ObjectMapper.class, ObjectMapper::new);
    }

    @Test
    void defaultRocketMqFlowsThroughSpiBridgeToWorkerAndKeepsDefaultSampler() throws Exception {
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        ProducerBuilder builder = mock(ProducerBuilder.class, RETURNS_SELF);
        Producer producer = mock(Producer.class);
        when(provider.newProducerBuilder()).thenReturn(builder);
        when(builder.build()).thenReturn(producer);

        database().withBean(ClientServiceProvider.class, () -> provider)
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues(
                        "reliable-event.rocketmq.endpoints=localhost:8081",
                        "reliable-event.rocketmq.mappings.order.destination=events:created")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(EventTransport.class);
                    assertThat(context).hasSingleBean(EventSender.class);
                    assertThat(context).hasSingleBean(JdbcEventPublicationWorker.class);
                    assertThat(context).hasBean("reliableEventMetricsSnapshotSampler");
                });

        verify(builder).setTopics("events");
        verify(builder).setMaxAttempts(1);
        verify(producer).close();
    }

    @Test
    void customProducerIsNotClosedByAutoConfiguration() throws Exception {
        Producer producer = mock(Producer.class);
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        database().withBean(Producer.class, () -> producer, definition -> definition.setDestroyMethodName(""))
                .withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues("reliable-event.rocketmq.mappings.order.destination=events:created")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(EventTransport.class);
                    assertThat(context.getBean(Producer.class)).isSameAs(producer);
                });
        verify(producer, never()).close();
        verify(provider, never()).newProducerBuilder();
    }

    @Test
    void customResolverWithoutProducerFailsClearlyBeforeClientBuild() {
        EventDestinationResolver resolver = eventType -> new RocketMqDestination("events", null);
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        database().withBean(EventDestinationResolver.class, () -> resolver)
                .withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues("reliable-event.rocketmq.endpoints=localhost:8081")
                .run(context -> assertThat(context.getStartupFailure())
                        .hasMessageContaining("Custom EventDestinationResolver requires a custom Producer"));
        verify(provider, never()).newProducerBuilder();
    }

    @Test
    void customResolverAndProducerNeedNoMappingOrGeneratedClientConfiguration() {
        EventDestinationResolver resolver = eventType -> new RocketMqDestination("events", null);
        database().withBean(EventDestinationResolver.class, () -> resolver)
                .withBean(Producer.class, () -> mock(Producer.class), definition -> definition.setDestroyMethodName(""))
                .withBean(ClientServiceProvider.class, () -> mock(ClientServiceProvider.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(EventTransport.class);
                    assertThat(context).doesNotHaveBean(ClientConfiguration.class);
                });
    }

    @Test
    void customCredentialProviderOverridesStaticCredentialProperties() throws Exception {
        SessionCredentialsProvider credentials = mock(SessionCredentialsProvider.class);
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        ProducerBuilder builder = mock(ProducerBuilder.class, RETURNS_SELF);
        when(provider.newProducerBuilder()).thenReturn(builder);
        when(builder.build()).thenReturn(mock(Producer.class));

        database().withBean(SessionCredentialsProvider.class, () -> credentials)
                .withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues(
                        "reliable-event.rocketmq.endpoints=localhost:8081",
                        "reliable-event.rocketmq.mappings.order.destination=events:created",
                        "reliable-event.rocketmq.access-key=public",
                        "reliable-event.rocketmq.secret-key=private")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(ClientConfiguration.class).getCredentialsProvider())
                            .contains(credentials);
                });
    }

    @Test
    void strictBindingAllowsDynamicMappingNamesButRejectsNestedAndRootTypos() {
        for (String property : new String[]{
                "reliable-event.rocketmq.mappings.order.destinaton=events",
                "reliable-event.claim-batch-szie=10"}) {
            database().withPropertyValues(
                            "reliable-event.rocketmq.endpoints=localhost:8081",
                            "reliable-event.rocketmq.mappings.order.destination=events:created",
                            property)
                    .run(context -> assertThat(context).hasFailed());
        }
    }

    @Test
    void explicitCustomWithRocketSettingsDoesNotCreateProducer() {
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        database().withBean(ClientServiceProvider.class, () -> provider)
                .withBean(EventTransport.class, () -> event -> TransportReceipt.confirmed())
                .withPropertyValues(
                        "reliable-event.transport=custom",
                        "reliable-event.scheduling-enabled=false",
                        "reliable-event.rocketmq.access-key=sensitive",
                        "reliable-event.rocketmq.secret-key=secret")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(Producer.class);
                    assertThat(context).hasSingleBean(EventSender.class);
                });
        verify(provider, never()).newProducerBuilder();
    }

    @Test
    void explicitRocketWithoutAdapterFailsBeforeAnyClient() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ReliableEventAutoConfiguration.class,
                        ReliableEventTransportSelectionAutoConfiguration.class))
                .withPropertyValues("reliable-event.transport=rocketmq")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining("not installed"));
    }

    @Test
    void explicitRegisteredSecondAdapterUsesItsSpiForWorker() {
        AtomicReference<String> sentType = new AtomicReference<>();
        TransportAdapterDescriptor descriptor = new TransportAdapterDescriptor() {
            @Override public String transportName() { return "second"; }
            @Override public String configurationNamespace() { return "second"; }
            @Override public Set<String> runtimeBeanNames() { return Set.of("secondTransport"); }
        };

        database().withBean("secondAdapter", TransportAdapterDescriptor.class, () -> descriptor)
                .withBean("secondTransport", EventTransport.class, () -> event -> {
                    sentType.set(event.eventType());
                    return TransportReceipt.confirmed();
                })
                .withPropertyValues("reliable-event.transport=second", "reliable-event.scheduling-enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(JdbcEventPublicationWorker.class);
                    context.getBean(EventSender.class).send(
                            new StoredEvent(new EventId(1), "OrderCreated", "key", "{}", "{}"));
                    assertThat(sentType).hasValue("OrderCreated");
                });
    }

    @Test
    void duplicateCustomTransportBeansFailEvenWhenOneIsPrimary() {
        database().withBean("transportOne", EventTransport.class, () -> event -> TransportReceipt.confirmed())
                .withBean("transportTwo", EventTransport.class, () -> event -> TransportReceipt.confirmed(),
                        definition -> definition.setPrimary(true))
                .withPropertyValues("reliable-event.transport=custom", "reliable-event.scheduling-enabled=false")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining("Multiple custom EventSender/EventTransport"));
    }

    @Test
    void providerGetsExactlyOneAttemptAndLeaseMustExceedRequestTimeout() throws Exception {
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        ProducerBuilder builder = mock(ProducerBuilder.class, RETURNS_SELF);
        when(provider.newProducerBuilder()).thenReturn(builder);
        when(builder.build()).thenReturn(mock(Producer.class));

        database().withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues(
                        "reliable-event.rocketmq.endpoints=localhost:8081",
                        "reliable-event.rocketmq.request-timeout=40s",
                        "reliable-event.rocketmq.mappings.order.destination=events:created")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining("lease-duration must exceed RocketMQ client request timeout"));
        verify(provider, never()).newProducerBuilder();
    }

    @Test
    void missingDataSourceDoesNotCreateProducerOrConnectToBroker() {
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        runner.withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues(
                        "reliable-event.scheduling-enabled=false",
                        "reliable-event.rocketmq.endpoints=localhost:8081",
                        "reliable-event.rocketmq.mappings.order-created.destination=events:created")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(Producer.class);
                });
        verify(provider, never()).newProducerBuilder();
    }

    @Test
    void missingRocketMqClassesBackOffWithoutLoadingRuntimeConfiguration() {
        runner.withClassLoader(new FilteredClassLoader(
                        "org.apache.rocketmq.client.apis", "dev.reliableevent.rocketmq"))
                .withPropertyValues("reliable-event.scheduling-enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(Producer.class);
                    assertThat(context).doesNotHaveBean(EventTransport.class);
                });
    }

    @Test
    void rocketMappingYamlPreservesEventTypeCaseAndHyphens() {
        String yaml = """
                reliable-event:
                  scheduling-enabled: false
                  rocketmq:
                    endpoints: localhost:8081
                    mappings:
                      Order-Created:
                        destination: events:created
                      coupon-task-execute:
                        destination: coupons:execute
                """;
        database().withBean(ClientServiceProvider.class, () -> mock(ClientServiceProvider.class))
                .withBean(Producer.class, () -> mock(Producer.class))
                .withInitializer(context -> {
                    var source = new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8));
                    try {
                        context.getEnvironment().getPropertySources().addFirst(
                                new YamlPropertySourceLoader().load("test-yaml", source).get(0));
                    } catch (java.io.IOException exception) {
                        throw new IllegalStateException(exception);
                    }
                })
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(RocketMqProperties.class).getMappings())
                            .containsKeys("Order-Created", "coupon-task-execute");
                    assertThat(context.getBean(EventDestinationResolver.class).resolve("Order-Created"))
                            .isEqualTo(new RocketMqDestination("events", "created"));
                });
    }

    @Test
    void malformedRocketConfigurationFailsBeforeProducerBuild() {
        for (String property : new String[]{
                "reliable-event.rocketmq.endpoints=",
                "reliable-event.rocketmq.mappings.order-created.destination=",
                "reliable-event.rocketmq.mappings.order-created.destination=bad topic",
                "reliable-event.rocketmq.mappings.order-created.destination=bad:tag:extra",
                "reliable-event.rocketmq.access-key=public",
                "reliable-event.rocketmq.request-timeout=40s"
        }) {
            ClientServiceProvider provider = mock(ClientServiceProvider.class);
            database().withBean(ClientServiceProvider.class, () -> provider)
                    .withPropertyValues(
                            "reliable-event.rocketmq.endpoints=localhost:8081",
                            "reliable-event.rocketmq.mappings.order-created.destination=events:created",
                            property)
                    .run(context -> assertThat(context).hasFailed());
            verify(provider, never()).newProducerBuilder();
        }
    }
}
