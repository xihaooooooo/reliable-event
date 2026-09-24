package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.internal.cycle.JdbcEventPublicationCycle;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.rocketmq.EventDestinationResolver;
import dev.reliableevent.rocketmq.RocketMqDestination;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReliableEventAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ReliableEventAutoConfiguration.class,
                    ReliableEventPublicationAutoConfiguration.class
            ));

    @Test
    void disabledCreatesNoDefaultBeans() {
        runner.withPropertyValues("reliable-event.enabled=false")
                .withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(ReliableEventPublisher.class);
                    assertThat(context).doesNotHaveBean(Producer.class);
                    assertThat(context).doesNotHaveBean(JdbcEventPublicationCycle.class);
                });
    }

    @Test
    void customSenderAllowsJdbcAssemblyWithoutBrokerSettings() {
        jdbcRunner()
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .withPropertyValues("reliable-event.max-attempts=3")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);
                    assertThat(context).hasSingleBean(JdbcExpiredLeaseRecovery.class);
                    assertThat(context).hasSingleBean(JdbcEventPublicationWorker.class);
                    assertThat(context).hasSingleBean(JdbcEventPublicationCycle.class);
                    assertThat(context).doesNotHaveBean(Producer.class);
                });
    }

    @Test
    void publisherOverrideDoesNotPreventPublicationAssembly() {
        ReliableEventPublisher custom = mock(ReliableEventPublisher.class);
        jdbcRunner().withBean(ReliableEventPublisher.class, () -> custom)
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).getBean(ReliableEventPublisher.class).isSameAs(custom);
                    assertThat(context).hasSingleBean(JdbcEventPublicationCycle.class);
                });
    }

    @Test
    void defaultProducerUsesMappedTopicsOneAttemptAndCloses() throws Exception {
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        ProducerBuilder builder = mock(ProducerBuilder.class, RETURNS_SELF);
        Producer producer = mock(Producer.class);
        when(provider.newProducerBuilder()).thenReturn(builder);
        when(builder.build()).thenReturn(producer);
        jdbcRunner().withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues(
                        "reliable-event.rocketmq.endpoints=localhost:8081",
                        "reliable-event.rocketmq.mappings.order-created.destination=events:created",
                        "reliable-event.rocketmq.mappings.order-updated.destination=events:updated"
                ).run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).getBean(Producer.class).isSameAs(producer);
                    assertThat(context).hasSingleBean(EventSender.class);
                    assertThat(context).hasSingleBean(JdbcEventPublicationCycle.class);
                });
        verify(builder).setTopics("events");
        verify(builder).setMaxAttempts(1);
        verify(producer).close();
    }

    @Test
    void customProducerIsNotClosedByAutoConfiguration() throws Exception {
        Producer producer = mock(Producer.class);
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        jdbcRunner().withBean(Producer.class, () -> producer,
                        definition -> definition.setDestroyMethodName(""))
                .withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues("reliable-event.rocketmq.mappings.order-created.destination=events:created")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).getBean(Producer.class).isSameAs(producer);
                });
        org.mockito.Mockito.verify(producer, org.mockito.Mockito.never()).close();
    }

    @Test
    void customResolverWithoutProducerFailsClearly() {
        EventDestinationResolver resolver = eventType -> new RocketMqDestination("events", null);
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        jdbcRunner().withBean(EventDestinationResolver.class, () -> resolver)
                .withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues("reliable-event.rocketmq.endpoints=localhost:8081")
                .run(context -> assertThat(context.getStartupFailure())
                        .hasMessageContaining("Custom EventDestinationResolver requires a custom Producer"));
    }

    @Test
    void customResolverAndProducerNeedNoMapping() {
        EventDestinationResolver resolver = eventType -> new RocketMqDestination("events", null);
        jdbcRunner().withBean(EventDestinationResolver.class, () -> resolver)
                .withBean(Producer.class, () -> mock(Producer.class),
                        definition -> definition.setDestroyMethodName(""))
                .withBean(ClientServiceProvider.class, () -> mock(ClientServiceProvider.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(EventSender.class);
                    assertThat(context).doesNotHaveBean(ClientConfiguration.class);
                });
    }

    @Test
    void customCredentialProviderTakesPrecedenceOverStaticProperties() throws Exception {
        SessionCredentialsProvider credentials = mock(SessionCredentialsProvider.class);
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        ProducerBuilder builder = mock(ProducerBuilder.class, RETURNS_SELF);
        when(provider.newProducerBuilder()).thenReturn(builder);
        when(builder.build()).thenReturn(mock(Producer.class));
        jdbcRunner().withBean(SessionCredentialsProvider.class, () -> credentials)
                .withBean(ClientServiceProvider.class, () -> provider)
                .withPropertyValues(
                        "reliable-event.rocketmq.endpoints=localhost:8081",
                        "reliable-event.rocketmq.mappings.order-created.destination=events:created",
                        "reliable-event.rocketmq.access-key=public",
                        "reliable-event.rocketmq.secret-key=private"
                ).run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(ClientConfiguration.class).getCredentialsProvider())
                            .contains(credentials);
                });
    }

    @Test
    void missingJdbcClassBacksOff() {
        runner.withClassLoader(new FilteredClassLoader(JdbcTemplate.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(ReliableEventPublisher.class);
                });
    }

    @Test
    void missingRocketMqClassBacksOffWithoutLoadingItsConfiguration() {
        runner.withClassLoader(new FilteredClassLoader(
                        "org.apache.rocketmq.client.apis", "dev.reliableevent.rocketmq"))
                .withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);
                    assertThat(context).doesNotHaveBean(EventSender.class);
                });
    }

    @Test
    void missingDataSourceDoesNotConnectToBroker() {
        ClientServiceProvider provider = mock(ClientServiceProvider.class);
        runner.withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(ClientServiceProvider.class, () -> provider)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(ReliableEventPublisher.class);
                    assertThat(context).doesNotHaveBean(Producer.class);
                });
        org.mockito.Mockito.verify(provider, org.mockito.Mockito.never()).newProducerBuilder();
    }

    @Test
    void yamlBindsEventTypeWithHyphenAndPreservesCase() {
        String yaml = """
                reliable-event:
                  rocketmq:
                    mappings:
                      Order-Created:
                        destination: events:created
                      coupon-task-execute:
                        destination: coupons:execute
                """;
        runner.withInitializer(context -> {
                    var source = new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8));
                    try {
                        context.getEnvironment().getPropertySources().addFirst(
                                new YamlPropertySourceLoader().load("test-yaml", source).get(0)
                        );
                    } catch (java.io.IOException exception) {
                        throw new IllegalStateException(exception);
                    }
                })
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ReliableEventProperties properties = context.getBean(ReliableEventProperties.class);
                    assertThat(properties.destinations()).containsKeys("Order-Created", "coupon-task-execute");
                    assertThat(properties.destinations().get("Order-Created"))
                            .isEqualTo(new RocketMqDestination("events", "created"));
                });
    }

    @Test
    void invalidAndFuturePropertiesFailAtStartup() {
        for (String property : new String[]{
                "reliable-event.claim-batch-size=0",
                "reliable-event.lease-duration=0ms",
                "reliable-event.max-retry-delay=500ms",
                "reliable-event.poll-interval=1s"
        }) {
            jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                    .withPropertyValues(property)
                    .run(context -> assertThat(context).hasFailed());
        }
    }

    @Test
    void invalidDestinationAndCredentialPairFailBeforeProducerBuild() {
        for (String property : new String[]{
                "reliable-event.rocketmq.mappings.order-created.destination=bad:tag:extra",
                "reliable-event.rocketmq.access-key=public",
                "reliable-event.rocketmq.request-timeout=40s"
        }) {
            ClientServiceProvider provider = mock(ClientServiceProvider.class);
            jdbcRunner().withBean(ClientServiceProvider.class, () -> provider)
                    .withPropertyValues(
                            "reliable-event.rocketmq.endpoints=localhost:8081",
                            "reliable-event.rocketmq.mappings.order-created.destination=events:created",
                            property
                    )
                    .run(context -> assertThat(context).hasFailed());
            org.mockito.Mockito.verify(provider, org.mockito.Mockito.never()).newProducerBuilder();
        }
    }

    @Test
    void invalidOrMissingBrokerConfigurationFailsBeforeConnection() {
        for (String property : new String[]{
                "reliable-event.rocketmq.endpoints=",
                "reliable-event.rocketmq.mappings.order-created.destination=",
                "reliable-event.rocketmq.mappings.order-created.destination=bad topic"
        }) {
            ClientServiceProvider provider = mock(ClientServiceProvider.class);
            jdbcRunner().withBean(ClientServiceProvider.class, () -> provider)
                    .withPropertyValues(
                            "reliable-event.rocketmq.endpoints=localhost:8081",
                            "reliable-event.rocketmq.mappings.order-created.destination=events:created",
                            property
                    )
                    .run(context -> assertThat(context).hasFailed());
            org.mockito.Mockito.verify(provider, org.mockito.Mockito.never()).newProducerBuilder();
        }
    }

    private ApplicationContextRunner jdbcRunner() {
        return runner.withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(ObjectMapper.class, ObjectMapper::new);
    }
}
