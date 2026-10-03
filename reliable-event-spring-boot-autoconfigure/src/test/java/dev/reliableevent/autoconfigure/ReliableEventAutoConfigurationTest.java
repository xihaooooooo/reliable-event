package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.jdbc.DeadEventOperations;
import dev.reliableevent.jdbc.JdbcDeadEventOperations;
import dev.reliableevent.jdbc.JdbcPublishedEventRetention;
import dev.reliableevent.jdbc.PublishedRetentionResult;
import dev.reliableevent.jdbc.internal.cycle.JdbcEventPublicationCycle;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.tracing.RegistrationTracer;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.opentelemetry.OpenTelemetryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.tracing.OpenTelemetryTracingAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentCaptor.forClass;

class ReliableEventAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ReliableEventAutoConfiguration.class,
                    ReliableEventPublicationAutoConfiguration.class,
                    ReliableEventMetricsSnapshotAutoConfiguration.class,
                    ReliableEventTracingAutoConfiguration.class,
                    ReliableEventTransportSelectionAutoConfiguration.class,
                    ReliableEventTransportBridgeAutoConfiguration.class
            ))
            .withPropertyValues("reliable-event.scheduling-enabled=false");

    @Test
    void publicTransportUsesBridgeWithoutBrokerTypes() {
        runner.withBean(EventTransport.class, () -> event -> dev.reliableevent.spi.TransportReceipt.confirmed())
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(EventSender.class);
                    assertThat(context.getBean(EventSender.class))
                            .isInstanceOf(EventTransportSenderBridge.class);

                });
    }

    @Test
    void systemEnvironmentPropertyNamesUseBootRelaxedBindingAndRemainStrict() {
        runner.withBean(EventSender.class, () -> mock(EventSender.class))
                .withInitializer(context -> addSystemEnvironment(context,
                        "RELIABLEEVENT_CLAIMBATCHSIZE", "17"))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(ReliableEventProperties.class).getClaimBatchSize()).isEqualTo(17);
                });

        runner.withBean(EventSender.class, () -> mock(EventSender.class))
                .withInitializer(context -> addSystemEnvironment(context,
                        "RELIABLEEVENT_CLAIMBATCHSZIE", "17"))
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining("claimbatchszie"));
    }

    @Test
    void publicAndLegacySendingEntryPointsFailFastWhenBothAreDefined() {
        runner.withBean(EventTransport.class, () -> event -> dev.reliableevent.spi.TransportReceipt.confirmed())
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining("Both EventSender and EventTransport"));
    }

    @Test
    void multiplePublicTransportBeansFailEvenWhenOneIsPrimary() {
        jdbcRunner().withUserConfiguration(MultipleTransportConfiguration.class)
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining("Exactly one EventTransport bean is required"));
    }

    @Test
    void disabledReliableEventBacksOffFromAllSendingAssembly() {
        jdbcRunner().withBean(EventTransport.class,
                        () -> event -> dev.reliableevent.spi.TransportReceipt.confirmed())
                .withPropertyValues("reliable-event.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(EventSender.class);

                    assertThat(context).doesNotHaveBean(JdbcEventPublicationWorker.class);
                });
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MultipleTransportConfiguration {
        @Bean @Primary EventTransport primaryTransport() {
            return event -> dev.reliableevent.spi.TransportReceipt.confirmed();
        }
        @Bean EventTransport secondTransport() {
            return event -> dev.reliableevent.spi.TransportReceipt.confirmed();
        }
    }

    @Test
    void publishedRetentionIsOffByDefaultAndRequiresExplicitDuration() {
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(JdbcPublishedEventRetention.class);
                    assertThat(context).doesNotHaveBean(PublishedRetentionScheduler.class);
                });

        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withPropertyValues("reliable-event.published-retention-enabled=true")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void enabledRetentionHasBoundedSettingsAndScheduler() {
        JdbcPublishedEventRetention cleaner = mock(JdbcPublishedEventRetention.class);
        when(cleaner.runOnce(Duration.ofDays(7), 10))
                .thenReturn(new PublishedRetentionResult(0, 0, 0));
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(JdbcPublishedEventRetention.class, () -> cleaner)
                .withPropertyValues("reliable-event.published-retention-enabled=true",
                        "reliable-event.published-retention=7d",
                        "reliable-event.cleanup-batch-size=10",
                        "reliable-event.cleanup-interval=1h")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(PublishedRetentionScheduler.class);
                    assertThat(context.getBean(JdbcPublishedEventRetention.class)).isSameAs(cleaner);
                });
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withPropertyValues("reliable-event.published-retention-enabled=true",
                        "reliable-event.published-retention=7d",
                        "reliable-event.cleanup-batch-size=1001")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void deadOperationsRequireExplicitOptInWithoutAffectingPublisher() {
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);
                    assertThat(context).doesNotHaveBean(DeadEventOperations.class);
                });

        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withPropertyValues("reliable-event.dead-operations-enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);
                    assertThat(context).hasSingleBean(DeadEventOperations.class);
                    assertThat(context.getBean(DeadEventOperations.class))
                            .isInstanceOf(JdbcDeadEventOperations.class);
                });
    }

    @Test
    void customDeadOperationsOverrideIsPreserved() {
        DeadEventOperations custom = mock(DeadEventOperations.class);
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(DeadEventOperations.class, () -> custom)
                .withPropertyValues("reliable-event.dead-operations-enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(DeadEventOperations.class);
                    assertThat(context.getBean(DeadEventOperations.class)).isSameAs(custom);
                });
    }

    @Test
    void bootOpenTelemetryTracerEnablesTracingWithoutReplacingCustomObserver() {
        PublicationObserver observer = mock(PublicationObserver.class);
        jdbcRunner().withConfiguration(AutoConfigurations.of(
                        OpenTelemetryAutoConfiguration.class,
                        OpenTelemetryTracingAutoConfiguration.class,
                        MicrometerTracingAutoConfiguration.class))
                .withBean(PublicationObserver.class, () -> observer)
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(io.micrometer.core.instrument.MeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues("management.tracing.sampling.probability=1.0")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(io.micrometer.tracing.Tracer.class);
                    assertThat(context).hasSingleBean(io.micrometer.tracing.propagation.Propagator.class);
                    assertThat(context).hasSingleBean(RegistrationTracer.class);
                    assertThat(context).hasSingleBean(PublicationTracer.class);
                    assertThat(publicationTracerOf(context.getBean(JdbcEventPublicationWorker.class)))
                            .isInstanceOf(MicrometerPublicationTracer.class);
                    assertThat(context.getBean(PublicationObserver.class)).isSameAs(observer);
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);

                    io.micrometer.tracing.Tracer tracer = context.getBean(io.micrometer.tracing.Tracer.class);
                    io.micrometer.tracing.Span request = tracer.nextSpan().name("incoming-request").start();
                    org.mockito.ArgumentCaptor<Object[]> arguments = forClass(Object[].class);
                    try (io.micrometer.tracing.Tracer.SpanInScope ignored = tracer.withSpan(request)) {
                        JdbcTemplate jdbcTemplate = context.getBean(JdbcTemplate.class);
                        when(jdbcTemplate.update(any(PreparedStatementCreator.class), any(KeyHolder.class)))
                                .thenAnswer(invocation -> {
                                    KeyHolder keys = invocation.getArgument(1);
                                    keys.getKeyList().add(java.util.Map.of("id", 7123L));
                                    return 1;
                                });
                        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
                        TransactionSynchronizationManager.setActualTransactionActive(true);
                        try {
                            context.getBean(ReliableEventPublisher.class).publish(new ReliableEvent<>(
                                    "order-created", "boot-tracing-test", java.util.Map.of("orderId", 1),
                                    Instant.parse("2026-10-01T00:00:00Z"),
                                    java.util.Map.of("traceparent",
                                            "00-ffffffffffffffffffffffffffffffff-eeeeeeeeeeeeeeee-01")));
                        } finally {
                            TransactionSynchronizationManager.setActualTransactionActive(false);
                        }
                        assertThat(tracer.currentSpan().context()).isEqualTo(request.context());
                    } finally {
                        request.end();
                    }
                    assertThat(tracer.currentSpan()).isNull();
                    TransactionSynchronizationManager.setActualTransactionActive(true);
                    try {
                        context.getBean(ReliableEventPublisher.class).publish(new ReliableEvent<>(
                                "order-created", "boot-tracing-root-test", java.util.Map.of("orderId", 2),
                                Instant.parse("2026-10-01T00:00:00Z"), java.util.Map.of()));
                    } finally {
                        TransactionSynchronizationManager.setActualTransactionActive(false);
                    }
                    assertThat(tracer.currentSpan()).isNull();
                    JdbcTemplate jdbcTemplate = context.getBean(JdbcTemplate.class);
                    org.mockito.Mockito.verify(jdbcTemplate, org.mockito.Mockito.times(2))
                            .update(anyString(), arguments.capture());
                    String requestTraceparent = storedTraceparent((String) arguments.getAllValues().get(0)[4]);
                    assertThat(requestTraceparent).startsWith("00-" + request.context().traceId() + "-")
                            .isNotEqualTo("00-ffffffffffffffffffffffffffffffff-eeeeeeeeeeeeeeee-01");
                    String rootTraceparent = storedTraceparent((String) arguments.getAllValues().get(1)[4]);
                    assertThat(rootTraceparent).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
                    assertThat(rootTraceparent.split("-")[1]).isNotEqualTo(request.context().traceId());
                    assertThat(rootTraceparent).doesNotContain("ffffffffffffffffffffffffffffffff");
                });
    }

    @Test
    void tracingAndDefaultMicrometerPublicationObserverAreBothAssembled() {
        jdbcRunner().withConfiguration(AutoConfigurations.of(
                        OpenTelemetryAutoConfiguration.class,
                        OpenTelemetryTracingAutoConfiguration.class,
                        MicrometerTracingAutoConfiguration.class))
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(io.micrometer.core.instrument.MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(PublicationObserver.class))
                            .isInstanceOf(MicrometerPublicationObserver.class);
                    assertThat(context).hasSingleBean(RegistrationTracer.class);
                    assertThat(context).hasSingleBean(PublicationTracer.class);
                    assertThat(publicationTracerOf(context.getBean(JdbcEventPublicationWorker.class)))
                            .isInstanceOf(MicrometerPublicationTracer.class);
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);
                    assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class);
                });
    }

    @Test
    void defaultRegistryManagedRuntimeStartsItsPeriodicSampler() {
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues("reliable-event.scheduling-enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(MicrometerPublicationObserver.class);
                    assertThat(context).hasSingleBean(JdbcEventPublicationWorker.class);
                    assertThat(context).hasSingleBean(MetricsSnapshotSampler.class);
                    assertThat(context.getBean(MetricsSnapshotSampler.class).isRunning()).isTrue();
                    assertThat(context.getBean(SimpleMeterRegistry.class)
                            .get("reliable_event.snapshot.periodic_enabled").gauge().value()).isEqualTo(1);
                });
    }

    @Test
    void periodicSamplerRequiresItsExplicitDefaultRuntimePrerequisites() {
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues("reliable-event.enabled=false", "reliable-event.scheduling-enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class));

        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withPropertyValues("reliable-event.scheduling-enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class));

        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues("reliable-event.scheduling-enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class));

        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues("reliable-event.metrics-snapshot-enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class));

        PublicationObserver customObserver = mock(PublicationObserver.class);
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(PublicationObserver.class, () -> customObserver)
                .withPropertyValues("reliable-event.scheduling-enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(PublicationObserver.class)).isSameAs(customObserver);
                    assertThat(context).doesNotHaveBean(MicrometerPublicationObserver.class);
                    assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class);
                });

        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(JdbcEventPublicationCycle.class, () -> mock(JdbcEventPublicationCycle.class))
                .withPropertyValues("reliable-event.scheduling-enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class));

        ReliableEventScheduler customScheduler = mock(ReliableEventScheduler.class);
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(ReliableEventScheduler.class, () -> customScheduler)
                .withPropertyValues("reliable-event.scheduling-enabled=true")
                .run(context -> {
                    assertThat(context.getBean(ReliableEventScheduler.class)).isSameAs(customScheduler);
                    assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class);
                });
    }

    @Test
    void disabledTracingIgnoresCustomPublicationTracerForWorker() {
        PublicationTracer custom = mock(PublicationTracer.class);
        jdbcRunner().withConfiguration(AutoConfigurations.of(
                        OpenTelemetryAutoConfiguration.class,
                        OpenTelemetryTracingAutoConfiguration.class,
                        MicrometerTracingAutoConfiguration.class))
                .withPropertyValues("reliable-event.tracing-enabled=false")
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(PublicationTracer.class, () -> custom)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(publicationTracerOf(context.getBean(JdbcEventPublicationWorker.class)))
                            .isSameAs(PublicationTracer.NOOP);
                });
    }

    private static PublicationTracer publicationTracerOf(JdbcEventPublicationWorker worker) {
        try {
            var field = JdbcEventPublicationWorker.class.getDeclaredField("publicationTracer");
            field.setAccessible(true);
            return (PublicationTracer) field.get(worker);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
    }

    @Test
    void tracingDisableAndMetricsOnlyDoNotChangePublisherAvailability() {
        jdbcRunner().withBean(io.micrometer.core.instrument.MeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);
                    assertThat(context).hasSingleBean(PublicationObserver.class);
                    assertThat(context).doesNotHaveBean(RegistrationTracer.class);
                });

        jdbcRunner().withConfiguration(AutoConfigurations.of(
                        OpenTelemetryAutoConfiguration.class,
                        OpenTelemetryTracingAutoConfiguration.class,
                        MicrometerTracingAutoConfiguration.class))
                .withPropertyValues("reliable-event.tracing-enabled=false")
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(RegistrationTracer.class, () -> mock(RegistrationTracer.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(io.micrometer.tracing.Tracer.class);
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);
                    RegistrationTracer custom = context.getBean(RegistrationTracer.class);
                    JdbcTemplate jdbcTemplate = context.getBean(JdbcTemplate.class);
                    when(jdbcTemplate.update(any(PreparedStatementCreator.class), any(KeyHolder.class)))
                            .thenAnswer(invocation -> {
                                KeyHolder keys = invocation.getArgument(1);
                                keys.getKeyList().add(java.util.Map.of("id", 8123L));
                                return 1;
                            });
                    when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
                    TransactionSynchronizationManager.setActualTransactionActive(true);
                    try {
                        context.getBean(ReliableEventPublisher.class).publish(new ReliableEvent<>(
                                "order-created", "disabled-tracing-test", java.util.Map.of("orderId", 3),
                                Instant.parse("2026-10-01T00:00:00Z"),
                                java.util.Map.of("traceparent", "caller-context")));
                    } finally {
                        TransactionSynchronizationManager.setActualTransactionActive(false);
                    }
                    org.mockito.ArgumentCaptor<Object[]> arguments = forClass(Object[].class);
                    org.mockito.Mockito.verify(jdbcTemplate).update(anyString(), arguments.capture());
                    assertThat(storedTraceparent((String) arguments.getValue()[4]))
                            .isEqualTo("caller-context");
                    org.mockito.Mockito.verify(custom, org.mockito.Mockito.never()).begin(any());
                });
    }

    @Test
    void missingTracingClassesLeavePublisherAvailable() {
        jdbcRunner().withClassLoader(new FilteredClassLoader("io.micrometer.tracing"))
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventPublisher.class);
                    assertThat(context).doesNotHaveBean(RegistrationTracer.class);
                });
    }

    @Test
    void meterRegistryEnablesOptionalPublicationMetrics() {
        jdbcRunner()
                .withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(PublicationObserver.class);
                    assertThat(context.getBean(PublicationObserver.class))
                            .isInstanceOf(MicrometerPublicationObserver.class);
                    assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class);
                });
    }

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

                    assertThat(context).doesNotHaveBean(JdbcEventPublicationCycle.class);
                    assertThat(context).doesNotHaveBean(ReliableEventScheduler.class);
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
                    assertThat(context).doesNotHaveBean(ReliableEventScheduler.class);

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
    void schedulingCanStartWithCustomSenderAndBacksOffForCustomRuntime() {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        when(worker.findDueEventCandidates(org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(java.util.List.of());
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(JdbcExpiredLeaseRecovery.class, () -> recovery)
                .withBean(JdbcEventPublicationWorker.class, () -> worker)
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues("reliable-event.scheduling-enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ReliableEventScheduler.class);
                    assertThat(context.getBean(ReliableEventScheduler.class).isRunning()).isTrue();
                    assertThat(context).doesNotHaveBean(MetricsSnapshotSampler.class);
                });

        ReliableEventScheduler custom = mock(ReliableEventScheduler.class);
        jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                .withBean(ReliableEventScheduler.class, () -> custom)
                .withPropertyValues("reliable-event.scheduling-enabled=true")
                .run(context -> assertThat(context.getBean(ReliableEventScheduler.class)).isSameAs(custom));
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
    void generatedMetadataIncludesSchedulerOptions() throws IOException {
        try (InputStream resource = getClass().getClassLoader().getResourceAsStream(
                "META-INF/spring-configuration-metadata.json")) {
            assertThat(resource).isNotNull();
            assertThat(new String(resource.readAllBytes(), StandardCharsets.UTF_8))
                    .contains("\"name\": \"reliable-event.shutdown-timeout\"")
                    .contains("\"name\": \"reliable-event.adaptive-polling-enabled\"")
                    .contains("\"name\": \"reliable-event.active-poll-interval\"");
        }
    }

    @Test
    void invalidAndFuturePropertiesFailAtStartup() {
        for (String property : new String[]{
                "reliable-event.claim-batch-size=0",
                "reliable-event.lease-duration=0ms",
                "reliable-event.max-retry-delay=500ms",
                "reliable-event.poll-interval=0ms",
                "reliable-event.active-poll-interval=0ms",
                "reliable-event.worker-threads=0",
                "reliable-event.worker-queue-capacity=-1",
                "reliable-event.worker-threads=2147483647",
                "reliable-event.shutdown-timeout=0ms",
                "reliable-event.unrecognized-option=true"
        }) {
            jdbcRunner().withBean(EventSender.class, () -> mock(EventSender.class))
                    .withPropertyValues(property)
                    .run(context -> assertThat(context).hasFailed());
        }
    }

    private ApplicationContextRunner jdbcRunner() {
        return runner.withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(ObjectMapper.class, ObjectMapper::new);
    }

    private void addSystemEnvironment(
            org.springframework.context.ConfigurableApplicationContext context,
            String key,
            String value) {
        var environment = context.getEnvironment();
        environment.getPropertySources().addFirst(new org.springframework.core.env.SystemEnvironmentPropertySource(
                org.springframework.core.env.StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                java.util.Map.of(key, value)));
        org.springframework.boot.context.properties.source.ConfigurationPropertySources.attach(environment);
    }

    private String storedTraceparent(String headersJson) {
        try {
            return new ObjectMapper().readTree(headersJson).path("traceparent").asText();
        } catch (IOException exception) {
            throw new AssertionError("Publisher did not serialize trace headers as JSON", exception);
        }
    }
}
