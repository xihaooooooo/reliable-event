package dev.reliableevent.autoconfigure;

import dev.reliableevent.spi.TransportReceipt;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.autoconfigure.*;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import javax.sql.DataSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ReliableEventBaseRuntimeValidationAutoConfigurationTest {
    private final ApplicationContextRunner runner=new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(
            ReliableEventAutoConfiguration.class,ReliableEventTransportSelectionAutoConfiguration.class,
            ReliableEventBaseRuntimeValidationAutoConfiguration.class,
            ReliableEventPublicationAutoConfiguration.class));
    private ApplicationContextRunner database(){return runner.withBean(DataSource.class,()->mock(DataSource.class))
            .withBean(JdbcTemplate.class,()->mock(JdbcTemplate.class)).withBean(PlatformTransactionManager.class,()->mock(PlatformTransactionManager.class))
            .withBean(ObjectMapper.class,ObjectMapper::new);}
    @Test void defaultSchedulingWithoutSenderFails(){runner.run(ctx->assertThat(ctx).hasFailed().getFailure().hasMessageContaining("no EventTransport"));}
    @Test void disabledSchedulingIsExplicitIngressOnlyMode(){database().withPropertyValues("reliable-event.scheduling-enabled=false")
            .run(ctx->{assertThat(ctx).hasNotFailed();assertThat(ctx).hasSingleBean(dev.reliableevent.ReliableEventPublisher.class);assertThat(ctx).doesNotHaveBean(EventTransport.class);});}
    @Test void customSpiRunsWithNoRocketMqClassesOnClasspath(){
        database().withClassLoader(new FilteredClassLoader("org.apache.rocketmq.client.apis","dev.reliableevent.rocketmq"))
                .withBean(EventTransport.class,()->event->TransportReceipt.confirmed())
                .withPropertyValues("reliable-event.scheduling-enabled=false")
                .run(ctx->{assertThat(ctx).hasNotFailed();assertThat(ctx).hasSingleBean(EventTransport.class);assertThat(ctx).hasSingleBean(JdbcEventPublicationWorker.class);
                    assertThat(org.springframework.util.ClassUtils.isPresent("org.apache.rocketmq.client.apis.ClientServiceProvider",ctx.getClassLoader())).isFalse();});
    }

    @Test void primaryTransportDoesNotHideMultipleAdapterTransports() {
        // Adapter-owned names are excluded from the custom-transport selection check.
        var descriptor = new dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor() {
            @Override public String transportName() { return "test"; }
            @Override public String configurationNamespace() { return "test"; }
            @Override public java.util.Set<String> runtimeBeanNames() {
                return java.util.Set.of("firstTransport", "secondTransport");
            }
        };
        database().withBean("testAdapter", dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor.class,
                        () -> descriptor)
                .withBean("firstTransport", EventTransport.class,
                        () -> event -> TransportReceipt.confirmed(), definition -> definition.setPrimary(true))
                .withBean("secondTransport", EventTransport.class, () -> event -> TransportReceipt.confirmed())
                .withPropertyValues("reliable-event.transport=test", "reliable-event.scheduling-enabled=false")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure()
                        .hasMessageContaining("Exactly one EventTransport bean is required"));
    }
}
