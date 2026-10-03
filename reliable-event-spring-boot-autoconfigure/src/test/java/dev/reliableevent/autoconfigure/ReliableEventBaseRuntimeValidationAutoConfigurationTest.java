package dev.reliableevent.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.autoconfigure.*;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.spi.EventTransport;
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
            ReliableEventTransportBridgeAutoConfiguration.class,ReliableEventBaseRuntimeValidationAutoConfiguration.class,
            ReliableEventPublicationAutoConfiguration.class));
    private ApplicationContextRunner database(){return runner.withBean(DataSource.class,()->mock(DataSource.class))
            .withBean(JdbcTemplate.class,()->mock(JdbcTemplate.class)).withBean(PlatformTransactionManager.class,()->mock(PlatformTransactionManager.class))
            .withBean(ObjectMapper.class,ObjectMapper::new);}
    @Test void defaultSchedulingWithoutSenderFails(){runner.run(ctx->assertThat(ctx).hasFailed().getFailure().hasMessageContaining("no EventSender"));}
    @Test void disabledSchedulingIsExplicitIngressOnlyMode(){database().withPropertyValues("reliable-event.scheduling-enabled=false")
            .run(ctx->{assertThat(ctx).hasNotFailed();assertThat(ctx).hasSingleBean(dev.reliableevent.ReliableEventPublisher.class);assertThat(ctx).doesNotHaveBean(EventSender.class);});}
    @Test void customSpiRunsWithNoRocketMqClassesOnClasspath(){
        database().withClassLoader(new FilteredClassLoader("org.apache.rocketmq.client.apis","dev.reliableevent.rocketmq"))
                .withBean(EventTransport.class,()->event->dev.reliableevent.spi.TransportReceipt.confirmed())
                .withPropertyValues("reliable-event.scheduling-enabled=false")
                .run(ctx->{assertThat(ctx).hasNotFailed();assertThat(ctx).hasSingleBean(EventSender.class);assertThat(ctx).hasSingleBean(JdbcEventPublicationWorker.class);
                    assertThat(org.springframework.util.ClassUtils.isPresent("org.apache.rocketmq.client.apis.ClientServiceProvider",ctx.getClassLoader())).isFalse();});
    }
}
