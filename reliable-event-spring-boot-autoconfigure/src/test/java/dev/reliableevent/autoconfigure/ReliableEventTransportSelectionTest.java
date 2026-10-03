package dev.reliableevent.autoconfigure;

import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.autoconfigure.spi.TransportAdapterDescriptor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

class ReliableEventTransportSelectionTest {
    private static TransportAdapterDescriptor adapter(String name, String namespace) {
        return new TransportAdapterDescriptor() {
            @Override public String transportName() { return name; }
            @Override public String configurationNamespace() { return namespace; }
        };
    }

    @Test
    void defaultsToTheOnlyInstalledAdapter() {
        var result = ReliableEventTransportSelection.resolve(
                new MockEnvironment(), List.of(adapter("rocketmq", "rocketmq")), new StaticListableBeanFactory());
        assertThat(result.transport()).isEqualTo("rocketmq");
    }

    @Test
    void customLegacySenderSuppressesTheDefaultAdapter() {
        var beans = new StaticListableBeanFactory();
        beans.addBean("legacy", (EventSender) event -> null);
        var result = ReliableEventTransportSelection.resolve(
                new MockEnvironment(), List.of(adapter("rocketmq", "rocketmq")), beans);
        assertThat(result.transport()).isEqualTo("custom");
    }

    @Test
    void multipleInstalledAdaptersRequireExplicitChoice() {
        assertThatIllegalStateException().isThrownBy(() -> ReliableEventTransportSelection.resolve(
                new MockEnvironment(), List.of(adapter("one", "one"), adapter("two", "two")),
                new StaticListableBeanFactory())).withMessageContaining("explicitly");
    }

    @Test
    void explicitMissingAndUnknownChoicesFail() {
        assertThatIllegalStateException().isThrownBy(() -> ReliableEventTransportSelection.resolve(
                new MockEnvironment().withProperty("reliable-event.transport", "rocketmq"),
                List.of(), new StaticListableBeanFactory())).withMessageContaining("not installed");
        assertThatIllegalStateException().isThrownBy(() -> ReliableEventTransportSelection.resolve(
                new MockEnvironment().withProperty("reliable-event.transport", "kafka"),
                List.of(), new StaticListableBeanFactory())).withMessageContaining("not installed");
    }

    @Test
    void strictRootBindingAllowsRegisteredNamespaceButRejectsSiblingTypo() {
        var valid = new MockEnvironment()
                .withProperty("reliable-event.rocketmq.endpoints", "localhost:8081")
                .withProperty("reliable-event.claimBatchSize", "11");
        assertThatNoException().isThrownBy(() -> ReliableEventTransportSelection.resolve(
                valid, List.of(adapter("rocketmq", "rocketmq")), new StaticListableBeanFactory()));
        var invalid = new MockEnvironment()
                .withProperty("reliable-event.rocketmq.endpoints", "localhost:8081")
                .withProperty("reliable-event.claim-batch-szie", "5");
        assertThatIllegalStateException().isThrownBy(() -> ReliableEventTransportSelection.resolve(
                invalid, List.of(adapter("rocketmq", "rocketmq")), new StaticListableBeanFactory()))
                .withMessageContaining("claim-batch-szie");
    }

    @Test
    void namespaceCannotShadowPublicPropertyAndCustomIsNotAnAdapterName() {
        assertThatIllegalStateException().isThrownBy(() -> ReliableEventTransportSelection.resolve(
                new MockEnvironment(), List.of(adapter("some", "enabled")),
                new StaticListableBeanFactory())).withMessageContaining("namespace");
        assertThatIllegalStateException().isThrownBy(() -> ReliableEventTransportSelection.resolve(
                new MockEnvironment(), List.of(adapter("custom", "some")),
                new StaticListableBeanFactory())).withMessageContaining("registration");
    }

    @Test
    void duplicateEntryPointsFailEvenIfOneWouldBePrimary() {
        var beans = new StaticListableBeanFactory();
        beans.addBean("first", (EventTransport) event -> null);
        beans.addBean("second", (EventTransport) event -> null);
        assertThatIllegalStateException().isThrownBy(() -> ReliableEventTransportSelection.resolve(
                new MockEnvironment().withProperty("reliable-event.transport", "custom"),
                List.of(), beans)).withMessageContaining("Multiple");
    }
}
