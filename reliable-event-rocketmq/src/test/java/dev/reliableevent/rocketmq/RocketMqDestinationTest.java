package dev.reliableevent.rocketmq;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RocketMqDestinationTest {

    @Test
    void acceptsOrdinaryTopicsAndAnOptionalTag() {
        assertThat(new RocketMqDestination("coupon-task_topic%1", "execute"))
                .isEqualTo(new RocketMqDestination("coupon-task_topic%1", "execute"));
        assertThat(new RocketMqDestination("coupon-task-topic", null).tag()).isNull();
    }

    @Test
    void rejectsInvalidAndReservedTargets() {
        assertThatThrownBy(() -> new RocketMqDestination(" ", "tag"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic");
        assertThatThrownBy(() -> new RocketMqDestination("topic with spaces", "tag"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic");
        assertThatThrownBy(() -> new RocketMqDestination("RMQ_SYS_TRACE_TOPIC", "tag"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
        assertThatThrownBy(() -> new RocketMqDestination("topic", " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tag");
    }
}
