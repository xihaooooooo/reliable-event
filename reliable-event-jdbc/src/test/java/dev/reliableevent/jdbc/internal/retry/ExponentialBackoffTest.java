package dev.reliableevent.jdbc.internal.retry;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExponentialBackoffTest {

    @Test
    void calculatesExponentialDelayWithDeterministicJitter() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                0.2,
                () -> 0.5
        );

        assertThat(backoff.nextDelay(1)).isEqualTo(Duration.ofMillis(1_100));
        assertThat(backoff.nextDelay(2)).isEqualTo(Duration.ofMillis(2_200));
        assertThat(backoff.nextDelay(3)).isEqualTo(Duration.ofMillis(4_400));
    }

    @Test
    void capsBaseDelayBeforeAddingJitter() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofSeconds(5),
                0.2,
                () -> 0.5
        );

        assertThat(backoff.nextDelay(4)).isEqualTo(Duration.ofMillis(5_500));
        assertThat(backoff.nextDelay(Integer.MAX_VALUE)).isEqualTo(Duration.ofMillis(5_500));
    }

    @Test
    void zeroJitterDoesNotReadRandomSource() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofMillis(1_001),
                Duration.ofSeconds(5),
                0.0,
                () -> Double.NaN
        );

        assertThat(backoff.nextDelay(1)).isEqualTo(Duration.ofMillis(1_001));
    }

    @Test
    void rejectsInvalidConfigurationAndInputs() {
        assertThatThrownBy(() -> new ExponentialBackoff(
                Duration.ZERO,
                Duration.ofSeconds(1),
                0.2,
                () -> 0.0
        )).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new ExponentialBackoff(
                Duration.ofSeconds(2),
                Duration.ofSeconds(1),
                0.2,
                () -> 0.0
        )).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                0.21,
                () -> 0.0
        )).isInstanceOf(IllegalArgumentException.class);

        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                0.2,
                () -> 1.0
        );
        assertThatThrownBy(() -> backoff.nextDelay(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> backoff.nextDelay(1))
                .isInstanceOf(IllegalStateException.class);
    }
}
