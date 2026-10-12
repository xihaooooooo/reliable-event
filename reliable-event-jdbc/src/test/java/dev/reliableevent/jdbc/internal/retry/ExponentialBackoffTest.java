package dev.reliableevent.jdbc.internal.retry;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExponentialBackoffTest {

    @Test
    void calculatesExponentialDelayWithoutJitter() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(1), Duration.ofMinutes(5), 0.0);

        assertThat(backoff.nextDelay(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(backoff.nextDelay(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(backoff.nextDelay(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(backoff.nextDelay(9)).isEqualTo(Duration.ofSeconds(256));
        assertThat(backoff.nextDelay(10)).isEqualTo(Duration.ofMinutes(5));
        assertThat(backoff.nextDelay(Integer.MAX_VALUE)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void addsJitterWithinTheConfiguredRange() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(1), Duration.ofMinutes(5), 0.1);

        assertThat(backoff.nextDelay(1).toMillis()).isBetween(1_000L, 1_099L);
        assertThat(backoff.nextDelay(2).toMillis()).isBetween(2_000L, 2_199L);
        assertThat(backoff.nextDelay(3).toMillis()).isBetween(4_000L, 4_399L);
    }

    @Test
    void capsBaseDelayBeforeAddingJitter() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(1), Duration.ofSeconds(5), 0.2);

        assertThat(backoff.nextDelay(4).toMillis()).isBetween(5_000L, 5_999L);
        assertThat(backoff.nextDelay(Integer.MAX_VALUE).toMillis()).isBetween(5_000L, 5_999L);
    }

    @Test
    void truncatesSubMillisecondJitter() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofMillis(1), Duration.ofMillis(1), 0.2);

        assertThat(backoff.nextDelay(1)).isEqualTo(Duration.ofMillis(1));
    }

    @Test
    void capsBeforeDoublingCanOverflow() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofMillis(Long.MAX_VALUE / 2 + 1), Duration.ofMillis(Long.MAX_VALUE), 0.0);

        assertThat(backoff.nextDelay(2)).isEqualTo(Duration.ofMillis(Long.MAX_VALUE));
    }

    @Test
    void rejectsInvalidDelaysAndAttemptCounts() {
        assertThatThrownBy(() -> new ExponentialBackoff(
                Duration.ZERO, Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExponentialBackoff(
                Duration.ofNanos(999_999), Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExponentialBackoff(
                Duration.ofSeconds(2), Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExponentialBackoff(
                null, Duration.ofSeconds(1), 0.0))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ExponentialBackoff(
                Duration.ofSeconds(1), null, 0.0))
                .isInstanceOf(NullPointerException.class);

        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(1), Duration.ofSeconds(2), 0.0);
        assertThatThrownBy(() -> backoff.nextDelay(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> backoff.nextDelay(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvalidJitterRatios() {
        for (double ratio : new double[]{-0.01, 0.21, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThatThrownBy(() -> new ExponentialBackoff(
                    Duration.ofSeconds(1), Duration.ofSeconds(2), ratio))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("jitterRatio");
        }
    }
}
