package dev.reliableevent.jdbc.internal.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

public final class ExponentialBackoff {

    static final Duration DEFAULT_INITIAL_DELAY = Duration.ofSeconds(1);
    static final Duration DEFAULT_MAX_DELAY = Duration.ofMinutes(5);
    static final double DEFAULT_JITTER_RATIO = 0.2;

    private final long initialDelayMillis;
    private final long maxDelayMillis;
    private final double jitterRatio;
    private final DoubleSupplier randomSource;

    public static ExponentialBackoff defaults() {
        return new ExponentialBackoff(
                DEFAULT_INITIAL_DELAY,
                DEFAULT_MAX_DELAY,
                DEFAULT_JITTER_RATIO,
                () -> ThreadLocalRandom.current().nextDouble()
        );
    }

    public ExponentialBackoff(
            Duration initialDelay,
            Duration maxDelay,
            double jitterRatio,
            DoubleSupplier randomSource
    ) {
        this.initialDelayMillis = positiveMillis(initialDelay, "initialDelay");
        this.maxDelayMillis = positiveMillis(maxDelay, "maxDelay");
        if (maxDelayMillis < initialDelayMillis) {
            throw new IllegalArgumentException("maxDelay must not be shorter than initialDelay");
        }
        if (!Double.isFinite(jitterRatio)
                || jitterRatio < 0.0
                || jitterRatio > DEFAULT_JITTER_RATIO) {
            throw new IllegalArgumentException("jitterRatio must be between 0.0 and 0.2");
        }
        this.jitterRatio = jitterRatio;
        this.randomSource = Objects.requireNonNull(randomSource, "randomSource must not be null");
    }

    public Duration nextDelay(int attemptCount) {
        if (attemptCount <= 0) {
            throw new IllegalArgumentException("attemptCount must be positive");
        }

        long baseDelayMillis = initialDelayMillis;
        for (int attempt = 1; attempt < attemptCount && baseDelayMillis < maxDelayMillis; attempt++) {
            if (baseDelayMillis > maxDelayMillis / 2) {
                baseDelayMillis = maxDelayMillis;
            } else {
                baseDelayMillis *= 2;
            }
        }

        if (jitterRatio == 0.0) {
            return Duration.ofMillis(baseDelayMillis);
        }

        double random = randomSource.getAsDouble();
        if (!Double.isFinite(random) || random < 0.0 || random >= 1.0) {
            throw new IllegalStateException("randomSource must return a value between 0.0 and 1.0");
        }

        long jitterMillis = (long) Math.floor(baseDelayMillis * jitterRatio * random);
        return Duration.ofMillis(Math.addExact(baseDelayMillis, jitterMillis));
    }

    private static long positiveMillis(Duration duration, String name) {
        Objects.requireNonNull(duration, name + " must not be null");
        long millis = duration.toMillis();
        if (millis <= 0) {
            throw new IllegalArgumentException(name + " must be at least one millisecond");
        }
        return millis;
    }
}
