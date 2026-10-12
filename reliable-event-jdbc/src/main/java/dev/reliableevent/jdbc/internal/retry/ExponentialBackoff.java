package dev.reliableevent.jdbc.internal.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class ExponentialBackoff {

    public static final double MAX_JITTER_RATIO = 0.2;

    private final long initialDelayMillis;
    private final long maxBaseDelayMillis;
    private final double jitterRatio;

    public ExponentialBackoff(
            Duration initialDelay,
            Duration maxBaseDelay,
            double jitterRatio
    ) {
        this.initialDelayMillis = positiveMillis(initialDelay, "initialDelay");
        this.maxBaseDelayMillis = positiveMillis(maxBaseDelay, "maxBaseDelay");
        if (maxBaseDelayMillis < initialDelayMillis) {
            throw new IllegalArgumentException("maxBaseDelay must not be shorter than initialDelay");
        }
        if (!Double.isFinite(jitterRatio)
                || jitterRatio < 0.0
                || jitterRatio > MAX_JITTER_RATIO) {
            throw new IllegalArgumentException("jitterRatio must be between 0.0 and 0.2");
        }
        this.jitterRatio = jitterRatio;
    }

    public Duration nextDelay(int attemptCount) {
        if (attemptCount <= 0) {
            throw new IllegalArgumentException("attemptCount must be positive");
        }

        long baseDelayMillis = initialDelayMillis;
        for (int attempt = 1; attempt < attemptCount && baseDelayMillis < maxBaseDelayMillis; attempt++) {
            if (baseDelayMillis > maxBaseDelayMillis / 2) {
                baseDelayMillis = maxBaseDelayMillis;
            } else {
                baseDelayMillis *= 2;
            }
        }

        if (jitterRatio == 0.0) {
            return Duration.ofMillis(baseDelayMillis);
        }

        double random = ThreadLocalRandom.current().nextDouble();

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
