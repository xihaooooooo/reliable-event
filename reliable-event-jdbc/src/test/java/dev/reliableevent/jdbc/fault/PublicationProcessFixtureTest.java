package dev.reliableevent.jdbc.fault;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublicationProcessFixtureTest {

    @Test
    void parsesSupportedCrashModes() {
        assertThat(PublicationCrashMode.parse("AFTER_CLAIM_BEFORE_SEND"))
                .isEqualTo(PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND);
        assertThat(PublicationCrashMode.parse("AFTER_DELIVERY_BEFORE_STATE_UPDATE"))
                .isEqualTo(PublicationCrashMode.AFTER_DELIVERY_BEFORE_STATE_UPDATE);

        assertThatThrownBy(() -> PublicationCrashMode.parse("unknown"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown crash mode");
    }

    @Test
    void parsesAndFormatsClaimedReadySignal() {
        PublicationReadySignal signal = PublicationReadySignal.parse(
                "READY CLAIMED 41 1 crash-worker"
        );

        assertThat(signal.mode())
                .isEqualTo(PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND);
        assertThat(signal.eventId()).isEqualTo(41L);
        assertThat(signal.claimVersion()).isOne();
        assertThat(signal.workerId()).isEqualTo("crash-worker");
        assertThat(signal.messageId()).isNull();
        assertThat(signal.format()).isEqualTo("READY CLAIMED 41 1 crash-worker");
    }

    @Test
    void parsesAndFormatsDeliveredReadySignal() {
        PublicationReadySignal signal = PublicationReadySignal.parse(
                "READY DELIVERED 42 1 crash-worker message-1"
        );

        assertThat(signal.mode())
                .isEqualTo(PublicationCrashMode.AFTER_DELIVERY_BEFORE_STATE_UPDATE);
        assertThat(signal.messageId()).isEqualTo("message-1");
        assertThat(signal.format())
                .isEqualTo("READY DELIVERED 42 1 crash-worker message-1");
    }

    @Test
    void rejectsMalformedReadySignals() {
        assertThatThrownBy(() -> PublicationReadySignal.parse("CLAIMED 41 1 worker"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PublicationReadySignal.parse("READY CLAIMED x 1 worker"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("eventId");
        assertThatThrownBy(() -> PublicationReadySignal.parse(
                "READY DELIVERED 41 1 worker"
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void processConfigurationRoundTripsThroughEnvironment() {
        PublicationProcessConfig expected = config();
        Map<String, String> environment = new HashMap<>();

        expected.applyTo(environment);

        assertThat(PublicationProcessConfig.fromEnvironment(environment))
                .isEqualTo(expected);
    }

    @Test
    void processConfigurationRejectsMissingAndInvalidValues() {
        Map<String, String> missing = new HashMap<>();
        assertThatThrownBy(() -> PublicationProcessConfig.fromEnvironment(missing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PublicationProcessConfig.JDBC_URL_ENV);

        Map<String, String> invalidLease = environmentFor(config());
        invalidLease.put(PublicationProcessConfig.LEASE_MILLIS_ENV, "not-a-number");
        assertThatThrownBy(() -> PublicationProcessConfig.fromEnvironment(invalidLease))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PublicationProcessConfig.LEASE_MILLIS_ENV);

        Map<String, String> invalidInstant = environmentFor(config());
        invalidInstant.put(PublicationProcessConfig.CLAIM_NOW_ENV, "not-an-instant");
        assertThatThrownBy(() -> PublicationProcessConfig.fromEnvironment(invalidInstant))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PublicationProcessConfig.CLAIM_NOW_ENV);
    }

    @Test
    void processCommandKeepsEveryArgumentSeparate() {
        List<String> command = PublicationProcessFixture.commandFor(
                Path.of("C:\\Java Runtime\\bin\\java.exe"),
                "first path;second path",
                PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND,
                43L
        );

        assertThat(command).containsExactly(
                "C:\\Java Runtime\\bin\\java.exe",
                "-cp",
                "first path;second path",
                PublicationCrashProcess.class.getName(),
                "AFTER_CLAIM_BEFORE_SEND",
                "43"
        );
    }

    private PublicationProcessConfig config() {
        return new PublicationProcessConfig(
                "jdbc:mysql://localhost:3306/test",
                "test",
                "secret",
                "crash-worker",
                Duration.ofSeconds(30),
                Instant.parse("2026-09-23T00:00:00Z")
        );
    }

    private Map<String, String> environmentFor(PublicationProcessConfig config) {
        Map<String, String> environment = new HashMap<>();
        config.applyTo(environment);
        return environment;
    }
}
