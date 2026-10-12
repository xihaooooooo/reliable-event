package dev.reliableevent.kafka.example;

import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.persistence.JdbcPublicationStore;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.tracing.PublicationTracer;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/** Test-only child JVM that blocks at a deterministic boundary around the real Kafka producer. */
public final class KafkaPublicationCrashProcess {
    private KafkaPublicationCrashProcess() { }

    public static void main(String[] args) {
        if (args.length != 2) throw new IllegalArgumentException("expected fault mode and worker id");
        KafkaCrashMode mode = KafkaCrashMode.valueOf(args[0]);
        String workerId = args[1];
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(KafkaExampleApplication.class)
                .properties("spring.main.web-application-type=none")
                .run("--spring.datasource.url=" + required("KAFKA_CRASH_JDBC_URL"),
                        "--spring.datasource.username=" + required("KAFKA_CRASH_JDBC_USER"),
                        "--spring.datasource.password=" + required("KAFKA_CRASH_JDBC_PASSWORD"),
                        "--reliable-event.kafka.bootstrap-servers=" + required("KAFKA_CRASH_BOOTSTRAP"),
                        "--example.kafka.topic=" + required("KAFKA_CRASH_TOPIC"),
                        "--example.kafka.create-topic=false",
                        "--example.consumer.enabled=false",
                        "--reliable-event.scheduling-enabled=false")) {
            JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
            EventTransport actualProducer = context.getBean(EventTransport.class);
            EventTransport gatedProducer = event -> {
                if (mode == KafkaCrashMode.AFTER_CLAIM_BEFORE_SEND) {
                    ready(mode, event.id().value(), workerId);
                    waitUntilKilled();
                }
                var receipt = actualProducer.send(event);
                ready(mode, event.id().value(), workerId);
                waitUntilKilled();
                return receipt;
            };
            Clock clock = Clock.systemUTC();
            JdbcPublicationStore store = new JdbcPublicationStore(jdbc,
                    context.getBean(PlatformTransactionManager.class), clock, workerId,
                    Duration.ofSeconds(12), PublicationObserver.NOOP);
            JdbcEventPublicationWorker worker = new JdbcEventPublicationWorker(store, gatedProducer,
                    clock, new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0),
                    PublicationObserver.NOOP, PublicationTracer.NOOP);
            var candidate = store.findDueEventCandidates(20).stream().findFirst().orElseThrow();
            var claim = store.claimCandidate(candidate).orElseThrow();
            worker.publishClaimedEvent(claim);
            throw new IllegalStateException("fault gate returned unexpectedly");
        }
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalStateException("missing environment: " + name);
        return value;
    }

    private static void ready(KafkaCrashMode mode, long eventId, String workerId) {
        PrintWriter out = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
        String line = String.format("READY %s %d %s%n", mode.name(), eventId, workerId);
        out.print(line);
        out.flush();
        Path readyFile = Path.of(required("KAFKA_CRASH_READY_FILE"));
        Path pending = readyFile.resolveSibling(readyFile.getFileName() + ".pending");
        try {
            Files.writeString(pending, line, StandardCharsets.UTF_8);
            try { Files.move(pending, readyFile, StandardCopyOption.ATOMIC_MOVE); }
            catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(pending, readyFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Unable to publish child readiness marker", failure);
        }
    }

    private static void waitUntilKilled() {
        while (true) {
            try { TimeUnit.SECONDS.sleep(30); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
        }
    }
}
