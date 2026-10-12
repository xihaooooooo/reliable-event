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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.AtomicMoveNotSupportedException;
import java.time.Clock;
import java.time.Duration;

/** Independent-JVM worker used to race for the same shared MySQL Outbox row. */
public final class KafkaPublicationWorkerProcess {
    private KafkaPublicationWorkerProcess() { }

    public static void main(String[] args) {
        if (args.length != 1) throw new IllegalArgumentException("expected worker id");
        String workerId = args[0];
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
            publishReady(workerId);
            awaitBarrier(Path.of(required("KAFKA_CRASH_START_BARRIER")));
            Clock clock = Clock.systemUTC();
            JdbcPublicationStore store = new JdbcPublicationStore(context.getBean(JdbcTemplate.class),
                    context.getBean(PlatformTransactionManager.class), clock, workerId,
                    Duration.ofSeconds(12), PublicationObserver.NOOP);
            JdbcEventPublicationWorker worker = new JdbcEventPublicationWorker(store,
                    context.getBean(EventTransport.class), clock,
                    new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0),
                    PublicationObserver.NOOP, PublicationTracer.NOOP);
            int count = 0;
            for (var candidate : store.findDueEventCandidates(20)) {
                var claim = store.claimCandidate(candidate);
                if (claim.isPresent() && worker.publishClaimedEvent(claim.get())) count++;
            }
            String result = "RESULT " + workerId + " " + count + System.lineSeparator();
            System.out.print(result);
            System.out.flush();
            publishResult(result);
        }
    }

    private static void publishReady(String workerId) { atomicWrite(Path.of(required("KAFKA_CRASH_READY_FILE")),
            "READY " + workerId + System.lineSeparator()); }
    private static void publishResult(String result) { atomicWrite(Path.of(required("KAFKA_CRASH_RESULT_FILE")), result); }
    private static void awaitBarrier(Path path) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!Files.exists(path) && System.nanoTime() < deadline) {
            try { Thread.sleep(20); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
        }
        if (!Files.exists(path)) throw new IllegalStateException("worker start barrier timed out");
    }
    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalStateException("missing environment: " + name);
        return value;
    }
    private static void atomicWrite(Path target, String contents) {
        Path pending = target.resolveSibling(target.getFileName() + ".pending");
        try {
            Files.writeString(pending, contents, StandardCharsets.UTF_8);
            try { Files.move(pending, target, StandardCopyOption.ATOMIC_MOVE); }
            catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(pending, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (java.io.IOException failure) { throw new IllegalStateException("Unable to write worker marker", failure); }
    }
}
