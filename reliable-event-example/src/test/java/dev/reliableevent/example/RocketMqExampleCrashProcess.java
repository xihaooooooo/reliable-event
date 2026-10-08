package dev.reliableevent.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliableevent.internal.model.StoredEvent;
import dev.reliableevent.internal.publication.EventSender;
import dev.reliableevent.internal.publication.SendReceipt;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import dev.reliableevent.rocketmq.MapEventDestinationResolver;
import dev.reliableevent.rocketmq.RocketMqDestination;
import dev.reliableevent.rocketmq.RocketMqEventSender;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Separate JVM held after a real RocketMQ confirmation for the end-to-end crash test. */
public final class RocketMqExampleCrashProcess {

    private RocketMqExampleCrashProcess() { }

    public static void main(String[] args) throws Exception {
        String jdbcUrl = args[0];
        String username = args[1];
        String password = args[2];
        long eventId = Long.parseLong(args[3]);
        String endpoint = args[4];
        String topic = args[5];
        Path readyMarker = Path.of(args[6]);

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DataSourceTransactionManager transactions = new DataSourceTransactionManager(dataSource);
        JdbcEventPublicationWorker uninitializedWorker = new JdbcEventPublicationWorker(
                jdbc, transactions, event -> { throw new IllegalStateException("sender not initialized"); },
                Clock.systemUTC(), 8, "rocket-example-child-" + ProcessHandle.current().pid(),
                Duration.ofSeconds(15),
                new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0));
        EventCandidate candidate = uninitializedWorker.findDueEventCandidates(32).stream()
                .filter(item -> item.id().value() == eventId)
                .findFirst().orElseThrow(() -> new IllegalStateException("event candidate not found"));

        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoint)
                .setRequestTimeout(Duration.ofSeconds(5))
                .enableSsl(false)
                .build();
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .setTopics(topic)
                .setMaxAttempts(1)
                .build();
        RocketMqEventSender sender = new RocketMqEventSender(provider, producer,
                new MapEventDestinationResolver(Map.of(OrderService.EVENT_TYPE,
                        new RocketMqDestination(topic, "created"))), new ObjectMapper());
        EventSender holdAfterRealConfirmation = (StoredEvent event) -> {
            SendReceipt receipt = sender.send(event);
            writeReady(readyMarker, "boundary=real-broker-confirmed-before-state-commit\n" +
                    "eventId=" + event.id().value() + "\nmessageId=" + receipt.messageId());
            awaitParentKill();
            return receipt;
        };
        JdbcEventPublicationWorker worker = new JdbcEventPublicationWorker(
                jdbc, transactions, holdAfterRealConfirmation, Clock.systemUTC(), 8,
                "rocket-example-child-" + ProcessHandle.current().pid(), Duration.ofSeconds(15),
                new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0));
        worker.publishCandidate(candidate);
    }

    private static void writeReady(Path marker, String contents) {
        try {
            Path temporary = marker.resolveSibling(marker.getFileName() + ".tmp-" + ProcessHandle.current().pid());
            Files.writeString(temporary, contents);
            Files.move(temporary, marker, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("could not write child readiness marker", failure);
        }
    }

    private static void awaitParentKill() {
        try {
            new CountDownLatch(1).await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("child unexpectedly interrupted", interrupted);
        }
    }
}
