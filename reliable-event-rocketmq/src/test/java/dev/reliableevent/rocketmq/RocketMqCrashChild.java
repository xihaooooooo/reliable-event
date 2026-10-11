package dev.reliableevent.rocketmq;

import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.TransportReceipt;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.retry.ExponentialBackoff;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Child JVM held at an exact process-failure boundary by the parent integration test. */
public final class RocketMqCrashChild {

    private RocketMqCrashChild() { }

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String jdbcUrl = args[1];
        String username = args[2];
        String password = args[3];
        long eventId = Long.parseLong(args[4]);
        int proxyPort = Integer.parseInt(args[5]);
        Path ready = Path.of(args[6]);

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        var tx = new org.springframework.jdbc.datasource.DataSourceTransactionManager(dataSource);
        JdbcEventPublicationWorker worker = new JdbcEventPublicationWorker(
                jdbc, tx, event -> { throw new IllegalStateException("sender not initialized"); },
                Clock.systemUTC(), 8, "crash-child-" + ProcessHandle.current().pid(), Duration.ofSeconds(15),
                new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0));
        EventCandidate candidate = worker.findDueEventCandidates(32).stream()
                .filter(item -> item.id().value() == eventId)
                .findFirst().orElseThrow(() -> new IllegalStateException("event candidate not found"));

        if (mode.equals("compete")) {
            Path gate = Path.of(args[7]);
            writeReady(ready, "scanned=true\n");
            while (!Files.exists(gate)) Thread.sleep(10);
            var claim = worker.claimCandidate(candidate).orElse(null);
            if (claim == null) {
                writeReady(ready, "scanned=true\noutcome=lost\n");
                return;
            }
            writeReady(ready, "scanned=true\noutcome=won\nowner=" + claim.leaseOwner());
            new CountDownLatch(1).await();
            return;
        }

        if (mode.equals("claim-before-send")) {
            var claim = worker.claimCandidate(candidate).orElseThrow();
            writeReady(ready, "boundary=claimed-before-send\nclaimVersion=" + claim.claimVersion()
                    + "\nleaseOwner=" + claim.leaseOwner());
            new CountDownLatch(1).await();
            return;
        }
        if (!mode.equals("confirmed-before-state-commit")) {
            throw new IllegalArgumentException("unknown crash mode: " + mode);
        }

        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints("localhost:" + proxyPort)
                .setRequestTimeout(Duration.ofSeconds(5))
                .enableSsl(false)
                .build();
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .setTopics(RocketMqTestEnvironment.TOPIC)
                .setMaxAttempts(1)
                .build();
        RocketMqEventTransport rocketSender = new RocketMqEventTransport(provider, producer, new MapEventDestinationResolver(Map.of("coupon-task-execute",
                        new RocketMqDestination(RocketMqTestEnvironment.TOPIC, RocketMqTestEnvironment.TAG))));
        EventTransport holdAfterConfirmation = (OutboundEvent event) -> {
            TransportReceipt receipt = rocketSender.send(event);
            writeReady(ready, "boundary=broker-confirmed-before-db-state-commit\nmessageId="
                    + receipt.brokerMessageId().orElse(null));
            awaitParentKill();
            return receipt;
        };
        JdbcEventPublicationWorker confirmedWorker = new JdbcEventPublicationWorker(
                jdbc, tx, holdAfterConfirmation, Clock.systemUTC(), 8,
                "confirmed-crash-child-" + ProcessHandle.current().pid(), Duration.ofSeconds(15),
                new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, () -> 0.0));
        confirmedWorker.publishCandidate(candidate);
    }

    private static void writeReady(Path path, String value) {
        try {
            Path temporary = path.resolveSibling(path.getFileName() + ".tmp-" + ProcessHandle.current().pid());
            Files.writeString(temporary, value);
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("unable to write child readiness marker", failure);
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
