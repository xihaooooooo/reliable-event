package dev.reliableevent.jdbc.fault;

import dev.reliableevent.EventId;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import dev.reliableevent.jdbc.internal.publication.SendReceipt;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class PublicationCrashProcess {

    private PublicationCrashProcess() {
    }

    public static void main(String[] args) {
        try {
            run(args, System.getenv(), new PrintWriter(System.out, true, StandardCharsets.UTF_8));
        } catch (RuntimeException | IOException exception) {
            exception.printStackTrace(System.err);
            System.err.flush();
            System.exit(2);
        }
    }

    static void run(
            String[] args,
            Map<String, String> environment,
            PrintWriter output
    ) throws IOException {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("expected crash mode and event id");
        }
        PublicationCrashMode mode = PublicationCrashMode.parse(args[0]);
        long eventId = positiveEventId(args[1]);
        PublicationProcessConfig config = PublicationProcessConfig.fromEnvironment(environment);

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(config.jdbcUrl());
        dataSource.setUsername(config.username());
        dataSource.setPassword(config.password());

        JdbcOutboxRepository repository = new JdbcOutboxRepository(
                new JdbcTemplate(dataSource)
        );
        EventCandidate candidate = repository.findDueEventCandidates(
                        config.claimNow(),
                        100
                ).stream()
                .filter(current -> current.id().equals(new EventId(eventId)))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "event is not an available publication candidate: " + eventId
                ));

        TransactionTemplate transaction = new TransactionTemplate(
                new DataSourceTransactionManager(dataSource)
        );
        ClaimedEvent claimedEvent = transaction.execute(status -> repository.claim(
                candidate,
                config.claimNow(),
                config.workerId(),
                config.leaseDuration()
        ).orElseThrow(() -> new IllegalStateException(
                "event claim failed: " + eventId
        )));
        if (claimedEvent == null) {
            throw new IllegalStateException("event claim returned no result: " + eventId);
        }

        PublicationReadySignal signal;
        if (mode == PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND) {
            signal = PublicationReadySignal.claimed(
                    eventId,
                    claimedEvent.claimVersion(),
                    config.workerId()
            );
        } else {
            SendReceipt receipt = new JdbcDeliveryProbeSender(
                    dataSource,
                    config.workerId()
            ).send(claimedEvent.event());
            signal = PublicationReadySignal.delivered(
                    eventId,
                    claimedEvent.claimVersion(),
                    config.workerId(),
                    receipt.messageId()
            );
        }

        output.println(signal.format());
        output.flush();
        waitForParentTermination();
    }

    private static long positiveEventId(String value) {
        try {
            long eventId = Long.parseLong(value);
            if (eventId <= 0) {
                throw new IllegalArgumentException("event id must be positive");
            }
            return eventId;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("event id must be a number", exception);
        }
    }

    private static void waitForParentTermination() throws IOException {
        if (System.in.read() == -1) {
            throw new IllegalStateException("parent input closed before process termination");
        }
        throw new IllegalStateException("crash process must be terminated at the fault point");
    }
}
