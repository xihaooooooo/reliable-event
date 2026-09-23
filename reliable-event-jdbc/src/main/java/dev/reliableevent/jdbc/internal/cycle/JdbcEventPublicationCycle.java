package dev.reliableevent.jdbc.internal.cycle;

import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import java.util.Objects;

public final class JdbcEventPublicationCycle {

    private final JdbcExpiredLeaseRecovery recovery;
    private final JdbcEventPublicationWorker worker;

    public JdbcEventPublicationCycle(
            JdbcExpiredLeaseRecovery recovery,
            JdbcEventPublicationWorker worker
    ) {
        this.recovery = Objects.requireNonNull(recovery, "recovery must not be null");
        this.worker = Objects.requireNonNull(worker, "worker must not be null");
    }

    public PublicationCycleResult runOnce() {
        int recoveredCount = recovery.recoverExpiredLeases();
        int publishedCount = worker.publishDueEvents();
        return new PublicationCycleResult(recoveredCount, publishedCount);
    }
}
