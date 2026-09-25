package dev.reliableevent.autoconfigure;

import dev.reliableevent.EventId;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReliableEventSchedulerTest {

    @Test
    void queuedCandidateIsNotClaimedAndLocalCapacityBoundsDispatch() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate first = new EventCandidate(new EventId(1), 0);
        EventCandidate second = new EventCandidate(new EventId(2), 0);
        EventCandidate third = new EventCandidate(new EventId(3), 0);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        ClaimedEvent firstClaim = mock(ClaimedEvent.class);
        ClaimedEvent secondClaim = mock(ClaimedEvent.class);
        when(worker.findDueEventCandidates(anyInt())).thenReturn(List.of(first, second, third));
        when(worker.claimCandidate(first)).thenReturn(Optional.of(firstClaim));
        when(worker.claimCandidate(second)).thenReturn(Optional.of(secondClaim));
        when(worker.publishClaimedEvent(firstClaim)).thenAnswer(invocation -> {
            firstStarted.countDown();
            if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("First sender was not released");
            }
            return true;
        });
        when(worker.publishClaimedEvent(secondClaim)).thenAnswer(invocation -> {
            secondStarted.countDown();
            return true;
        });

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, worker, 3, 1, 1, Duration.ofSeconds(1)
        );
        try {
            assertThat(scheduler.dispatchOnce()).isEqualTo(new AsyncPublicationCycleResult(0, 2));
            assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scheduler.dispatchOnce().submittedCount()).isZero();
            verify(worker, never()).claimCandidate(second);
            verify(worker, never()).claimCandidate(third);

            releaseFirst.countDown();
            assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseFirst.countDown();
            scheduler.stop();
        }
    }

    @Test
    void scanFailureDoesNotCancelLaterScheduledRounds() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        CountDownLatch nextRound = new CountDownLatch(1);
        when(recovery.recoverExpiredLeases())
                .thenThrow(new IllegalStateException("database unavailable"))
                .thenAnswer(invocation -> {
                    nextRound.countDown();
                    return 0;
                });
        when(worker.findDueEventCandidates(anyInt())).thenReturn(List.of());

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, worker, 1, 1, 0, Duration.ofMillis(10)
        );
        try {
            scheduler.start();
            assertThat(nextRound.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scheduler.isRunning()).isTrue();
        } finally {
            scheduler.stop();
        }
    }
}
