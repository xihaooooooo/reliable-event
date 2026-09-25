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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReliableEventSchedulerTest {

    @Test
    void stopCancelsQueuedCandidatesAndWaitsForTheWholeActiveTask() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate first = new EventCandidate(new EventId(1), 0);
        EventCandidate queued = new EventCandidate(new EventId(2), 0);
        ClaimedEvent claim = mock(ClaimedEvent.class);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch callback = new CountDownLatch(1);
        AtomicBoolean publicationCompleted = new AtomicBoolean();
        AtomicBoolean callbackAfterPublication = new AtomicBoolean();
        when(worker.findDueEventCandidates(anyInt())).thenReturn(List.of(first, queued));
        when(worker.claimCandidate(first)).thenReturn(Optional.of(claim));
        when(worker.publishClaimedEvent(claim)).thenAnswer(invocation -> {
            publishing.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Publication was not released");
            }
            publicationCompleted.set(true);
            return true;
        });

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, worker, 2, 1, 1, Duration.ofSeconds(1), Duration.ofSeconds(3));
        try {
            assertThat(scheduler.dispatchOnce().submittedCount()).isEqualTo(2);
            assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
            scheduler.stop(() -> {
                callbackAfterPublication.set(publicationCompleted.get());
                callback.countDown();
            });
            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(scheduler.outstandingCount()).isOne());
            assertThat(callback.getCount()).isOne();
            assertThat(scheduler.dispatchOnce().submittedCount()).isZero();
            verify(worker, never()).claimCandidate(queued);

            release.countDown();
            assertThat(callback.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(callbackAfterPublication).isTrue();
            assertThat(scheduler.outstandingCount()).isZero();
            assertThatThrownBy(scheduler::start).isInstanceOf(IllegalStateException.class);
        } finally {
            release.countDown();
            scheduler.stop();
        }
    }

    @Test
    void timeoutSkipsSendingWhenAClaimFinishesAfterShutdown() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate candidate = new EventCandidate(new EventId(3), 0);
        ClaimedEvent claim = mock(ClaimedEvent.class);
        CountDownLatch claimStarted = new CountDownLatch(1);
        CountDownLatch releaseClaim = new CountDownLatch(1);
        CountDownLatch callback = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();
        when(worker.findDueEventCandidates(anyInt())).thenReturn(List.of(candidate));
        when(worker.claimCandidate(candidate)).thenAnswer(invocation -> {
            claimStarted.countDown();
            while (releaseClaim.getCount() != 0) {
                try {
                    releaseClaim.await(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    // Exercise a database call that does not respond to interruption.
                }
            }
            return Optional.of(claim);
        });

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, worker, 1, 1, 0, Duration.ofSeconds(1), Duration.ofMillis(100));
        try {
            assertThat(scheduler.dispatchOnce().submittedCount()).isOne();
            assertThat(claimStarted.await(5, TimeUnit.SECONDS)).isTrue();
            scheduler.stop(() -> {
                callbackCount.incrementAndGet();
                callback.countDown();
            });
            assertThat(callback.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scheduler.outstandingCount()).isOne();
            scheduler.stop(callbackCount::incrementAndGet);
            assertThat(callbackCount).hasValue(2);

            releaseClaim.countDown();
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(scheduler.outstandingCount()).isZero());
            verify(worker, never()).publishClaimedEvent(claim);
        } finally {
            releaseClaim.countDown();
            scheduler.stop();
        }
    }

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
