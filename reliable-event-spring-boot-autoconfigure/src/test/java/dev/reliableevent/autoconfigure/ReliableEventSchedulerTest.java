package dev.reliableevent.autoconfigure;

import dev.reliableevent.EventId;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.persistence.JdbcPublicationStore;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import dev.reliableevent.jdbc.internal.observation.PublicationObserver;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReliableEventSchedulerTest {

    @Test
    void stopCancelsQueuedCandidatesAndWaitsForTheWholeActiveTask() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate first = new EventCandidate(new EventId(1), 0);
        EventCandidate queued = new EventCandidate(new EventId(2), 0);
        ClaimedEvent claim = mock(ClaimedEvent.class);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch callback = new CountDownLatch(1);
        AtomicBoolean publicationCompleted = new AtomicBoolean();
        AtomicBoolean callbackAfterPublication = new AtomicBoolean();
        when(store.findDueEventCandidates(anyInt())).thenReturn(List.of(first, queued));
        when(store.claimCandidate(first)).thenReturn(Optional.of(claim));
        when(worker.publishClaimedEvent(claim)).thenAnswer(invocation -> {
            publishing.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Publication was not released");
            }
            publicationCompleted.set(true);
            return true;
        });

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, store, worker, 2, 1, 1, Duration.ofSeconds(1), Duration.ofSeconds(3));
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
            verify(store, never()).claimCandidate(queued);

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
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate candidate = new EventCandidate(new EventId(3), 0);
        ClaimedEvent claim = mock(ClaimedEvent.class);
        CountDownLatch claimStarted = new CountDownLatch(1);
        CountDownLatch releaseClaim = new CountDownLatch(1);
        CountDownLatch callback = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();
        when(store.findDueEventCandidates(anyInt())).thenReturn(List.of(candidate));
        when(store.claimCandidate(candidate)).thenAnswer(invocation -> {
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
                recovery, store, worker, 1, 1, 0, Duration.ofSeconds(1), Duration.ofMillis(100));
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
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate first = new EventCandidate(new EventId(1), 0);
        EventCandidate second = new EventCandidate(new EventId(2), 0);
        EventCandidate third = new EventCandidate(new EventId(3), 0);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        ClaimedEvent firstClaim = mock(ClaimedEvent.class);
        ClaimedEvent secondClaim = mock(ClaimedEvent.class);
        when(store.findDueEventCandidates(anyInt())).thenReturn(List.of(first, second, third));
        when(store.claimCandidate(first)).thenReturn(Optional.of(firstClaim));
        when(store.claimCandidate(second)).thenReturn(Optional.of(secondClaim));
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
                recovery, store, worker, 3, 1, 1, Duration.ofSeconds(1)
        );
        try {
            assertThat(scheduler.dispatchOnce()).isEqualTo(new AsyncPublicationCycleResult(0, 2));
            assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scheduler.dispatchOnce().submittedCount()).isZero();
            verify(store, never()).claimCandidate(second);
            verify(store, never()).claimCandidate(third);

            releaseFirst.countDown();
            assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseFirst.countDown();
            scheduler.stop();
        }
    }

    @Test
    void recoveryFailureDoesNotCancelLaterRecoveryOrScanning() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        CountDownLatch nextRound = new CountDownLatch(1);
        CountDownLatch scanAfterRecovery = new CountDownLatch(1);
        when(recovery.recoverExpiredLeases())
                .thenThrow(new IllegalStateException("database unavailable"))
                .thenAnswer(invocation -> {
                    nextRound.countDown();
                    return 0;
                });
        when(store.findDueEventCandidates(anyInt())).thenAnswer(invocation -> {
            scanAfterRecovery.countDown();
            return List.of();
        });
        PublicationObserver observer = mock(PublicationObserver.class);

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, store, worker, 1, 1, 0, Duration.ofMillis(10), Duration.ofSeconds(2),
                false, Duration.ofMillis(10), observer
        );
        try {
            scheduler.start();
            assertThat(nextRound.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scanAfterRecovery.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scheduler.isRunning()).isTrue();
            verify(observer).schedulerCycleFailed(true, "lease_recovery");
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void manualDispatchOnlyScansAndDoesNotInvokeRecovery() {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, store, worker, 1, 1, 0, Duration.ofMillis(10)
        );
        try {
            assertThat(scheduler.dispatchOnce()).isEqualTo(new AsyncPublicationCycleResult(0, 0));
            verify(recovery, never()).recoverExpiredLeases();
            verify(store).findDueEventCandidates(anyInt());
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void adaptivePollingScansAgainWhenBusyAndReturnsToIdleDelay() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate candidate = new EventCandidate(new EventId(10), 0);
        List<Long> scanTimes = new CopyOnWriteArrayList<>();
        CountDownLatch threeScans = new CountDownLatch(3);
        when(store.findDueEventCandidates(anyInt())).thenAnswer(invocation -> {
            scanTimes.add(System.nanoTime());
            threeScans.countDown();
            return scanTimes.size() == 1 ? List.of(candidate) : List.of();
        });
        when(store.claimCandidate(candidate)).thenReturn(Optional.empty());

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, store, worker, 1, 1, 0, Duration.ofMillis(600), Duration.ofSeconds(2),
                true, Duration.ofMillis(50));
        try {
            scheduler.start();
            assertThat(threeScans.await(3, TimeUnit.SECONDS)).isTrue();
            long busyDelay = TimeUnit.NANOSECONDS.toMillis(scanTimes.get(1) - scanTimes.get(0));
            long idleDelay = TimeUnit.NANOSECONDS.toMillis(scanTimes.get(2) - scanTimes.get(1));
            assertThat(busyDelay).isLessThan(450);
            assertThat(idleDelay).isGreaterThanOrEqualTo(500);
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void adaptivePollingUsesRateLimitWhileWorkIsInFlight() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate candidate = new EventCandidate(new EventId(11), 0);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch threeScans = new CountDownLatch(3);
        List<Long> scanTimes = new CopyOnWriteArrayList<>();
        org.mockito.Mockito.doAnswer(invocation -> {
            scanTimes.add(System.nanoTime());
            threeScans.countDown();
            return null;
        }).when(recovery).refreshSnapshotAutomatically();
        when(store.findDueEventCandidates(anyInt())).thenReturn(List.of(candidate));
        when(store.claimCandidate(candidate)).thenAnswer(invocation -> {
            release.await(3, TimeUnit.SECONDS);
            return Optional.empty();
        });

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, store, worker, 1, 1, 0, Duration.ofSeconds(1), Duration.ofSeconds(2),
                true, Duration.ofMillis(100));
        try {
            scheduler.start();
            assertThat(threeScans.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(TimeUnit.NANOSECONDS.toMillis(scanTimes.get(1) - scanTimes.get(0)))
                    .isGreaterThanOrEqualTo(90);
            assertThat(TimeUnit.NANOSECONDS.toMillis(scanTimes.get(2) - scanTimes.get(1)))
                    .isGreaterThanOrEqualTo(90);
        } finally {
            release.countDown();
            scheduler.stop();
        }
    }

    @Test
    void adaptivePollingRefillsSoonAfterCapacityIsReleasedDuringAScan() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate candidate = new EventCandidate(new EventId(12), 0);
        CountDownLatch claimStarted = new CountDownLatch(1);
        CountDownLatch releaseClaim = new CountDownLatch(1);
        CountDownLatch secondSnapshot = new CountDownLatch(1);
        CountDownLatch finishSecondSnapshot = new CountDownLatch(1);
        CountDownLatch thirdScan = new CountDownLatch(1);
        AtomicInteger snapshots = new AtomicInteger();
        org.mockito.Mockito.doAnswer(invocation -> {
            int round = snapshots.incrementAndGet();
            if (round == 2) {
                secondSnapshot.countDown();
                finishSecondSnapshot.await(3, TimeUnit.SECONDS);
            }
            if (round == 3) thirdScan.countDown();
            return null;
        }).when(recovery).refreshSnapshotAutomatically();
        when(store.findDueEventCandidates(anyInt())).thenReturn(List.of(candidate));
        when(store.claimCandidate(candidate)).thenAnswer(invocation -> {
            claimStarted.countDown();
            releaseClaim.await(3, TimeUnit.SECONDS);
            return Optional.empty();
        });

        ReliableEventScheduler scheduler = new ReliableEventScheduler(
                recovery, store, worker, 1, 1, 0, Duration.ofSeconds(1), Duration.ofSeconds(2),
                true, Duration.ofMillis(50));
        try {
            scheduler.start();
            assertThat(claimStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(secondSnapshot.await(2, TimeUnit.SECONDS)).isTrue();
            releaseClaim.countDown();
            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(scheduler.outstandingCount()).isZero());
            finishSecondSnapshot.countDown();
            assertThat(thirdScan.await(500, TimeUnit.MILLISECONDS)).isTrue();
        } finally {
            releaseClaim.countDown();
            finishSecondSnapshot.countDown();
            scheduler.stop();
        }
    }

    @Test
    void blockedRecoveryDoesNotDelayNewEventsOrQueueRecoveryAndStopUsesOneDeadline() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcPublicationStore store = mock(JdbcPublicationStore.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        EventCandidate candidate = new EventCandidate(new EventId(99), 0);
        ClaimedEvent claimed = mock(ClaimedEvent.class);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        CountDownLatch releaseRecovery = new CountDownLatch(1);
        CountDownLatch publicationStarted = new CountDownLatch(1);
        CountDownLatch releasePublication = new CountDownLatch(1);
        AtomicBoolean eventReady = new AtomicBoolean();
        AtomicInteger scans = new AtomicInteger();
        AtomicInteger recoveries = new AtomicInteger();
        when(recovery.recoverExpiredLeases()).thenAnswer(invocation -> {
            recoveries.incrementAndGet();
            recoveryStarted.countDown();
            awaitIgnoringInterrupt(releaseRecovery);
            return 0;
        });
        when(store.findDueEventCandidates(anyInt())).thenAnswer(invocation -> {
            scans.incrementAndGet();
            return eventReady.get() ? List.of(candidate) : List.of();
        });
        when(store.claimCandidate(candidate)).thenReturn(Optional.of(claimed));
        when(worker.publishClaimedEvent(claimed)).thenAnswer(invocation -> {
            publicationStarted.countDown();
            awaitIgnoringInterrupt(releasePublication);
            return true;
        });
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPublicationObserver observer = new MicrometerPublicationObserver(
                registry, mock(JdbcOutboxRepository.class));
        ReliableEventScheduler scheduler = new ReliableEventScheduler(recovery, store, worker, 1, 1, 0,
                Duration.ofMillis(10), Duration.ofMillis(300), false, Duration.ofMillis(10), observer);
        try {
            scheduler.start();
            assertThat(recoveryStarted.await(2, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(scans).hasValueGreaterThan(4));
            assertThat(recoveries).hasValue(1);
            // The event arrives only after recovery is already blocked.
            eventReady.set(true);
            assertThat(publicationStarted.await(2, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(registry.get("reliable_event.scheduler.last_success_timestamp").gauge().value()).isPositive());
            assertThat(registry.get("reliable_event.recovery.running").gauge().value()).isEqualTo(1);
            assertThat(registry.get("reliable_event.recovery.started_timestamp").gauge().value()).isPositive();
            assertThat(registry.get("reliable_event.recovery.last_success_timestamp").gauge().value()).isZero();
            assertThat(recoveries).hasValue(1);

            // Both recovery and a send are blocked: all executors share one shutdown deadline.
            CountDownLatch stopped = new CountDownLatch(1);
            long startedAt = System.nanoTime();
            scheduler.stop(stopped::countDown);
            assertThat(stopped.await(550, TimeUnit.MILLISECONDS)).isTrue();
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)).isLessThan(550);
            assertThat(registry.get("reliable_event.recovery.running").gauge().value()).isEqualTo(1);
            int stoppedScans = scans.get();
            releaseRecovery.countDown();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(registry.get("reliable_event.recovery.running").gauge().value()).isZero());
            Thread.sleep(60);
            assertThat(recoveries).hasValue(1);
            assertThat(scans).hasValue(stoppedScans);
        } finally {
            releaseRecovery.countDown();
            releasePublication.countDown();
            scheduler.stop();
            observer.close();
            registry.close();
        }
    }

    private static void awaitIgnoringInterrupt(CountDownLatch release) {
        while (release.getCount() != 0) {
            try { release.await(20, TimeUnit.MILLISECONDS); }
            catch (InterruptedException ignored) { /* Simulate application code that cannot be cancelled. */ }
        }
    }
}
