package dev.reliableevent.autoconfigure;

import dev.reliableevent.EventId;
import dev.reliableevent.jdbc.internal.model.ClaimedEvent;
import dev.reliableevent.jdbc.internal.model.EventCandidate;
import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReliableEventShutdownContextTest {

    @Test
    void contextClosesTheProducerAfterPublicationAndStateCompletion() throws Exception {
        JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
        JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);
        Producer producer = mock(Producer.class);
        EventCandidate candidate = new EventCandidate(new EventId(1), 0);
        ClaimedEvent claim = mock(ClaimedEvent.class);
        CountDownLatch publicationStarted = new CountDownLatch(1);
        CountDownLatch releasePublication = new CountDownLatch(1);
        AtomicBoolean publicationCompleted = new AtomicBoolean();
        AtomicBoolean closedBeforeCompletion = new AtomicBoolean();
        when(worker.findDueEventCandidates(anyInt())).thenReturn(List.of(candidate));
        when(worker.claimCandidate(candidate)).thenReturn(Optional.of(claim));
        when(worker.publishClaimedEvent(claim)).thenAnswer(invocation -> {
            publicationStarted.countDown();
            if (!releasePublication.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Publication was not released");
            }
            publicationCompleted.set(true);
            return true;
        });
        doAnswer(invocation -> {
            closedBeforeCompletion.set(!publicationCompleted.get());
            return null;
        }).when(producer).close();

        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean(Producer.class, () -> producer,
                definition -> definition.setDestroyMethodName("close"));
        context.registerBean(ReliableEventScheduler.class, () -> new ReliableEventScheduler(
                recovery, worker, 1, 1, 0, Duration.ofMillis(100), Duration.ofSeconds(3)));
        Thread closer = new Thread(context::close, "test-context-close");
        try {
            context.refresh();
            assertThat(publicationStarted.await(5, TimeUnit.SECONDS)).isTrue();
            closer.start();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(context.getBean(ReliableEventScheduler.class).isRunning()).isFalse());
            assertThat(closer.isAlive()).isTrue();
            org.mockito.Mockito.verify(producer, org.mockito.Mockito.never()).close();

            releasePublication.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(closer.isAlive()).isFalse();
            assertThat(publicationCompleted).isTrue();
            assertThat(closedBeforeCompletion).isFalse();
            verify(producer).close();
        } finally {
            releasePublication.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(5));
            context.close();
        }
    }
}
