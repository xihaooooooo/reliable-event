package dev.reliableevent.jdbc.internal.cycle;

import dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker;
import dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JdbcEventPublicationCycleTest {

    private final JdbcExpiredLeaseRecovery recovery = mock(JdbcExpiredLeaseRecovery.class);
    private final JdbcEventPublicationWorker worker = mock(JdbcEventPublicationWorker.class);

    @Test
    void requiresItsDependencies() {
        assertThatThrownBy(() -> new JdbcEventPublicationCycle(null, worker))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("recovery");
        assertThatThrownBy(() -> new JdbcEventPublicationCycle(recovery, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("worker");
    }

    @Test
    void recoversBeforePublishingAndReturnsBothCounts() {
        when(recovery.recoverExpiredLeases()).thenReturn(2);
        when(worker.publishDueEvents()).thenReturn(3);

        PublicationCycleResult result =
                new JdbcEventPublicationCycle(recovery, worker).runOnce();

        assertThat(result).isEqualTo(new PublicationCycleResult(2, 3));
        InOrder order = inOrder(recovery, worker);
        order.verify(recovery).recoverExpiredLeases();
        order.verify(worker).publishDueEvents();
    }

    @Test
    void recoveryFailurePreventsPublishing() {
        IllegalStateException failure = new IllegalStateException("recovery failed");
        when(recovery.recoverExpiredLeases()).thenThrow(failure);

        assertThatThrownBy(() -> new JdbcEventPublicationCycle(recovery, worker).runOnce())
                .isSameAs(failure);
        verify(worker, never()).publishDueEvents();
    }

    @Test
    void publicationFailureIsPreserved() {
        IllegalStateException failure = new IllegalStateException("publication failed");
        when(recovery.recoverExpiredLeases()).thenReturn(1);
        when(worker.publishDueEvents()).thenThrow(failure);

        assertThatThrownBy(() -> new JdbcEventPublicationCycle(recovery, worker).runOnce())
                .isSameAs(failure);
        verify(recovery).recoverExpiredLeases();
    }
}
