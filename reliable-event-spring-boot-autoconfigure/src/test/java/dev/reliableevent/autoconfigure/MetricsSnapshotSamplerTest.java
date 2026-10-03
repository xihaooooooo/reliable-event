package dev.reliableevent.autoconfigure;

import dev.reliableevent.jdbc.internal.model.OutboxMetricsSnapshot;
import dev.reliableevent.jdbc.internal.persistence.JdbcOutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MetricsSnapshotSamplerTest {

    @Test
    void jdbcResourcesStaySingleUntilLateQueryExitsAndARebuiltContextCanSample() throws Exception {
        JdbcFixture dataSource = new JdbcFixture();
        JdbcOutboxRepository repository = new JdbcOutboxRepository(new JdbcTemplate(dataSource));
        SimpleMeterRegistry firstRegistry = new SimpleMeterRegistry();
        MicrometerPublicationObserver firstObserver = new MicrometerPublicationObserver(
                firstRegistry, repository, Duration.ofSeconds(2));
        MetricsSnapshotSampler firstSampler = new MetricsSnapshotSampler(firstObserver, true,
                Duration.ofMillis(10), Duration.ofMillis(80), Duration.ofMillis(40));
        firstSampler.start();
        try {
            assertThat(dataSource.firstQueryStarted.await(1, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(160);
            assertThat(dataSource.connections).hasValue(1);
            assertThat(dataSource.queryTimeoutSeconds).hasValue(2);
            CountDownLatch stopped = new CountDownLatch(1);
            firstSampler.stop(stopped::countDown);
            assertThat(stopped.await(500, TimeUnit.MILLISECONDS)).isTrue();
            assertThat(firstRegistry.get("reliable_event.ready").gauge().value()).isNaN();
            dataSource.releaseFirstQuery.countDown();
            assertThat(dataSource.firstConnectionClosed.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(firstRegistry.get("reliable_event.ready").gauge().value()).isNaN();
        } finally {
            dataSource.releaseFirstQuery.countDown();
            firstSampler.stop();
            firstObserver.close();
            firstRegistry.close();
        }

        SimpleMeterRegistry rebuiltRegistry = new SimpleMeterRegistry();
        MicrometerPublicationObserver rebuiltObserver = new MicrometerPublicationObserver(
                rebuiltRegistry, repository, Duration.ofSeconds(2));
        MetricsSnapshotSampler rebuiltSampler = new MetricsSnapshotSampler(rebuiltObserver, true,
                Duration.ofSeconds(15), Duration.ofSeconds(1), Duration.ofMillis(100));
        try {
            rebuiltSampler.start();
            awaitGauge(rebuiltRegistry, 1);
            assertThat(dataSource.connections).hasValue(2);
            assertThat(dataSource.closedConnections).hasValue(2);
        } finally {
            rebuiltSampler.stop();
            rebuiltObserver.close();
            rebuiltRegistry.close();
        }
    }

    @Test
    void blockedConnectionAcquisitionUsesTheSameSingleSlotAndDoesNotDelayStop() throws Exception {
        JdbcFixture dataSource = new JdbcFixture(true);
        JdbcOutboxRepository repository = new JdbcOutboxRepository(new JdbcTemplate(dataSource));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPublicationObserver observer = new MicrometerPublicationObserver(
                registry, repository, Duration.ofSeconds(2));
        MetricsSnapshotSampler sampler = new MetricsSnapshotSampler(observer, true,
                Duration.ofMillis(10), Duration.ofMillis(80), Duration.ofMillis(40));
        sampler.start();
        try {
            assertThat(dataSource.acquisitionStarted.await(1, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(160);
            assertThat(dataSource.connections).hasValue(1);
            assertThat(registry.get("reliable_event.snapshot.failure").counter().count()).isEqualTo(1);
            CountDownLatch stopped = new CountDownLatch(1);
            sampler.stop(stopped::countDown);
            assertThat(stopped.await(500, TimeUnit.MILLISECONDS)).isTrue();
            dataSource.releaseAcquisition.countDown();
            assertThat(dataSource.firstConnectionClosed.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(dataSource.connections).hasValue(1);
            assertThat(registry.get("reliable_event.ready").gauge().value()).isNaN();
        } finally {
            dataSource.releaseAcquisition.countDown();
            sampler.stop();
            observer.close();
            registry.close();
        }
    }

    @Test
    void springContextStopsSamplerAndSchedulerInParallelAndRejectsLateOldContextResults() throws Exception {
        JdbcFixture dataSource = new JdbcFixture();
        JdbcOutboxRepository repository = new JdbcOutboxRepository(new JdbcTemplate(dataSource));
        SimpleMeterRegistry firstRegistry = new SimpleMeterRegistry();
        MicrometerPublicationObserver firstObserver = new MicrometerPublicationObserver(
                firstRegistry, repository, Duration.ofSeconds(2));
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        CountDownLatch releaseRecovery = new CountDownLatch(1);
        dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery recovery =
                mock(dev.reliableevent.jdbc.internal.recovery.JdbcExpiredLeaseRecovery.class);
        when(recovery.recoverExpiredLeases()).thenAnswer(invocation -> {
            recoveryStarted.countDown();
            while (releaseRecovery.getCount() > 0) {
                try { releaseRecovery.await(20, TimeUnit.MILLISECONDS); }
                catch (InterruptedException ignored) { /* simulate a recovery call that cannot be cancelled */ }
            }
            return 0;
        });
        dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker worker =
                mock(dev.reliableevent.jdbc.internal.publication.JdbcEventPublicationWorker.class);
        MetricsSnapshotSampler firstSampler = new MetricsSnapshotSampler(firstObserver, true,
                Duration.ofMillis(10), Duration.ofMillis(80), Duration.ofMillis(250));
        ReliableEventScheduler scheduler = new ReliableEventScheduler(recovery, worker, 1, 1, 0,
                Duration.ofSeconds(10), Duration.ofMillis(250), false, Duration.ofMillis(1), firstObserver);
        RecordingLifecycle recordedSampler = new RecordingLifecycle(firstSampler);
        RecordingLifecycle recordedScheduler = new RecordingLifecycle(scheduler);
        GenericApplicationContext firstContext = new GenericApplicationContext();
        firstContext.registerBean("metricsObserver", MicrometerPublicationObserver.class,
                () -> firstObserver, definition -> definition.setDestroyMethodName("close"));
        firstContext.registerBean("samplerLifecycle", RecordingLifecycle.class, () -> recordedSampler);
        firstContext.registerBean("schedulerLifecycle", RecordingLifecycle.class, () -> recordedScheduler);
        firstContext.refresh();

        try {
            assertThat(recoveryStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(dataSource.firstQueryStarted.await(1, TimeUnit.SECONDS)).isTrue();
            firstObserver.refreshSnapshot(); // Manual call observes busy and returns without a second query.
            assertThat(dataSource.connections).hasValue(1);

            long closeStarted = System.nanoTime();
            firstContext.close();
            long closeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStarted);
            assertThat(Math.abs(recordedSampler.stopStarted.get() - recordedScheduler.stopStarted.get()))
                    .isLessThan(TimeUnit.MILLISECONDS.toNanos(100));
            assertThat(closeMillis).isLessThan(400);
            assertThat(recordedSampler.stopCompleted).hasValueGreaterThan(0L);
            assertThat(recordedScheduler.stopCompleted).hasValueGreaterThan(0L);

            SimpleMeterRegistry rebuiltRegistry = new SimpleMeterRegistry();
            MicrometerPublicationObserver rebuiltObserver = new MicrometerPublicationObserver(
                    rebuiltRegistry, repository, Duration.ofSeconds(2));
            MetricsSnapshotSampler rebuiltSampler = new MetricsSnapshotSampler(rebuiltObserver, true,
                    Duration.ofSeconds(15), Duration.ofSeconds(1), Duration.ofMillis(100));
            GenericApplicationContext rebuiltContext = new GenericApplicationContext();
            rebuiltContext.registerBean("rebuiltObserver", MicrometerPublicationObserver.class,
                    () -> rebuiltObserver, definition -> definition.setDestroyMethodName("close"));
            rebuiltContext.registerBean("rebuiltSampler", MetricsSnapshotSampler.class, () -> rebuiltSampler);
            rebuiltContext.refresh();
            try {
                awaitGauge(rebuiltRegistry, 1);
                assertThat(dataSource.connections).hasValue(2);
                dataSource.releaseFirstQuery.countDown();
                assertThat(dataSource.firstConnectionClosed.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(firstRegistry.find("reliable_event.ready").gauge()).isNull();
                assertThat(rebuiltRegistry.get("reliable_event.ready").gauge().value()).isEqualTo(1);
            } finally {
                rebuiltContext.close();
                rebuiltRegistry.close();
            }
        } finally {
            dataSource.releaseFirstQuery.countDown();
            releaseRecovery.countDown();
            if (firstContext.isActive()) firstContext.close();
            firstRegistry.close();
        }
    }

    @Test
    void timedOutQueryKeepsItsSingleSlotUntilItActuallyReturnsAndStopStaysBounded() throws Exception {
        JdbcOutboxRepository repository = mock(JdbcOutboxRepository.class);
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch releaseQuery = new CountDownLatch(1);
        AtomicInteger queryCount = new AtomicInteger();
        when(repository.readMetricsSnapshot(2)).thenAnswer(invocation -> {
            queryCount.incrementAndGet();
            queryStarted.countDown();
            while (releaseQuery.getCount() != 0) {
                try { releaseQuery.await(20, TimeUnit.MILLISECONDS); }
                catch (InterruptedException ignored) { /* Simulate a JDBC call that ignores cancel. */ }
            }
            return new OutboxMetricsSnapshot(0, 0, 1, 1, 1, 1, 0);
        });
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPublicationObserver observer = new MicrometerPublicationObserver(registry, repository);
        MetricsSnapshotSampler sampler = new MetricsSnapshotSampler(observer, true,
                Duration.ofMillis(10), Duration.ofMillis(80), Duration.ofMillis(40));
        sampler.start();
        try {
            assertThat(queryStarted.await(1, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(160);
            assertThat(queryCount).hasValue(1);
            assertThat(registry.get("reliable_event.ready").gauge().value()).isNaN();
            assertThat(registry.get("reliable_event.snapshot.failure").counter().count()).isEqualTo(1);

            CountDownLatch stopped = new CountDownLatch(1);
            long stopStarted = System.nanoTime();
            sampler.stop(stopped::countDown);
            assertThat(stopped.await(500, TimeUnit.MILLISECONDS)).isTrue();
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopStarted)).isLessThan(300);

            releaseQuery.countDown();
            Thread.sleep(80);
            assertThat(queryCount).hasValue(1);
            assertThat(registry.get("reliable_event.ready").gauge().value()).isNaN();
        } finally {
            releaseQuery.countDown();
            sampler.stop();
            observer.close();
            registry.close();
        }
    }

    private static void awaitGauge(SimpleMeterRegistry registry, double expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            var gauge = registry.find("reliable_event.ready").gauge();
            if (gauge != null && gauge.value() == expected) return;
            Thread.sleep(5);
        }
        assertThat(registry.get("reliable_event.ready").gauge().value()).isEqualTo(expected);
    }

    private static final class RecordingLifecycle implements SmartLifecycle {
        private final SmartLifecycle delegate;
        private final AtomicLong stopStarted = new AtomicLong();
        private final AtomicLong stopCompleted = new AtomicLong();

        private RecordingLifecycle(SmartLifecycle delegate) { this.delegate = delegate; }
        @Override public void start() { delegate.start(); }
        @Override public void stop() { delegate.stop(); }
        @Override public void stop(Runnable callback) {
            stopStarted.set(System.nanoTime());
            delegate.stop(() -> {
                stopCompleted.set(System.nanoTime());
                callback.run();
            });
        }
        @Override public boolean isRunning() { return delegate.isRunning(); }
        @Override public boolean isAutoStartup() { return delegate.isAutoStartup(); }
        @Override public int getPhase() { return delegate.getPhase(); }
    }

    private static final class JdbcFixture extends AbstractDataSource {
        final CountDownLatch acquisitionStarted = new CountDownLatch(1);
        final CountDownLatch releaseAcquisition = new CountDownLatch(1);
        final CountDownLatch firstQueryStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirstQuery = new CountDownLatch(1);
        final CountDownLatch firstConnectionClosed = new CountDownLatch(1);
        final AtomicInteger connections = new AtomicInteger();
        final AtomicInteger closedConnections = new AtomicInteger();
        final AtomicInteger queryTimeoutSeconds = new AtomicInteger();
        private final boolean blockAcquisition;

        private JdbcFixture() { this(false); }

        private JdbcFixture(boolean blockAcquisition) { this.blockAcquisition = blockAcquisition; }

        @Override
        public Connection getConnection(String username, String password) {
            return getConnection();
        }

        @Override
        public Connection getConnection() {
            int connectionNumber = connections.incrementAndGet();
            if (connectionNumber == 1) {
                acquisitionStarted.countDown();
                if (blockAcquisition) {
                    while (releaseAcquisition.getCount() > 0) {
                        try { releaseAcquisition.await(20, TimeUnit.MILLISECONDS); }
                        catch (InterruptedException ignored) { /* emulate a pool that ignores interruption */ }
                    }
                }
            }
            AtomicBoolean connectionClosed = new AtomicBoolean();
            return proxy(Connection.class, (proxy, method, args) -> {
                return switch (method.getName()) {
                    case "prepareStatement" -> statement(connectionNumber);
                    case "close" -> {
                        if (connectionClosed.compareAndSet(false, true)) {
                            closedConnections.incrementAndGet();
                            if (connectionNumber == 1) firstConnectionClosed.countDown();
                        }
                        yield null;
                    }
                    case "isClosed" -> connectionClosed.get();
                    case "toString" -> "fixture-connection-" + connectionNumber;
                    default -> defaultValue(method.getReturnType());
                };
            });
        }

        private PreparedStatement statement(int connectionNumber) {
            AtomicBoolean statementClosed = new AtomicBoolean();
            return proxy(PreparedStatement.class, (proxy, method, args) -> {
                return switch (method.getName()) {
                    case "setQueryTimeout" -> { queryTimeoutSeconds.set((Integer) args[0]); yield null; }
                    case "executeQuery" -> {
                        if (connectionNumber == 1 && !blockAcquisition) {
                            firstQueryStarted.countDown();
                            while (releaseFirstQuery.getCount() > 0) {
                                try { releaseFirstQuery.await(20, TimeUnit.MILLISECONDS); }
                                catch (InterruptedException ignored) { /* emulate an uncancellable driver call */ }
                            }
                        }
                        yield resultSet();
                    }
                    case "close" -> { statementClosed.set(true); yield null; }
                    case "isClosed" -> statementClosed.get();
                    case "toString" -> "fixture-statement";
                    default -> defaultValue(method.getReturnType());
                };
            });
        }

        private ResultSet resultSet() {
            AtomicBoolean returned = new AtomicBoolean();
            return proxy(ResultSet.class, (proxy, method, args) -> {
                return switch (method.getName()) {
                    case "next" -> returned.compareAndSet(false, true);
                    case "getLong" -> switch (args[0].toString()) {
                        case "backlog", "dead", "unfinished_timestamp_missing" -> 0L;
                        case "ready", "unfinished_overdue" -> 1L;
                        default -> 0L;
                    };
                    case "getDouble" -> 12.0;
                    case "wasNull" -> false;
                    case "close", "toString" -> null;
                    default -> defaultValue(method.getReturnType());
                };
            });
        }

        @SuppressWarnings("unchecked")
        private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
        }

        private static Object defaultValue(Class<?> type) {
            if (!type.isPrimitive() || type == void.class) return null;
            if (type == boolean.class) return false;
            if (type == char.class) return '\0';
            if (type == byte.class) return (byte) 0;
            if (type == short.class) return (short) 0;
            if (type == int.class) return 0;
            if (type == long.class) return 0L;
            if (type == float.class) return 0f;
            if (type == double.class) return 0d;
            return null;
        }
    }
}
