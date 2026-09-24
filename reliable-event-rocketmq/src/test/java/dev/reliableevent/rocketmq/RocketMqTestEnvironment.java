package dev.reliableevent.rocketmq;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.awaitility.Awaitility.await;

final class RocketMqTestEnvironment implements AutoCloseable {

    static final String IMAGE = "apache/rocketmq:5.5.0";
    static final String TOPIC = "reliable-event-test-topic";
    static final String TAG = "execute";
    static final String CONSUMER_GROUP = "reliable-event-test-consumer";
    static final int PROXY_GRPC_PORT = 8081;

    private static final String ROCKETMQ_HOME = "/home/rocketmq/rocketmq-5.5.0";
    private static final String BROKER_CONFIG_PATH = ROCKETMQ_HOME + "/conf/reliable-event-test.conf";
    private static final String BROKER_CONFIG = """
            brokerClusterName=DefaultCluster
            brokerName=broker-a
            brokerId=0
            deleteWhen=04
            fileReservedTime=1
            brokerRole=ASYNC_MASTER
            flushDiskType=ASYNC_FLUSH
            brokerIP1=127.0.0.1
            autoCreateTopicEnable=false
            """;

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> nameserver;
    private final FixedPortContainer broker;
    private boolean brokerPaused;

    RocketMqTestEnvironment() {
        DockerImageName image = DockerImageName.parse(IMAGE);
        nameserver = new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases("namesrv")
                .withCommand("sh", "mqnamesrv")
                .waitingFor(Wait.forLogMessage(".*Name Server boot success.*\\n", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        broker = new FixedPortContainer(image)
                .withNetwork(network)
                .withNetworkAliases("broker")
                .withEnv("NAMESRV_ADDR", "namesrv:9876")
                .withCopyToContainer(
                        Transferable.of(BROKER_CONFIG, 0644),
                        BROKER_CONFIG_PATH
                )
                .withCommand(
                        "sh",
                        "mqbroker",
                        "--enable-proxy",
                        "-c",
                        BROKER_CONFIG_PATH
                )
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
        broker.bindFixedPort(PROXY_GRPC_PORT, PROXY_GRPC_PORT);
    }

    void start() {
        nameserver.start();
        nameserver.followOutput(new Slf4jLogConsumer(
                LoggerFactory.getLogger(RocketMqTestEnvironment.class)
        ).withPrefix("nameserver"));
        broker.start();
        broker.followOutput(new Slf4jLogConsumer(
                LoggerFactory.getLogger(RocketMqTestEnvironment.class)
        ).withPrefix("broker"));
        createTopic();
    }

    ClientConfiguration clientConfiguration(Duration requestTimeout) {
        return ClientConfiguration.newBuilder()
                .setEndpoints(broker.getHost() + ":" + broker.getMappedPort(PROXY_GRPC_PORT))
                .setRequestTimeout(requestTimeout)
                .enableSsl(false)
                .build();
    }

    void pauseBroker() {
        broker.getDockerClient().pauseContainerCmd(broker.getContainerId()).exec();
        brokerPaused = true;
    }

    void unpauseBroker() {
        if (brokerPaused) {
            broker.getDockerClient().unpauseContainerCmd(broker.getContainerId()).exec();
            brokerPaused = false;
        }
    }

    private void createTopic() {
        StringBuilder diagnostics = new StringBuilder();
        try {
            await("RocketMQ broker registration becomes visible")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(250))
                    .until(this::brokerRegistrationIsVisible);
            org.testcontainers.containers.Container.ExecResult result = broker.execInContainer(
                    "sh",
                    "-c",
                    ROCKETMQ_HOME + "/bin/mqadmin updateTopic"
                            + " -n namesrv:9876 -c DefaultCluster -t " + TOPIC
            );
            if (result.getExitCode() != 0
                    || result.getStderr().contains("SubCommandException")) {
                throw new IllegalStateException(
                        "Unable to create RocketMQ test topic: "
                                + result.getStdout() + result.getStderr()
                );
            }
            org.testcontainers.containers.Container.ExecResult groupResult = broker.execInContainer(
                    "sh",
                    "-c",
                    ROCKETMQ_HOME + "/bin/mqadmin updateSubGroup"
                            + " -n namesrv:9876 -c DefaultCluster -g " + CONSUMER_GROUP
            );
            if (groupResult.getExitCode() != 0
                    || groupResult.getStderr().contains("SubCommandException")) {
                throw new IllegalStateException(
                        "Unable to create RocketMQ test consumer group: "
                                + groupResult.getStdout() + groupResult.getStderr()
                );
            }
            diagnostics.append("updateTopic stdout: ").append(result.getStdout())
                    .append("; stderr: ").append(result.getStderr());
            await("RocketMQ topic route becomes visible")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> topicRouteIsVisible(diagnostics));
        } catch (org.awaitility.core.ConditionTimeoutException exception) {
            appendCommandDiagnostics(
                    diagnostics,
                    "clusterList",
                    ROCKETMQ_HOME + "/bin/mqadmin clusterList -n namesrv:9876"
            );
            appendCommandDiagnostics(
                    diagnostics,
                    "broker.log",
                    "tail -n 80 /home/rocketmq/logs/rocketmqlogs/broker.log"
            );
            throw new IllegalStateException(diagnostics.toString(), exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating RocketMQ test topic", exception);
        } catch (java.io.IOException exception) {
            throw new IllegalStateException("Unable to create RocketMQ test topic", exception);
        }
    }

    private boolean brokerRegistrationIsVisible() {
        try {
            org.testcontainers.containers.Container.ExecResult result = broker.execInContainer(
                    "sh",
                    "-c",
                    ROCKETMQ_HOME + "/bin/mqadmin clusterList -n namesrv:9876"
            );
            return result.getExitCode() == 0 && result.getStdout().contains("broker-a");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while checking RocketMQ broker registration",
                    exception
            );
        } catch (java.io.IOException exception) {
            return false;
        }
    }

    private void appendCommandDiagnostics(
            StringBuilder diagnostics,
            String label,
            String command
    ) {
        try {
            org.testcontainers.containers.Container.ExecResult result = broker.execInContainer(
                    "sh",
                    "-c",
                    command
            );
            diagnostics.append(System.lineSeparator()).append(label)
                    .append(" exitCode: ").append(result.getExitCode())
                    .append("; stdout: ").append(result.getStdout())
                    .append("; stderr: ").append(result.getStderr());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (java.io.IOException exception) {
            diagnostics.append(System.lineSeparator()).append(label)
                    .append(" diagnostics unavailable: ").append(exception.getClass().getSimpleName());
        }
    }

    private boolean topicRouteIsVisible(StringBuilder diagnostics) {
        try {
            org.testcontainers.containers.Container.ExecResult result = broker.execInContainer(
                    "sh",
                    "-c",
                    ROCKETMQ_HOME + "/bin/mqadmin topicRoute"
                            + " -n namesrv:9876 -t " + TOPIC
            );
            diagnostics.setLength(0);
            diagnostics.append("topicRoute exitCode: ").append(result.getExitCode())
                    .append("; stdout: ").append(result.getStdout())
                    .append("; stderr: ").append(result.getStderr());
            return result.getExitCode() == 0
                    && result.getStdout().contains("broker-a");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while checking RocketMQ topic route", exception);
        } catch (java.io.IOException exception) {
            return false;
        }
    }

    @Override
    public void close() {
        unpauseBroker();
        broker.stop();
        nameserver.stop();
        network.close();
    }

    private static final class FixedPortContainer
            extends GenericContainer<FixedPortContainer> {

        private FixedPortContainer(DockerImageName image) {
            super(image);
        }

        private void bindFixedPort(int hostPort, int containerPort) {
            addFixedExposedPort(hostPort, containerPort);
        }
    }
}
