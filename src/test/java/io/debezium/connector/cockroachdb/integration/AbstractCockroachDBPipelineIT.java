/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.cockroachdb.CockroachDBConnectorTask;

/**
 * Base class for pipeline integration tests that run the connector task in process against a
 * CockroachDB and Kafka Testcontainers stack.
 *
 * <p>The containers are started once per subclass and shared by every scenario in it, so
 * scenario classes carry only their own schema setup and assertions instead of a private copy
 * of the stack and the task plumbing. Scenarios isolate themselves by using distinct database
 * names and topic prefixes. The stack is released when the class finishes so later test
 * classes do not compete with it for container runtime resources.</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public abstract class AbstractCockroachDBPipelineIT {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCockroachDBPipelineIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.13");

    private static final Network NETWORK = Network.newNetwork();

    @Container
    protected static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    protected static final CockroachContainer cockroachdb = new CockroachContainer(
            DockerImageName.parse("cockroachdb/cockroach:" + COCKROACHDB_VERSION))
            .withNetwork(NETWORK)
            .withNetworkAliases("cockroachdb");

    @BeforeAll
    public static void enableRangefeeds() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb"),
                cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
        }
    }

    protected CockroachDBConnectorTask task;

    @AfterEach
    public void stopTaskAfterTest() {
        stopTask();
    }

    /**
     * Creates the database if it does not exist and returns a connection to it.
     * The caller owns the connection.
     */
    protected Connection openDatabase(String databaseName) throws Exception {
        try (Connection conn = DriverManager.getConnection(
                cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb"),
                cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + databaseName);
        }
        return DriverManager.getConnection(
                cockroachdb.getJdbcUrl().replace("/postgres", "/" + databaseName),
                cockroachdb.getUsername(), cockroachdb.getPassword());
    }

    /**
     * Builds the standard connector configuration every pipeline scenario uses. The scenario
     * name doubles as the connector name, server name, and topic prefix, which keeps the
     * intermediate topics of concurrently defined scenarios apart.
     */
    protected Map<String, String> baseConnectorConfig(String scenarioName, String databaseName, String tableIncludeList) {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", scenarioName);
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", databaseName);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", scenarioName);
        config.put("topic.prefix", scenarioName);
        config.put("table.include.list", tableIncludeList);
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", hostBootstrap);
        config.put("cockroachdb.changefeed.enriched.properties", "source,schema");
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("cockroachdb.changefeed.resolved.interval", "2s");
        config.put("snapshot.mode", "initial");
        config.put("heartbeat.interval.ms", "1000");
        config.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        return config;
    }

    /**
     * Starts the connector task on a daemon thread and asserts that startup succeeds.
     */
    protected void startTask(Map<String, String> config) throws Exception {
        task = new CockroachDBConnectorTask();
        task.initialize(createMockContext());

        AtomicReference<Throwable> taskError = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread taskThread = new Thread(() -> {
            try {
                task.start(config);
                started.countDown();
            }
            catch (Throwable e) {
                taskError.set(e);
                started.countDown();
            }
        });
        taskThread.setDaemon(true);
        taskThread.start();

        boolean didStart = started.await(30, TimeUnit.SECONDS);
        assertThat(didStart).as("Task should start within 30 seconds").isTrue();
        assertThat(taskError.get()).as("Task should start without error").isNull();
    }

    protected void stopTask() {
        if (task != null) {
            try {
                task.stop();
            }
            catch (Exception e) {
                LOGGER.warn("Error stopping task: {}", e.getMessage());
            }
            task = null;
        }
    }

    /**
     * Polls the running task until at least {@code minimum} non-heartbeat records matching the
     * filter arrive, or the attempts are exhausted. One attempt is roughly one second.
     */
    protected List<SourceRecord> pollForRecords(Predicate<SourceRecord> filter, int minimum, int attempts) throws Exception {
        List<SourceRecord> collected = new ArrayList<>();
        for (int i = 0; i < attempts; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null) {
                    for (SourceRecord r : records) {
                        if (r.topic() != null && !r.topic().contains("__debezium-heartbeat") && filter.test(r)) {
                            collected.add(r);
                        }
                    }
                }
            }
            catch (Exception e) {
                LOGGER.warn("Poll failed: {}", e.getMessage());
            }
            if (collected.size() >= minimum) {
                break;
            }
            Thread.sleep(1000);
        }
        return collected;
    }

    protected List<SourceRecord> pollForRecords(int minimum, int attempts) throws Exception {
        return pollForRecords(r -> true, minimum, attempts);
    }

    protected SourceTaskContext createMockContext() {
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return new HashMap<>();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return null;
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                                                                                java.util.Collection<Map<String, T>> partitions) {
                        return new HashMap<>();
                    }
                };
            }

            @Override
            public PluginMetrics pluginMetrics() {
                return null;
            }
        };
    }
}
