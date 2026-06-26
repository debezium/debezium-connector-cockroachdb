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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
 * Regression test for dbz#2154: on restart the connector must resume the intermediate changefeed
 * consumer from its persisted position instead of resetting to {@code earliest} and re-emitting the
 * whole retained topic.
 *
 * <p>The test runs a task, captures the rows it emits and the source offset it would persist, then
 * starts a second task seeded with that offset (simulating a Kafka Connect restart) and asserts the
 * second task does not re-emit the already-processed rows. Before the fix the second task replays the
 * entire topic; after the fix it resumes and emits nothing for the unchanged data.</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBRestartResumeIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBRestartResumeIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.11");
    private static final String DATABASE_NAME = "restartresume_testdb";
    private static final String TABLE_NAME = "restartresume_orders";

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(
            DockerImageName.parse("cockroachdb/cockroach:" + COCKROACHDB_VERSION))
            .withNetwork(NETWORK)
            .withNetworkAliases("cockroachdb");

    private Connection connection;
    private CockroachDBConnectorTask task1;
    private CockroachDBConnectorTask task2;

    @BeforeEach
    public void setUp() throws Exception {
        kafka.start();
        cockroachdb.start();

        String defaultJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb");
        try (Connection defaultConn = DriverManager.getConnection(
                defaultJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = defaultConn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
        }

        String testJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/" + DATABASE_NAME);
        connection = DriverManager.getConnection(testJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (id INT PRIMARY KEY, name STRING)");
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        for (CockroachDBConnectorTask t : new CockroachDBConnectorTask[]{ task1, task2 }) {
            if (t != null) {
                try {
                    t.stop();
                }
                catch (Exception e) {
                    LOGGER.warn("Error stopping task: {}", e.getMessage());
                }
            }
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    public void shouldResumeFromPersistedOffsetInsteadOfReplayingOnRestart() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");
        Map<String, String> config = baseConfig(hostBootstrap);

        // Holds the offset the "previous run" persisted, fed back to the restarted task.
        AtomicReference<Map<String, Object>> persistedOffset = new AtomicReference<>(null);

        // --- Run 1: process the initial inserts and capture the offset that would be persisted. ---
        task1 = new CockroachDBConnectorTask();
        task1.initialize(contextReturning(persistedOffset));
        startTask(task1, config);
        waitForRunningChangefeed(30);

        int rowCount = 5;
        try (Statement stmt = connection.createStatement()) {
            for (int i = 1; i <= rowCount; i++) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (" + i + ", 'name-" + i + "')");
            }
        }

        Set<Integer> firstRunIds = new HashSet<>();
        for (int i = 0; i < 60 && firstRunIds.size() < rowCount; i++) {
            for (SourceRecord r : poll(task1)) {
                Integer id = createdId(r);
                if (id != null) {
                    firstRunIds.add(id);
                }
                // Capture the latest source offset; this is what Connect would persist and replay on restart.
                @SuppressWarnings("unchecked")
                Map<String, Object> off = (Map<String, Object>) r.sourceOffset();
                if (off != null) {
                    persistedOffset.set(new HashMap<>(off));
                }
            }
            Thread.sleep(1000);
        }
        assertThat(firstRunIds).as("Run 1 should emit all inserted rows").hasSize(rowCount);
        assertThat(persistedOffset.get())
                .as("Run 1 should persist a consumer position")
                .isNotNull();
        assertThat(persistedOffset.get().keySet().stream().anyMatch(k -> k.startsWith("consumer.offset.")))
                .as("Persisted offset must contain the intermediate consumer position")
                .isTrue();

        task1.stop();
        task1 = null;

        // --- Run 2: restart seeded with the persisted offset, no new DML. Must not replay. ---
        task2 = new CockroachDBConnectorTask();
        task2.initialize(contextReturning(persistedOffset));
        startTask(task2, config);

        Set<Integer> secondRunIds = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            for (SourceRecord r : poll(task2)) {
                Integer id = createdId(r);
                if (id != null) {
                    secondRunIds.add(id);
                }
            }
            Thread.sleep(1000);
        }

        LOGGER.info("Run 2 re-emitted ids (should be empty): {}", secondRunIds);
        assertThat(secondRunIds)
                .as("After restart the connector must resume from the persisted offset and not replay processed rows")
                .isEmpty();
    }

    private Map<String, String> baseConfig(String hostBootstrap) {
        Map<String, String> config = new HashMap<>();
        config.put("name", "restartresume-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "restartresume-test");
        config.put("topic.prefix", "restartresume-test");
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", hostBootstrap);
        config.put("cockroachdb.changefeed.resolved.interval", "5s");
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("snapshot.mode", "no_data");
        config.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        return config;
    }

    private void startTask(CockroachDBConnectorTask task, Map<String, String> config) throws Exception {
        AtomicReference<Throwable> taskError = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try {
                task.start(config);
                started.countDown();
            }
            catch (Throwable e) {
                taskError.set(e);
                started.countDown();
                LOGGER.error("Task start failed: {}", e.getMessage(), e);
            }
        });
        t.setDaemon(true);
        t.start();
        assertThat(started.await(30, TimeUnit.SECONDS)).as("Task should start within 30 seconds").isTrue();
        assertThat(taskError.get()).as("Task should start without error").isNull();
    }

    private static List<SourceRecord> poll(CockroachDBConnectorTask task) {
        try {
            List<SourceRecord> r = task.poll();
            return r != null ? r : List.of();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    private static Integer createdId(SourceRecord record) {
        if (record.value() instanceof Struct value && value.schema().field("after") != null) {
            Struct after = value.getStruct("after");
            if (after != null && after.schema().field("id") != null && after.get("id") != null) {
                return ((Number) after.get("id")).intValue();
            }
        }
        return null;
    }

    private void waitForRunningChangefeed(int maxSeconds) throws Exception {
        for (int i = 0; i < maxSeconds; i++) {
            try (Statement stmt = connection.createStatement();
                    var rs = stmt.executeQuery(
                            "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' AND description LIKE '%" + TABLE_NAME + "%'")) {
                if (rs.next() && rs.getInt(1) > 0) {
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Connector did not start a running changefeed within " + maxSeconds + "s");
    }

    private SourceTaskContext contextReturning(AtomicReference<Map<String, Object>> persistedOffset) {
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
                        return persistedOffset.get();
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                        Map<Map<String, T>, Map<String, Object>> result = new HashMap<>();
                        Map<String, Object> off = persistedOffset.get();
                        if (off != null) {
                            for (Map<String, T> p : partitions) {
                                result.put(p, off);
                            }
                        }
                        return result;
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
