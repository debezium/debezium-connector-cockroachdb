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
 * End-to-end integration test for the CockroachDB Debezium connector.
 *
 * <p>Validates the full pipeline: CockroachDB changefeed -> intermediate Kafka topic
 * -> connector's KafkaConsumer -> Debezium pipeline -> SourceRecords from poll().</p>
 *
 * <p>Uses Testcontainers for CockroachDB and Kafka, creates a real changefeed,
 * inserts data, starts the connector task, and asserts that Debezium-formatted
 * SourceRecords are produced.</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBEndToEndIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBEndToEndIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.10");
    private static final String DATABASE_NAME = "e2e_testdb";
    private static final String TABLE_NAME = "e2e_orders";

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
    private CockroachDBConnectorTask task;

    @BeforeEach
    public void setUp() throws Exception {
        kafka.start();
        cockroachdb.start();

        String defaultJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb");
        try (Connection defaultConn = DriverManager.getConnection(
                defaultJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword())) {
            try (Statement stmt = defaultConn.createStatement()) {
                stmt.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
            }
        }

        String testJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/" + DATABASE_NAME);
        connection = DriverManager.getConnection(testJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword());

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ("
                    + "id INT PRIMARY KEY, "
                    + "customer_name STRING NOT NULL, "
                    + "amount DECIMAL(10,2), "
                    + "status STRING DEFAULT 'PENDING'"
                    + ")");
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (task != null) {
            try {
                task.stop();
            }
            catch (Exception e) {
                LOGGER.warn("Error stopping task: {}", e.getMessage());
            }
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    public void shouldStartTaskAndProduceSourceRecords() throws Exception {
        String changefeedTopicPrefix = "e2e";

        // The connector owns the changefeed. CockroachDB runs in its own container, so its sink URI
        // must use the in-network Kafka alias (kafka:9092). The connector's own consumer runs in this
        // JVM, so it reads from the host-mapped Kafka port via the bootstrap.servers override.
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", "e2e-cockroachdb-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "e2e-test");
        config.put("topic.prefix", "e2e-test");

        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        config.put("cockroachdb.changefeed.sink.topic.prefix", changefeedTopicPrefix);
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", hostBootstrap);
        config.put("cockroachdb.changefeed.include.diff", "true");
        config.put("cockroachdb.changefeed.resolved.interval", "5s");
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("snapshot.mode", "no_data");
        config.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

        task = new CockroachDBConnectorTask();
        task.initialize(createMockContext());
        LOGGER.info("Task initialized, starting...");

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
                LOGGER.error("Task start failed: {}", e.getMessage(), e);
            }
        });
        taskThread.setDaemon(true);
        taskThread.start();

        boolean didStart = started.await(30, TimeUnit.SECONDS);
        if (taskError.get() != null) {
            LOGGER.error("Task failed to start", taskError.get());
        }
        assertThat(didStart).as("Task should start within 30 seconds").isTrue();
        assertThat(taskError.get()).as("Task should start without error").isNull();

        // Wait until the connector's changefeed is actually running before producing DML, otherwise
        // the changefeed (created with cursor=now) would not capture the changes.
        waitForRunningChangefeed(30);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Alice', 100.00, 'PENDING')");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 'Bob', 200.50, 'PROCESSING')");
            stmt.execute("UPDATE " + TABLE_NAME + " SET status = 'COMPLETED' WHERE id = 1");
            stmt.execute("DELETE FROM " + TABLE_NAME + " WHERE id = 2");
        }
        LOGGER.info("Inserted test data");

        List<SourceRecord> allRecords = new ArrayList<>();
        int maxAttempts = 60;
        for (int i = 0; i < maxAttempts && allRecords.size() < 4; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null && !records.isEmpty()) {
                    allRecords.addAll(records);
                    LOGGER.info("Poll attempt {}: received {} records (total: {})",
                            i + 1, records.size(), allRecords.size());
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            catch (Exception e) {
                LOGGER.warn("Poll attempt {} failed: {}", i + 1, e.getMessage());
            }
            Thread.sleep(1000);
        }

        LOGGER.info("End-to-end test collected {} total SourceRecords", allRecords.size());

        // The pipeline must actually deliver records end to end, not merely start the task.
        assertThat(allRecords)
                .as("Connector should produce SourceRecords for the changefeed DML (CRDB -> Kafka -> connector)")
                .isNotEmpty();

        for (SourceRecord record : allRecords) {
            assertThat(record.topic()).isNotNull();
            assertThat(record.sourcePartition()).isNotNull();
            assertThat(record.sourceOffset()).isNotNull();
        }

        // Confirm the create operations made it through (op='c'). Update/delete may arrive after the
        // poll window or as tombstones, so creates are the reliable end-to-end signal.
        long createCount = allRecords.stream()
                .map(SourceRecord::value)
                .filter(v -> v instanceof Struct)
                .map(v -> (Struct) v)
                .filter(s -> s.schema().field("op") != null)
                .map(s -> s.getString("op"))
                .filter("c"::equals)
                .count();
        assertThat(createCount)
                .as("Connector should deliver at least one create (op='c') event end to end")
                .isGreaterThanOrEqualTo(1L);
    }

    /**
     * Polls {@code SHOW CHANGEFEED JOBS} until the connector has a running changefeed for the test
     * table, so that DML produced afterward is captured by the changefeed.
     */
    private void waitForRunningChangefeed(int maxSeconds) throws Exception {
        for (int i = 0; i < maxSeconds; i++) {
            try (Statement stmt = connection.createStatement();
                    var rs = stmt.executeQuery(
                            "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' AND description LIKE '%" + TABLE_NAME + "%'")) {
                if (rs.next() && rs.getInt(1) > 0) {
                    LOGGER.info("Changefeed is running after {}s", i);
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Connector did not start a running changefeed for table " + TABLE_NAME + " within " + maxSeconds + "s");
    }

    private SourceTaskContext createMockContext() {
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
