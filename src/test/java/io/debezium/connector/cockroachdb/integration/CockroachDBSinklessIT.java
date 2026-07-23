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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.cockroachdb.CockroachDBConnectorTask;

/**
 * End-to-end integration test for the connector's <b>sinkless</b> changefeed delivery mode
 * ({@code cockroachdb.changefeed.sink.type=sinkless}).
 *
 * <p>Unlike the kafka mode, a sinkless (core) changefeed streams change events back over the
 * connector's own SQL connection, so there is <b>no intermediate Kafka</b> at all. This test
 * therefore starts only CockroachDB (no Kafka container), runs the connector task in sinkless
 * mode with <b>no sink URI</b>, produces DML, and asserts Debezium {@link SourceRecord}s come out
 * of {@code poll()}. It is the key validation that pgjdbc streams the never-ending sinkless
 * result set rather than buffering it.</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBSinklessIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSinklessIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.13");
    private static final String DATABASE_NAME = "sinkless_testdb";
    private static final String TABLE_NAME = "sinkless_orders";

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(
            DockerImageName.parse("cockroachdb/cockroach:" + COCKROACHDB_VERSION));

    private Connection connection;
    private CockroachDBConnectorTask task;

    @BeforeEach
    public void setUp() throws Exception {
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
    public void shouldStreamSinklessChangefeedOverSqlWithoutKafka() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("name", "sinkless-cockroachdb-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "sinkless-test");
        config.put("topic.prefix", "sinkless-test");

        // Sinkless mode: change events stream back over the SQL connection. No sink URI, no
        // intermediate Kafka, no consumer security to configure.
        config.put("cockroachdb.changefeed.sink.type", "sinkless");
        config.put("cockroachdb.changefeed.include.diff", "true");
        config.put("cockroachdb.changefeed.resolved.interval", "5s");
        // snapshot.mode=initial -> the sinkless changefeed runs initial_scan='yes', so rows that
        // exist before the connector starts are emitted deterministically (op='r'). This avoids the
        // cursor=now race where DML produced before the changefeed is actually capturing would be
        // missed: a core changefeed is not visible in SHOW CHANGEFEED JOBS, so there is no reliable
        // external readiness signal; instead the test waits until the initial-scan reads arrive.
        config.put("snapshot.mode", "initial");
        config.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

        // Seed rows BEFORE starting the connector so the initial scan captures them with no race.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Alice', 100.00, 'PENDING')");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 'Bob', 200.50, 'PROCESSING')");
        }
        LOGGER.info("Seeded 2 rows before connector start (captured by initial_scan)");

        task = new CockroachDBConnectorTask();
        task.initialize(createMockContext());
        LOGGER.info("Task initialized, starting in sinkless mode...");

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

        // Phase 1: the initial scan streams the two seeded rows as reads (op='r'). Receiving them
        // proves the sinkless changefeed streams over SQL AND that the changefeed is now live, with
        // no reliance on a racy external readiness signal.
        List<SourceRecord> allRecords = new ArrayList<>();
        long initialReads = pollUntil(allRecords, 60,
                () -> countOp(allRecords, "r") >= 2);
        LOGGER.info("Initial scan delivered {} read (op='r') records over the sinkless stream", initialReads);
        assertThat(countOp(allRecords, "r"))
                .as("Sinkless initial scan should deliver the 2 seeded rows as op='r' reads")
                .isGreaterThanOrEqualTo(2L);

        // Phase 2: the changefeed is confirmed live, so DML produced now is captured deterministically
        // (no cursor race). This exercises the long-lived streaming cursor that the EOF fix enables.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 'Carol', 320.00, 'NEW')");
            stmt.execute("UPDATE " + TABLE_NAME + " SET status = 'COMPLETED' WHERE id = 1");
            stmt.execute("DELETE FROM " + TABLE_NAME + " WHERE id = 2");
        }
        LOGGER.info("Produced live DML after the changefeed was confirmed streaming");

        pollUntil(allRecords, 60, () -> countOp(allRecords, "c") >= 1);
        LOGGER.info("Sinkless test collected {} total SourceRecords", allRecords.size());

        for (SourceRecord record : allRecords) {
            assertThat(record.topic()).isNotNull();
            assertThat(record.sourcePartition()).isNotNull();
            assertThat(record.sourceOffset()).isNotNull();
        }

        // The live INSERT must arrive as a create (op='c'), proving the unbounded cursor keeps
        // streaming changes after the initial scan rather than stalling or being torn down.
        assertThat(countOp(allRecords, "c"))
                .as("Connector should deliver the live create (op='c') event over the sinkless stream")
                .isGreaterThanOrEqualTo(1L);
    }

    /**
     * Polls the task until {@code done} is satisfied or {@code maxAttempts} elapse, accumulating
     * records into {@code sink}. Returns the number of records collected.
     */
    private long pollUntil(List<SourceRecord> sink, int maxAttempts, BooleanSupplier done)
            throws InterruptedException {
        for (int i = 0; i < maxAttempts && !done.getAsBoolean(); i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null && !records.isEmpty()) {
                    sink.addAll(records);
                    LOGGER.info("Poll attempt {}: received {} records (total: {})",
                            i + 1, records.size(), sink.size());
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
        return sink.size();
    }

    private static long countOp(List<SourceRecord> records, String op) {
        return records.stream()
                .map(SourceRecord::value)
                .filter(v -> v instanceof Struct)
                .map(v -> (Struct) v)
                .filter(s -> s.schema().field("op") != null)
                .map(s -> s.getString("op"))
                .filter(op::equals)
                .count();
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
                                                                                Collection<Map<String, T>> partitions) {
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
