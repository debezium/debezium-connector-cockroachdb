/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
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

    private static final String DATABASE_NAME = "sinkless_testdb";
    private static final String TABLE_NAME = "sinkless_orders";

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(CockroachDBTestImage.NAME);

    private Connection connection;
    private CockroachDBConnectorTask task;
    /** The offset that the task finds when it starts. A test that restarts the connector sets it. */
    private final AtomicReference<Map<String, Object>> storedOffset = new AtomicReference<>(null);

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
        Map<String, String> config = sinklessConfig();

        // Seed rows BEFORE starting the connector so the initial scan captures them with no race.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Alice', 100.00, 'PENDING')");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 'Bob', 200.50, 'PROCESSING')");
        }
        LOGGER.info("Seeded 2 rows before connector start (captured by initial_scan)");

        startTask(config);

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

    @Test
    public void shouldEmitRowsInsertedRightAfterTheInitialScanAsCreates() throws Exception {
        // CockroachDB v26.4 sends the first resolved timestamp of a changefeed over several tables only
        // with its first checkpoint, about 30 seconds after the scan. A row inserted in between must be
        // a create, not a snapshot read.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS sinkless_items (id INT PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("CREATE TABLE IF NOT EXISTS sinkless_parts (id INT PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO sinkless_items VALUES (1, 'scanned')");
            stmt.execute("UPSERT INTO sinkless_parts VALUES (1, 'scanned')");
        }

        final Map<String, String> config = sinklessConfig();
        config.put("table.include.list", "public.sinkless_items,public.sinkless_parts");
        startTask(config);

        final List<SourceRecord> allRecords = new ArrayList<>();
        pollUntil(allRecords, 60, () -> countOp(allRecords, "r") >= 2);
        assertThat(countOp(allRecords, "r")).as("The initial scan should deliver the row of each table").isGreaterThanOrEqualTo(2L);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO sinkless_items VALUES (2, 'inserted')");
        }

        pollUntil(allRecords, 60, () -> allRecords.stream().anyMatch(CockroachDBSinklessIT::isInsertedItem));
        final SourceRecord inserted = allRecords.stream().filter(CockroachDBSinklessIT::isInsertedItem).findFirst().orElse(null);
        assertThat(inserted).as("Should receive the inserted row").isNotNull();
        final Struct value = (Struct) inserted.value();
        assertThat(value.getString("op")).as("A row inserted after the scan is a create").isEqualTo("c");
        assertThat(value.getStruct("source").getString("snapshot")).as("A create is not a snapshot record").isIn(null, "false");
    }

    @Test
    public void shouldStopWhenATableWasCreatedAfterTheStoredCursor() throws Exception {
        Map<String, String> config = sinklessConfig();
        config.put("heartbeat.interval.ms", "1000");
        // A table of its own: the tests of this class share the database.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE sinkless_before_restart (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO sinkless_before_restart VALUES (1, 'Alice')");
        }

        // --- Run 1 stores a resolved timestamp. ---
        startTask(config);
        List<SourceRecord> firstRun = new ArrayList<>();
        pollUntil(firstRun, 120, () -> countOp(firstRun, "r") >= 1 && hasResolvedCursor(firstRun));
        assertThat(hasResolvedCursor(firstRun)).as("Run 1 should leave an offset with a resolved timestamp").isTrue();
        storedOffset.set(new HashMap<>(firstRun.get(firstRun.size() - 1).sourceOffset()));
        task.stop();

        // --- A table is created while the connector is stopped. The connector captures every table. ---
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE sinkless_created_later (id INT PRIMARY KEY, name STRING)");
        }

        // --- Run 2: CockroachDB rejects the stored cursor for the new table, and for a sinkless changefeed
        // that means for the one statement that covers all tables. No retry can fix that. ---
        task = new CockroachDBConnectorTask();
        task.initialize(createMockContext());
        task.start(config);
        Throwable failure = null;
        for (int i = 0; i < 60 && failure == null; i++) {
            try {
                task.poll();
                Thread.sleep(1000);
            }
            catch (ConnectException e) {
                failure = e;
            }
        }
        assertThat(failure).as("Task should stop").isInstanceOf(ConnectException.class).isNotInstanceOf(RetriableException.class);
        assertThat(failure.getCause()).hasMessageContaining("sinkless_created_later] did not exist at the stored cursor");
    }

    private static boolean hasResolvedCursor(List<SourceRecord> records) {
        return !records.isEmpty()
                && String.valueOf(records.get(records.size() - 1).sourceOffset().get("offset.cursor")).matches("\\d+\\.\\d+");
    }

    @Test
    public void shouldStreamWhenTheSessionDefaultsToReadCommitted() throws Exception {
        // CockroachDB evaluates cluster_logical_timestamp(), which gives the changefeed its cursor, only in a
        // SERIALIZABLE transaction. The streaming connection has auto-commit disabled. Role defaults do not
        // apply to root, so the connector gets a user of its own.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE USER IF NOT EXISTS read_committed_connector");
            stmt.execute("GRANT admin TO read_committed_connector");
            stmt.execute("ALTER ROLE read_committed_connector SET default_transaction_isolation = 'read committed'");
            stmt.execute("CREATE TABLE IF NOT EXISTS sinkless_read_committed (id INT PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO sinkless_read_committed VALUES (1, 'scanned')");
        }
        String url = cockroachdb.getJdbcUrl().replace("/postgres", "/" + DATABASE_NAME);
        try (Connection session = DriverManager.getConnection(url, "read_committed_connector", "");
                Statement stmt = session.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW transaction_isolation")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).as("A session of the connector's user should default to READ COMMITTED").isEqualTo("read committed");
        }

        final Map<String, String> config = sinklessConfig();
        config.put("database.user", "read_committed_connector");
        config.put("database.password", "");
        config.put("table.include.list", "public.sinkless_read_committed");
        startTask(config);

        final List<SourceRecord> allRecords = new ArrayList<>();
        pollUntil(allRecords, 60, () -> countOp(allRecords, "r") >= 1);
        assertThat(countOp(allRecords, "r")).as("The initial scan should deliver the row").isGreaterThanOrEqualTo(1L);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO sinkless_read_committed VALUES (2, 'inserted')");
        }

        pollUntil(allRecords, 60, () -> countOp(allRecords, "c") >= 1);
        assertThat(countOp(allRecords, "c")).as("The inserted row should arrive as a create").isGreaterThanOrEqualTo(1L);
    }

    private static boolean isInsertedItem(SourceRecord record) {
        return record.topic().endsWith(".sinkless_items") && record.value() instanceof Struct
                && ((Struct) record.value()).getStruct("after") != null
                && "inserted".equals(((Struct) record.value()).getStruct("after").getString("name"));
    }

    private Map<String, String> sinklessConfig() {
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
        return config;
    }

    private void startTask(Map<String, String> config) throws InterruptedException {
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
                        return storedOffset.get();
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                                                                                Collection<Map<String, T>> partitions) {
                        Map<Map<String, T>, Map<String, Object>> result = new HashMap<>();
                        if (storedOffset.get() != null) {
                            partitions.forEach(partition -> result.put(partition, storedOffset.get()));
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
