/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.debezium.connector.cockroachdb.CockroachDBConnectorTask;

/**
 * Restarts a connector with a sinkless changefeed at a stored cursor that CockroachDB no longer covers:
 * the garbage collection has removed the changes since then. CockroachDB rejects a sinkless changefeed
 * at such a cursor. No retry can fix that, because the cursor moves only when the connector streams.
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBSinklessRestartIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSinklessRestartIT.class);

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(CockroachDBTestImage.NAME);

    private final AtomicReference<Map<String, Object>> persistedOffset = new AtomicReference<>(null);
    private CockroachDBConnectorTask task;

    @BeforeEach
    public void setUp() throws Exception {
        cockroachdb.start();
        try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
            stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
        }
    }

    @AfterEach
    public void tearDown() {
        stopTask();
    }

    @Test
    public void shouldStopWhenCockroachDbNoLongerHasTheChangesSinceTheStoredCursor() throws Exception {
        Map<String, String> config = sinklessConfig("expired-sinkless-test", "expired_sinkless_testdb", "initial");
        runUntilTheStoredCursorExpires(config, "expired_sinkless_testdb");

        startTask(config);

        Throwable failure = pollUntilFailure(60);
        assertThat(failure).as("Task should stop at the stored cursor").isInstanceOf(ConnectException.class);
        // A retriable error restarts the connector, which runs into the same error again and again.
        assertThat(failure).as("No retry can fix a cursor that is too old").isNotInstanceOf(RetriableException.class);
        assertThat(messagesOf(failure)).contains("GC threshold", "when_needed");
    }

    @Test
    public void shouldScanAgainWhenTheStoredCursorExpiredAndTheSnapshotModeAllowsIt() throws Exception {
        Map<String, String> config = sinklessConfig("expired-sinkless-rescan-test", "expired_sinkless_rescan_testdb", "when_needed");
        runUntilTheStoredCursorExpires(config, "expired_sinkless_rescan_testdb");

        startTask(config);

        Map<String, String> operations = new HashMap<>();
        pollOperations(operations, 60, () -> operations.containsKey("2"));
        LOGGER.info("Run 2 emitted: {}", operations);
        assertThat(operations).as("row that is in the table now").containsEntry("2", "r");
        assertThat(operations).as("row that was deleted since the stored cursor").doesNotContainKey("1");
    }

    @Test
    public void shouldStopWhenATableIsNewerThanTheStoredCursor() throws Exception {
        // A sinkless changefeed starts all tables at the stored cursor, and CockroachDB rejects a cursor that is
        // older than one of the tables. The cursor moves only while the connector streams, so a restart would
        // run into the same rejection again.
        String database = "newtable_sinkless_testdb";
        Map<String, String> config = sinklessConfig("newtable-sinkless-test", database, "initial");
        try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + database);
            stmt.execute("CREATE TABLE " + database + ".public.expired_orders (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO " + database + ".public.expired_orders VALUES (1, 'before the first run')");
        }
        persistedOffset.set(null);
        startTask(config);
        Map<String, String> firstRun = new HashMap<>();
        pollOperations(firstRun, 120, () -> firstRun.containsKey("1") && hasResolvedCursor());
        assertThat(hasResolvedCursor()).as("Run 1 should leave an offset with a resolved timestamp").isTrue();
        stopTask();

        try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
            stmt.execute("CREATE TABLE " + database + ".public.added_orders (id INT PRIMARY KEY, name STRING)");
        }
        config.put("table.include.list", "public.expired_orders,public.added_orders");
        startTask(config);

        Throwable failure = pollUntilFailure(60);
        assertThat(failure).as("Task should stop at the stored cursor").isInstanceOf(ConnectException.class);
        assertThat(failure).as("No retry can fix a cursor that is older than a table").isNotInstanceOf(RetriableException.class);
        assertThat(messagesOf(failure)).contains("added_orders");
    }

    @Test
    public void shouldStopWhenTheInitialScanIsOvertakenByTheGarbageCollection() throws Exception {
        // The scan reads the table as of its start, and a sinkless changefeed has no job that protects that
        // data. When the connector reads the scan more slowly than the garbage collection moves on, CockroachDB
        // ends the changefeed. A new scan would begin again and end the same way.
        String database = "slowscan_sinkless_testdb";
        String table = database + ".public.expired_orders";
        try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + database);
            stmt.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO " + table + " SELECT i, repeat('x', 500) FROM generate_series(1, 40000) AS g(i)");
            stmt.execute("ALTER TABLE " + table + " CONFIGURE ZONE USING gc.ttlseconds = 1");
            // A changefeed reads ahead into its memory buffer. With the default limit it would read this table at once.
            stmt.execute("SET CLUSTER SETTING changefeed.memory.per_changefeed_limit = '1MiB'");
        }
        try {
            Map<String, String> config = sinklessConfig("slowscan-sinkless-test", database, "when_needed");
            // The connector stops reading the changefeed when its queue is full, which is while the test does not poll.
            config.put("max.queue.size", "64");
            config.put("max.batch.size", "32");
            persistedOffset.set(null);
            startTask(config);
            Map<String, String> scanned = new HashMap<>();
            pollOperations(scanned, 60, () -> scanned.size() >= 10);
            assertThat(scanned).as("The scan should have started").isNotEmpty();

            letTheGarbageCollectionPass(table, currentTimestamp());

            Throwable failure = pollUntilFailure(180);
            assertThat(failure).as("Task should stop").isInstanceOf(ConnectException.class);
            assertThat(failure).as("A new scan would end the same way").isNotInstanceOf(RetriableException.class);
            assertThat(messagesOf(failure)).contains("initial scan", "gc.ttlseconds", "GC threshold");
        }
        finally {
            try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
                stmt.execute("RESET CLUSTER SETTING changefeed.memory.per_changefeed_limit");
            }
        }
    }

    private String currentTimestamp() throws SQLException {
        try (Connection db = adminConnection();
                Statement stmt = db.createStatement();
                var rs = stmt.executeQuery("SELECT cluster_logical_timestamp()::STRING")) {
            rs.next();
            return rs.getString(1);
        }
    }

    /** Writes garbage to the table and runs its garbage collection until a read as of the given time fails. */
    private void letTheGarbageCollectionPass(String table, String timestamp) throws Exception {
        try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
            for (int i = 0; i < 20; i++) {
                stmt.execute("UPDATE " + table + " SET name = 'version " + i + "' WHERE id = 1");
            }
            try {
                // Later versions ask for this before they run the function below.
                stmt.execute("SET allow_unsafe_internals = true");
            }
            catch (SQLException e) {
                LOGGER.debug("This version has no allow_unsafe_internals setting: {}", e.getMessage());
            }
            boolean expired = false;
            for (int i = 0; i < 90 && !expired; i++) {
                Thread.sleep(2000);
                stmt.execute("SELECT crdb_internal.kv_enqueue_replica(range_id, 'mvccGC', true) FROM [SHOW RANGES FROM TABLE " + table + "]");
                try (var rs = stmt.executeQuery("SELECT count(*) FROM " + table + " AS OF SYSTEM TIME '" + timestamp + "'")) {
                    rs.next();
                }
                catch (SQLException e) {
                    expired = e.getMessage().contains("GC threshold");
                }
            }
            assertThat(expired).as("The garbage collection should pass the timestamp " + timestamp).isTrue();
        }
    }

    /**
     * Runs the connector until it has stored a resolved timestamp, stops it, and lets the garbage collection
     * of CockroachDB pass that timestamp.
     */
    private void runUntilTheStoredCursorExpires(Map<String, String> config, String database) throws Exception {
        String table = database + ".public.expired_orders";
        try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + database);
            stmt.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO " + table + " VALUES (1, 'before the first run')");
        }
        persistedOffset.set(null);

        startTask(config);
        Map<String, String> firstRun = new HashMap<>();
        pollOperations(firstRun, 120, () -> firstRun.containsKey("1") && hasResolvedCursor());
        assertThat(firstRun).as("Run 1 should emit the row of its table").containsKey("1");
        assertThat(hasResolvedCursor()).as("Run 1 should leave an offset with a resolved timestamp").isTrue();
        stopTask();
        String storedCursor = String.valueOf(persistedOffset.get().get("offset.cursor"));

        try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
            stmt.execute("ALTER TABLE " + table + " CONFIGURE ZONE USING gc.ttlseconds = 1");
        }
        letTheGarbageCollectionPass(table, storedCursor);
        try (Connection db = adminConnection(); Statement stmt = db.createStatement()) {
            stmt.execute("DELETE FROM " + table + " WHERE id = 1");
            stmt.execute("INSERT INTO " + table + " VALUES (2, 'after the stored cursor')");
        }
    }

    private boolean hasResolvedCursor() {
        Map<String, Object> offset = persistedOffset.get();
        return offset != null && String.valueOf(offset.get("offset.cursor")).matches("\\d+\\.\\d+");
    }

    /**
     * Polls the task until the condition holds, and collects the operation of the first event of every row
     * by its id. The offset of the last record is kept, as Kafka Connect would store it.
     */
    private void pollOperations(Map<String, String> operations, int attempts, BooleanSupplier done) throws InterruptedException {
        for (int i = 0; i < attempts && !done.getAsBoolean(); i++) {
            List<SourceRecord> records = task.poll();
            if (records != null) {
                for (SourceRecord r : records) {
                    if (r.sourceOffset() != null) {
                        persistedOffset.set(new HashMap<>(r.sourceOffset()));
                    }
                    if (r.value() instanceof Struct value && value.schema().field("op") != null && r.key() instanceof Struct key) {
                        operations.putIfAbsent(String.valueOf(key.get("id")), value.getString("op"));
                    }
                }
            }
            Thread.sleep(1000);
        }
    }

    /** Polls the task for up to the given number of seconds and returns the exception with which it fails. */
    private Throwable pollUntilFailure(int seconds) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(seconds);
        while (System.currentTimeMillis() < deadline) {
            final List<SourceRecord> records;
            try {
                records = task.poll();
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (Exception e) {
                return e;
            }
            // Records that are waiting are taken at once, so that what CockroachDB has buffered is read quickly.
            if (records == null || records.isEmpty()) {
                Thread.sleep(200);
            }
        }
        return null;
    }

    private static String messagesOf(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            messages.append(cause.getMessage()).append('\n');
        }
        return messages.toString();
    }

    private Connection adminConnection() throws SQLException {
        return DriverManager.getConnection(cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb"), cockroachdb.getUsername(),
                cockroachdb.getPassword());
    }

    private Map<String, String> sinklessConfig(String name, String database, String snapshotMode) {
        Map<String, String> config = new HashMap<>();
        config.put("name", name);
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", database);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", name);
        config.put("topic.prefix", name);
        config.put("cockroachdb.changefeed.sink.type", "sinkless");
        config.put("cockroachdb.changefeed.resolved.interval", "5s");
        config.put("snapshot.mode", snapshotMode);
        config.put("heartbeat.interval.ms", "1000");
        config.put("table.include.list", "public.expired_orders");
        config.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        return config;
    }

    private void startTask(Map<String, String> config) {
        task = new CockroachDBConnectorTask();
        task.initialize(contextReturningTheStoredOffset());
        task.start(config);
    }

    private void stopTask() {
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

    private SourceTaskContext contextReturningTheStoredOffset() {
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
                        Map<String, Object> offset = persistedOffset.get();
                        if (offset != null) {
                            for (Map<String, T> partition : partitions) {
                                result.put(partition, offset);
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
