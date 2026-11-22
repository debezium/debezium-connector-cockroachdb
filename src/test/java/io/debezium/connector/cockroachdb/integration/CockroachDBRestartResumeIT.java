/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
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
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
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

    private static final String DATABASE_NAME = "restartresume_testdb";
    private static final String TABLE_NAME = "restartresume_orders";

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(CockroachDBTestImage.NAME)
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

    @Test
    public void shouldCaptureTablesThatAreAddedToTheIncludeListAfterARestart() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");
        Map<String, String> config = baseConfig(hostBootstrap);
        config.put("name", "addedtable-test");
        config.put("database.server.name", "addedtable-test");
        config.put("topic.prefix", "addedtable-test");
        config.put("database.dbname", "addedtable_testdb");
        config.put("snapshot.mode", "initial");
        // The offset that the first run leaves behind must carry a resolved timestamp, which a heartbeat
        // record brings without waiting for another change.
        config.put("heartbeat.interval.ms", "1000");
        config.put("table.include.list", "public.added_orders");

        String adminUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb");
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS addedtable_testdb");
            stmt.execute("CREATE TABLE addedtable_testdb.public.added_orders (id INT PRIMARY KEY, name STRING)");
            stmt.execute("CREATE TABLE addedtable_testdb.public.added_extra (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO addedtable_testdb.public.added_orders VALUES (1, 'scanned by the first run')");
            stmt.execute("INSERT INTO addedtable_testdb.public.added_extra VALUES (1, 'before the first run')");
        }
        AtomicReference<Map<String, Object>> persistedOffset = new AtomicReference<>(null);

        // --- Run 1 captures one table. ---
        task1 = new CockroachDBConnectorTask();
        task1.initialize(contextReturning(persistedOffset));
        startTask(task1, config);
        Map<String, String> firstRun = new HashMap<>();
        pollOperations(task1, firstRun, persistedOffset, 120, () -> firstRun.containsKey("added_orders#1") && hasResolvedCursor(persistedOffset.get()));
        assertThat(firstRun).as("Run 1 should emit the row of its table").containsKey("added_orders#1");
        assertThat(hasResolvedCursor(persistedOffset.get())).as("Run 1 should leave an offset with a resolved timestamp").isTrue();
        task1.stop();
        task1 = null;

        // --- While the connector is stopped: a change in a table that existed all along, and a new table. ---
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            stmt.execute("INSERT INTO addedtable_testdb.public.added_extra VALUES (2, 'while the connector was stopped')");
            stmt.execute("CREATE TABLE addedtable_testdb.public.added_new (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO addedtable_testdb.public.added_new VALUES (1, 'while the connector was stopped')");
            stmt.execute("CREATE SCHEMA addedtable_testdb.later_schema");
            stmt.execute("CREATE TABLE addedtable_testdb.later_schema.added_in_schema (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO addedtable_testdb.later_schema.added_in_schema VALUES (1, 'while the connector was stopped')");
        }

        // --- Run 2 names both tables in addition. The changefeed of run 1 keeps running. ---
        config.put("table.include.list", "public.added_orders,public.added_extra,public.added_new,later_schema.added_in_schema");
        task2 = new CockroachDBConnectorTask();
        task2.initialize(contextReturning(persistedOffset));
        startTask(task2, config);

        Map<String, String> operations = new HashMap<>();
        pollOperations(task2, operations, persistedOffset, 120, () -> operations.containsKey("added_extra#2") && operations.containsKey("added_new#1")
                && operations.containsKey("added_in_schema#1"));
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            stmt.execute("INSERT INTO addedtable_testdb.public.added_new VALUES (2, 'after the restart')");
        }
        pollOperations(task2, operations, persistedOffset, 60, () -> operations.containsKey("added_new#2"));
        LOGGER.info("Run 2 emitted: {}", operations);

        // A table that existed at the stored cursor resumes there: it gets the change that it missed,
        // not the row from before.
        assertThat(operations).as("change of the added table since the stored cursor").containsEntry("added_extra#2", "c, snapshot=false");
        assertThat(operations).as("row of the added table from before the stored cursor").doesNotContainKey("added_extra#1");
        // A table that was created after the stored cursor has no history before it. With
        // snapshot.mode=initial it is scanned, like on a first start.
        assertThat(operations).as("row of the new table").containsEntry("added_new#1", "r, snapshot=true");
        // The same holds for a table whose schema was created after the stored cursor.
        assertThat(operations).as("row of the table in the new schema").containsEntry("added_in_schema#1", "r, snapshot=true");
        assertThat(operations).as("insert into the new table after the restart").containsEntry("added_new#2", "c, snapshot=false");
        // The table of run 1 is left to its running changefeed and is not read again.
        assertThat(operations).as("row that run 1 emitted").doesNotContainKey("added_orders#1");

        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement();
                var rs = stmt.executeQuery("SELECT count(*) FROM [SHOW CHANGEFEED JOBS] "
                        + "WHERE status = 'running' AND description LIKE '%topic_prefix=addedtable-test.''%'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).as("the changefeed of run 1, one for the table that existed at the cursor, and one for the tables that were created later")
                    .isEqualTo(3);
        }
    }

    @Test
    public void shouldCaptureAnAddedTableWhosePrivilegeIsNewerThanTheStoredCursor() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");
        Map<String, String> config = baseConfig(hostBootstrap);
        config.put("name", "latergrant-test");
        config.put("database.server.name", "latergrant-test");
        config.put("topic.prefix", "latergrant-test");
        config.put("database.dbname", "latergrant_testdb");
        // A user without the admin role, with the privileges that the documentation names.
        config.put("database.user", "latergrant_connector");
        config.put("database.password", "");
        config.put("heartbeat.interval.ms", "1000");
        config.put("table.include.list", "public.granted_first");

        String adminUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb");
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS latergrant_testdb");
            stmt.execute("CREATE TABLE latergrant_testdb.public.granted_first (id INT PRIMARY KEY, name STRING)");
            stmt.execute("CREATE TABLE latergrant_testdb.public.granted_later (id INT PRIMARY KEY, name STRING)");
            stmt.execute("CREATE USER IF NOT EXISTS latergrant_connector");
            stmt.execute("GRANT CHANGEFEED ON TABLE latergrant_testdb.public.granted_first TO latergrant_connector");
            stmt.execute("GRANT SYSTEM VIEWCLUSTERSETTING TO latergrant_connector");
            stmt.execute("GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO latergrant_connector");
        }
        AtomicReference<Map<String, Object>> persistedOffset = new AtomicReference<>(null);

        // --- Run 1 captures the table that the user may capture. ---
        task1 = new CockroachDBConnectorTask();
        task1.initialize(contextReturning(persistedOffset));
        startTask(task1, config);
        // The changefeed captures what happens after it has started, and nothing tells from the outside when that
        // is. So the inserts are repeated until one arrives.
        Map<String, String> firstRun = new HashMap<>();
        for (int id = 1; id <= 30 && !(firstRun.keySet().stream().anyMatch(key -> key.startsWith("granted_first#"))
                && hasResolvedCursor(persistedOffset.get())); id++) {
            try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                    Statement stmt = db.createStatement()) {
                stmt.execute("INSERT INTO latergrant_testdb.public.granted_first VALUES (" + id + ", 'in the first run')");
            }
            pollOperations(task1, firstRun, persistedOffset, 3, () -> false);
        }
        assertThat(firstRun.keySet()).as("Run 1 should emit an insert").anyMatch(key -> key.startsWith("granted_first#"));
        assertThat(hasResolvedCursor(persistedOffset.get())).as("Run 1 should leave an offset with a resolved timestamp").isTrue();
        final Map<String, Object> firstRunOffset = new HashMap<>(persistedOffset.get());
        task1.stop();
        task1 = null;

        // --- The usual order for another table: grant the privilege, add the table, restart. ---
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            stmt.execute("GRANT CHANGEFEED ON TABLE latergrant_testdb.public.granted_later TO latergrant_connector");
        }
        config.put("table.include.list", "public.granted_first,public.granted_later");
        task2 = new CockroachDBConnectorTask();
        task2.initialize(contextReturning(persistedOffset));
        startTask(task2, config);

        // CockroachDB checks the privilege as of the cursor of a changefeed, so it rejects the stored cursor for
        // the added table. Its changefeed starts at the current time instead. The inserts are repeated until one
        // arrives, because nothing tells from the outside when that changefeed has started.
        Map<String, String> operations = new HashMap<>();
        for (int id = 1; id <= 30 && operations.keySet().stream().noneMatch(key -> key.startsWith("granted_later#")); id++) {
            try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                    Statement stmt = db.createStatement()) {
                stmt.execute("INSERT INTO latergrant_testdb.public.granted_later VALUES (" + id + ", 'after the restart')");
            }
            pollOperations(task2, operations, persistedOffset, 3, () -> false);
        }
        LOGGER.info("Run 2 emitted: {}", operations);
        assertThat(operations.entrySet()).as("an insert into the added table")
                .anyMatch(entry -> entry.getKey().startsWith("granted_later#") && "c, snapshot=false".equals(entry.getValue()));

        // The changefeed of the added table started later than the stored cursor.
        final BigDecimal storedCursor = new BigDecimal(String.valueOf(firstRunOffset.get("offset.cursor")));
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            try (var rs = stmt.executeQuery("SELECT job_id, description FROM [SHOW CHANGEFEED JOBS] "
                    + "WHERE status = 'running' AND description LIKE '%granted_later%'")) {
                assertThat(rs.next()).as("a running changefeed for the added table").isTrue();
                final Matcher cursor = Pattern.compile("cursor = '([0-9.]+)'").matcher(rs.getString(2));
                assertThat(cursor.find()).as("the changefeed of the added table has a cursor: %s", rs.getString(2)).isTrue();
                assertThat(new BigDecimal(cursor.group(1))).as("it started later than the stored cursor, which CockroachDB rejected")
                        .isGreaterThan(storedCursor);
            }
        }
    }

    @Test
    public void shouldNotScanAnAddedTableThatTheUserCouldNotSeeAtTheStoredCursor() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");
        Map<String, String> config = baseConfig(hostBootstrap);
        config.put("name", "hidden-test");
        config.put("database.server.name", "hidden-test");
        config.put("topic.prefix", "hidden-test");
        config.put("database.dbname", "hidden_testdb");
        config.put("database.user", "hidden_connector");
        config.put("database.password", "");
        config.put("heartbeat.interval.ms", "1000");
        config.put("table.include.list", "public.seen_first");

        // Without CONNECT for public, the catalog shows a user only the tables on which it has a privilege.
        String adminUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb");
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS hidden_testdb");
            stmt.execute("REVOKE CONNECT ON DATABASE hidden_testdb FROM public");
            stmt.execute("CREATE TABLE hidden_testdb.public.seen_first (id INT PRIMARY KEY, name STRING)");
            stmt.execute("CREATE TABLE hidden_testdb.public.seen_later (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO hidden_testdb.public.seen_later VALUES (1, 'before the table was added')");
            stmt.execute("CREATE USER IF NOT EXISTS hidden_connector");
            stmt.execute("GRANT CHANGEFEED ON TABLE hidden_testdb.public.seen_first TO hidden_connector");
            stmt.execute("GRANT SYSTEM VIEWCLUSTERSETTING TO hidden_connector");
            stmt.execute("GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO hidden_connector");
        }
        AtomicReference<Map<String, Object>> persistedOffset = new AtomicReference<>(null);

        task1 = new CockroachDBConnectorTask();
        task1.initialize(contextReturning(persistedOffset));
        startTask(task1, config);
        Map<String, String> firstRun = new HashMap<>();
        for (int id = 1; id <= 30 && !(firstRun.keySet().stream().anyMatch(key -> key.startsWith("seen_first#"))
                && hasResolvedCursor(persistedOffset.get())); id++) {
            try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                    Statement stmt = db.createStatement()) {
                stmt.execute("INSERT INTO hidden_testdb.public.seen_first VALUES (" + id + ", 'in the first run')");
            }
            pollOperations(task1, firstRun, persistedOffset, 3, () -> false);
        }
        assertThat(hasResolvedCursor(persistedOffset.get())).as("Run 1 should leave an offset with a resolved timestamp").isTrue();
        task1.stop();
        task1 = null;

        // The table existed at the stored cursor, and the user could not see it then.
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            stmt.execute("GRANT CHANGEFEED ON TABLE hidden_testdb.public.seen_later TO hidden_connector");
        }
        config.put("table.include.list", "public.seen_first,public.seen_later");
        task2 = new CockroachDBConnectorTask();
        task2.initialize(contextReturning(persistedOffset));
        startTask(task2, config);

        Map<String, String> operations = new HashMap<>();
        for (int id = 2; id <= 31 && operations.keySet().stream().noneMatch(key -> key.startsWith("seen_later#") && !"seen_later#1".equals(key)); id++) {
            try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                    Statement stmt = db.createStatement()) {
                stmt.execute("INSERT INTO hidden_testdb.public.seen_later VALUES (" + id + ", 'after the restart')");
            }
            pollOperations(task2, operations, persistedOffset, 3, () -> false);
        }
        LOGGER.info("Run 2 emitted: {}", operations);
        assertThat(operations.entrySet()).as("an insert into the added table")
                .anyMatch(entry -> entry.getKey().startsWith("seen_later#") && "c, snapshot=false".equals(entry.getValue()));
        // snapshot.mode=no_data: a table that existed at the stored cursor is not scanned. Only a table that
        // was created after the cursor is, because all of its rows are newer than the cursor.
        assertThat(operations).as("the row from before the table was added").doesNotContainKey("seen_later#1");
    }

    @Test
    public void shouldKeepTheConfiguredCursorWhenThePrivilegeIsNewerThanIt() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");
        Map<String, String> config = baseConfig(hostBootstrap);
        config.put("name", "fixedcursor-test");
        config.put("database.server.name", "fixedcursor-test");
        config.put("topic.prefix", "fixedcursor-test");
        config.put("database.dbname", "fixedcursor_testdb");
        config.put("database.user", "fixedcursor_connector");
        config.put("database.password", "");
        config.put("table.include.list", "public.fixed_orders");

        String adminUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb");
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS fixedcursor_testdb");
            stmt.execute("CREATE TABLE fixedcursor_testdb.public.fixed_orders (id INT PRIMARY KEY, name STRING)");
            stmt.execute("CREATE USER IF NOT EXISTS fixedcursor_connector");
            stmt.execute("GRANT SYSTEM VIEWCLUSTERSETTING TO fixedcursor_connector");
            stmt.execute("GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO fixedcursor_connector");
            try (var rs = stmt.executeQuery("SELECT cluster_logical_timestamp()::STRING")) {
                assertThat(rs.next()).isTrue();
                // The changes since this time are what the configuration asks for.
                config.put("cockroachdb.changefeed.cursor", rs.getString(1));
            }
            stmt.execute("INSERT INTO fixedcursor_testdb.public.fixed_orders VALUES (1, 'after the configured cursor')");
            stmt.execute("GRANT CHANGEFEED ON TABLE fixedcursor_testdb.public.fixed_orders TO fixedcursor_connector");
        }

        // CockroachDB checks the privilege as of the cursor and rejects the changefeed. A start at the current
        // time instead would leave out the insert, without an error.
        task1 = new CockroachDBConnectorTask();
        task1.initialize(contextReturning(new AtomicReference<>(null)));
        startTask(task1, config);
        Map<String, String> operations = new HashMap<>();
        String failure = null;
        for (int id = 2; id <= 21 && failure == null; id++) {
            try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                    Statement stmt = db.createStatement()) {
                stmt.execute("INSERT INTO fixedcursor_testdb.public.fixed_orders VALUES (" + id + ", 'while the connector tries to start')");
            }
            try {
                pollOperations(task1, operations, new AtomicReference<>(null), 3, () -> false);
            }
            catch (ConnectException e) {
                failure = messagesOf(e);
            }
        }
        LOGGER.info("The connector emitted {} and failed with: {}", operations, failure);
        assertThat(operations).as("events from a changefeed that started at another time than the configured cursor").isEmpty();
        try (Connection db = DriverManager.getConnection(adminUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = db.createStatement();
                var rs = stmt.executeQuery("SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%fixed_orders%'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).as("changefeeds for the table").isZero();
        }
        assertThat(failure).as("the error of the task").isNotNull().contains("CHANGEFEED privilege");
    }

    private static String messagesOf(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            messages.append(cause.getMessage()).append('\n');
        }
        return messages.toString();
    }

    private static boolean hasResolvedCursor(Map<String, Object> offset) {
        return offset != null && String.valueOf(offset.get("offset.cursor")).matches("\\d+\\.\\d+");
    }

    /**
     * Polls the task until the condition holds, and collects the operation and the snapshot marker of the
     * first event of every row. The offset of the last record is kept in {@code persistedOffset}, as Kafka
     * Connect would store it.
     */
    private static void pollOperations(CockroachDBConnectorTask task, Map<String, String> operations,
                                       AtomicReference<Map<String, Object>> persistedOffset, int attempts, BooleanSupplier done)
            throws InterruptedException {
        for (int i = 0; i < attempts && !done.getAsBoolean(); i++) {
            for (SourceRecord r : poll(task)) {
                if (r.sourceOffset() != null) {
                    persistedOffset.set(new HashMap<>(r.sourceOffset()));
                }
                if (r.value() instanceof Struct value && value.schema().field("op") != null && r.key() instanceof Struct key) {
                    // CockroachDB can deliver an event again, so the first event of a row is the one that counts.
                    // The snapshot marker is left out of the source block of a record that is not a snapshot record.
                    final String snapshot = value.getStruct("source").getString("snapshot");
                    operations.putIfAbsent(r.topic().substring(r.topic().lastIndexOf('.') + 1) + "#" + key.get("id"),
                            value.getString("op") + ", snapshot=" + (snapshot == null ? "false" : snapshot));
                }
            }
            Thread.sleep(1000);
        }
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
