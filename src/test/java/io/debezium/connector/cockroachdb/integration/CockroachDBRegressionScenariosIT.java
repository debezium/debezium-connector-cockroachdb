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
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.cockroachdb.CockroachDBStreamingChangeEventSource;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * Regression scenarios reproduced from production reports, folded into one class so they share
 * the pipeline stack from {@link AbstractCockroachDBPipelineIT} instead of each booting their
 * own containers. Every scenario uses its own database and topic prefix.
 *
 * <ul>
 * <li>{@link #shouldEmitDecimalValuesWithSourcePrecision} (debezium/dbz#2256): DECIMAL values
 * above double precision must arrive with the exact source digits, not double-rounded.</li>
 * <li>{@link #shouldEmitDeleteEventWithKeyFromMessageKey} (debezium/dbz#2267): deletes carry
 * {@code after: null} and no {@code before}, so the record key must come from the changefeed
 * message key or key conversion fails on the required key schema.</li>
 * <li>{@link #shouldConvertBacklogEventsAfterAddingNotNullJsonbColumn} (debezium/dbz#2253):
 * backlog events written before a NOT NULL JSONB column was added must convert against the
 * refreshed schema, which requires JSONB fields to be optional.</li>
 * <li>{@link #shouldFailFastWhenReusedChangefeedLacksDiffOption} (debezium/dbz#2277): a
 * changefeed created without {@code diff} must not be silently reused when
 * {@code cockroachdb.changefeed.include.diff} is enabled.</li>
 * <li>{@link #shouldCreateChangefeedWithKafkaSinkConfig} (debezium/dbz#2278): the
 * {@code cockroachdb.changefeed.kafka.sink.config} JSON must reach CockroachDB as the
 * {@code kafka_sink_config} changefeed option.</li>
 * <li>DBZ-2390: generated changefeed SQL must quote a hyphenated schema and a reserved-word
 * table in both Kafka and sinkless delivery modes.</li>
 * <li>{@link #shouldMirrorConsumerPositionsToTheKafkaGroupWhenEnabled} (debezium/dbz#2472):
 * with {@code cockroachdb.changefeed.kafka.consumer.offset.commit.enabled} the connector
 * mirrors its consumed positions to the Kafka consumer group so external lag tooling works;
 * {@link #shouldNotCommitGroupOffsetsByDefault} guards that the default stays commit-free.</li>
 * <li>{@link #shouldKeepUpdatesAndDeletesThatArriveBeforeTheFirstResolvedTimestamp}
 * (debezium/dbz#3142): an update or a delete that arrives before the first resolved timestamp
 * must keep its operation and key instead of being emitted as a snapshot read.</li>
 * <li>{@link #shouldEmitRowsInsertedBeforeTheFirstResolvedTimestampAsCreates},
 * {@link #shouldCreateChangefeedsThatScanAtAnExplicitCursor} and
 * {@link #shouldProcessSignalsThatArriveBeforeTheFirstResolvedTimestamp}: a row
 * inserted after the initial scan must be emitted as a create even when it arrives before the first
 * resolved timestamp. Emitted as a snapshot read, a row of the signaling table is not acted on.
 * {@link #shouldEmitScanRowsThatArriveOutsideTheScanPhaseAsReads} covers the reverse case,
 * {@link #shouldTellScanRowsFromInsertedRowsAcrossSeveralChangefeeds} several changefeeds,
 * {@link #shouldUseTheCursorOfEveryReusedChangefeed} reused changefeeds that do not line up with the
 * connector's own grouping, and
 * {@link #shouldCreateChangefeedsWhenTheSessionDefaultsToReadCommitted} a session that cannot
 * evaluate the cluster timestamp in its default isolation level.</li>
 * <li>{@link #shouldDropEventsThatCockroachDbDeliversMoreThanOnce} and
 * {@link #shouldCreateEveryChangefeedWithTheUpdatedOption} (debezium/dbz#2927): an event that
 * CockroachDB delivers again has a new processing time but the same updated timestamp and row
 * images, and the connector must emit it only once.</li>
 * </ul>
 *
 * @author Virag Tripathi
 */
public class CockroachDBRegressionScenariosIT extends AbstractCockroachDBPipelineIT {

    private Connection connection;

    @AfterEach
    public void closeConnection() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        connection = null;
    }

    @Test
    public void shouldEmitDecimalValuesWithSourcePrecision() throws Exception {
        connection = openDatabase("decimal_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS decimal_events ("
                    + "id INT8 PRIMARY KEY, "
                    + "trade_dt_qty DECIMAL(28,18) NOT NULL DEFAULT 0.0, "
                    + "cost_basis DECIMAL(16,6) NOT NULL DEFAULT 0.0, "
                    + "seg_memo_qty DECIMAL(14,4) NOT NULL DEFAULT 0.0"
                    + ")");
            stmt.execute("INSERT INTO decimal_events (id, trade_dt_qty, cost_basis, seg_memo_qty) "
                    + "VALUES (1, 9999999999.999999999, 9999999999.999999, 9999.9999) "
                    + "ON CONFLICT (id) DO NOTHING");
        }

        startTask(baseConnectorConfig("decimal-test", "decimal_testdb", "public.decimal_events"));

        List<SourceRecord> records = pollForRecords(1, 45);
        assertThat(records).as("Should receive the inserted row").isNotEmpty();

        Struct value = (Struct) records.get(0).value();
        Struct after = value.getStruct("after");
        assertThat(after).isNotNull();

        // The emitted strings must carry the exact source digits. Compare numerically so a
        // representation with additional trailing zeros (the changefeed pads to the declared
        // scale) still passes, and reject any double-rounded value.
        assertThat(new BigDecimal(after.getString("trade_dt_qty")))
                .isEqualByComparingTo(new BigDecimal("9999999999.999999999"));
        assertThat(new BigDecimal(after.getString("cost_basis")))
                .isEqualByComparingTo(new BigDecimal("9999999999.999999"));
        assertThat(new BigDecimal(after.getString("seg_memo_qty")))
                .isEqualByComparingTo(new BigDecimal("9999.9999"));
    }

    @Test
    public void shouldEmitDeleteEventWithKeyFromMessageKey() throws Exception {
        connection = openDatabase("delete_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS delete_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO delete_events VALUES (42, 'to be deleted')");
        }

        startTask(baseConnectorConfig("delete-test", "delete_testdb", "public.delete_events"));

        List<SourceRecord> initial = pollForRecords(1, 45);
        assertThat(initial).as("Should receive the inserted row").isNotEmpty();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DELETE FROM delete_events WHERE id = 42");
        }

        List<SourceRecord> deletes = pollForRecords(this::isDelete, 1, 60);
        assertThat(deletes).as("Should receive the delete event").isNotEmpty();

        SourceRecord delete = deletes.get(0);
        assertThat(delete.key()).as("Delete record must carry its primary key").isNotNull();
        Struct key = (Struct) delete.key();
        assertThat(key.getInt64("id")).isEqualTo(42L);

        // The production failure point: key conversion with schemas enabled.
        try (JsonConverter jsonConverter = new JsonConverter()) {
            Map<String, Object> converterConfig = new HashMap<>();
            converterConfig.put("schemas.enable", "true");
            converterConfig.put("converter.type", "key");
            jsonConverter.configure(converterConfig);
            byte[] serializedKey = jsonConverter.fromConnectData(delete.topic(), delete.keySchema(), delete.key());
            assertThat(serializedKey).isNotEmpty();
        }
    }

    @Test
    public void shouldKeepUpdatesAndDeletesThatArriveBeforeTheFirstResolvedTimestamp() throws Exception {
        final Map<String, String> config = baseConnectorConfig("scan-window-test", "scan_window_testdb", "public.scan_window_events");
        connection = openDatabase("scan_window_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS scan_window_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO scan_window_events VALUES (1, 'to be updated'), (2, 'to be deleted')");
            // The connector reuses a running changefeed that has its topic prefix. One created without the
            // resolved option never sends a resolved timestamp, so every change made after the scan reaches
            // the connector before the first resolved timestamp. CockroachDB v26.4 opens the same window for
            // about 30 seconds on a changefeed over several tables that the connector creates itself.
            stmt.execute("CREATE CHANGEFEED FOR TABLE scan_window_testdb.public.scan_window_events INTO '"
                    + config.get("cockroachdb.changefeed.sink.uri") + "?topic_prefix=scan-window-test.' "
                    + "WITH full_table_name, envelope = 'enriched', enriched_properties = 'source,schema', initial_scan = 'yes'");
        }

        startTask(config);

        final List<SourceRecord> scanRows = pollForRecords(2, 45);
        assertThat(scanRows).as("Should receive both scan rows").hasSizeGreaterThanOrEqualTo(2);
        assertThat(scanRows).allSatisfy(row -> assertThat(((Struct) row.value()).getString("op")).isEqualTo("r"));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("UPDATE scan_window_events SET name = 'updated' WHERE id = 1");
            stmt.execute("DELETE FROM scan_window_events WHERE id = 2");
        }

        // The two changes touch different rows, so CockroachDB can deliver them in either order. It can
        // also deliver an event more than once, so only the first update and the first delete are kept.
        final Set<String> expected = new HashSet<>(Set.of("u", "d"));
        final List<SourceRecord> changes = pollForRecords(
                r -> r.value() != null && expected.remove(((Struct) r.value()).getString("op")), 2, 60);
        assertThat(changes).as("Should receive the update and the delete").hasSize(2);

        final SourceRecord update = changes.stream()
                .filter(r -> "u".equals(((Struct) r.value()).getString("op")))
                .findFirst().orElse(null);
        assertThat(update).as("An update must not be emitted as a snapshot read").isNotNull();
        final Struct updateValue = (Struct) update.value();
        assertThat(updateValue.getStruct("after").getString("name")).isEqualTo("updated");
        assertThat(((Struct) update.key()).getInt64("id")).isEqualTo(1L);
        assertThat(updateValue.getStruct("source").getString("snapshot")).as("An update is not a snapshot record").isIn(null, "false");

        final SourceRecord delete = changes.stream().filter(this::isDelete).findFirst().orElse(null);
        assertThat(delete).as("A delete must not be emitted as a snapshot read").isNotNull();
        assertThat(delete.key()).as("Delete record must carry its primary key").isNotNull();
        assertThat(((Struct) delete.key()).getInt64("id")).isEqualTo(2L);
        assertThat(((Struct) delete.value()).getStruct("source").getString("snapshot")).as("A delete is not a snapshot record").isIn(null, "false");

        // The production failure point: with the delete emitted as a read, the key was null.
        try (JsonConverter jsonConverter = new JsonConverter()) {
            final Map<String, Object> converterConfig = new HashMap<>();
            converterConfig.put("schemas.enable", "true");
            converterConfig.put("converter.type", "key");
            jsonConverter.configure(converterConfig);
            assertThat(jsonConverter.fromConnectData(delete.topic(), delete.keySchema(), delete.key())).isNotEmpty();
        }
    }

    @Test
    public void shouldEmitRowsInsertedBeforeTheFirstResolvedTimestampAsCreates() throws Exception {
        final Map<String, String> config = baseConnectorConfig("insert-window-test", "insert_window_testdb", "public.insert_window_events");
        connection = openDatabase("insert_window_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS insert_window_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO insert_window_events VALUES (1, 'scanned'), (2, 'scanned')");
            // As in the test above, a reused changefeed without the resolved option keeps the connector in
            // the scan phase. Its cursor is the timestamp of the scan. With the updated option every scan
            // row carries that timestamp and every row inserted afterwards carries a later one.
            final String cursor;
            try (ResultSet rs = stmt.executeQuery("SELECT cluster_logical_timestamp()::STRING")) {
                assertThat(rs.next()).isTrue();
                cursor = rs.getString(1);
            }
            stmt.execute("CREATE CHANGEFEED FOR TABLE insert_window_testdb.public.insert_window_events INTO '"
                    + config.get("cockroachdb.changefeed.sink.uri") + "?topic_prefix=insert-window-test.' "
                    + "WITH full_table_name, envelope = 'enriched', enriched_properties = 'source,schema', updated, "
                    + "initial_scan = 'yes', cursor = '" + cursor + "'");
        }

        startTask(config);

        final List<SourceRecord> scanRows = pollForRecords(2, 45);
        assertThat(scanRows).as("Should receive both scan rows").hasSizeGreaterThanOrEqualTo(2);
        assertThat(scanRows).allSatisfy(row -> {
            assertThat(((Struct) row.value()).getString("op")).isEqualTo("r");
            assertThat(((Struct) row.value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        });

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO insert_window_events VALUES (3, 'inserted')");
        }

        // CockroachDB can deliver a change more than once, so every record of the inserted row is checked.
        final List<SourceRecord> inserts = pollForRecords(r -> hasId(r, 3L), 1, 60);
        assertThat(inserts).as("Should receive the inserted row").isNotEmpty();
        assertThat(inserts).allSatisfy(record -> {
            final Struct insert = (Struct) record.value();
            assertThat(insert.getString("op")).as("A row inserted after the scan is a create").isEqualTo("c");
            assertThat(insert.getStruct("after").getString("name")).isEqualTo("inserted");
            assertThat(insert.getStruct("source").getString("snapshot")).as("A create is not a snapshot record").isIn(null, "false");
        });
    }

    @Test
    public void shouldProcessSignalsThatArriveBeforeTheFirstResolvedTimestamp() throws Exception {
        final Map<String, String> config = baseConnectorConfig("signal-window-test", "signal_window_testdb",
                "public.signal_window_products,public.debezium_signal");
        config.put("signal.data.collection", "signal_window_testdb.public.debezium_signal");
        connection = openDatabase("signal_window_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS signal_window_products (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("CREATE TABLE IF NOT EXISTS debezium_signal (id STRING PRIMARY KEY, type STRING NOT NULL, data STRING)");
            stmt.execute("UPSERT INTO signal_window_products VALUES (1, 'Widget'), (2, 'Gadget')");
            // As in the tests above, a reused changefeed without the resolved option keeps the connector
            // in the scan phase.
            final String cursor;
            try (ResultSet rs = stmt.executeQuery("SELECT cluster_logical_timestamp()::STRING")) {
                assertThat(rs.next()).isTrue();
                cursor = rs.getString(1);
            }
            stmt.execute("CREATE CHANGEFEED FOR TABLE signal_window_testdb.public.signal_window_products, "
                    + "signal_window_testdb.public.debezium_signal INTO '"
                    + config.get("cockroachdb.changefeed.sink.uri") + "?topic_prefix=signal-window-test.' "
                    + "WITH full_table_name, envelope = 'enriched', enriched_properties = 'source,schema', updated, "
                    + "initial_scan = 'yes', cursor = '" + cursor + "'");
        }

        startTask(config);

        final List<SourceRecord> scanRows = pollForRecords(2, 45);
        assertThat(scanRows).as("Should receive both scan rows").hasSizeGreaterThanOrEqualTo(2);
        assertThat(scanRows).allSatisfy(row -> {
            assertThat(((Struct) row.value()).getString("op")).isEqualTo("r");
            assertThat(((Struct) row.value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        });

        // The connector acts on a row of the signaling table only when the row arrives as a create.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO debezium_signal VALUES ('signal-window-1', 'execute-snapshot', "
                    + "'{\"data-collections\": [\"signal_window_testdb.public.signal_window_products\"]}')");
        }

        // The rows of the changefeed's scan are marked as snapshot records in the source block. A read
        // without that mark comes from the incremental snapshot.
        final List<SourceRecord> snapshotRows = pollForRecords(r -> r.topic().endsWith(".signal_window_products")
                && r.value() != null && "r".equals(((Struct) r.value()).getString("op"))
                && !"true".equals(((Struct) r.value()).getStruct("source").getString("snapshot")), 2, 60);
        assertThat(snapshotRows).as("The incremental snapshot that the signal asks for should read both rows")
                .extracting(r -> ((Struct) r.key()).getInt64("id"))
                .contains(1L, 2L);
    }

    @Test
    public void shouldEmitScanRowsThatArriveOutsideTheScanPhaseAsReads() throws Exception {
        final Map<String, String> config = baseConnectorConfig("late-scan-test", "late_scan_testdb", "public.late_scan_events");
        // With no_data the connector does not start a scan phase, so the scan rows of the reused changefeed
        // arrive outside of it.
        config.put("snapshot.mode", "no_data");
        connection = openDatabase("late_scan_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS late_scan_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO late_scan_events VALUES (1, 'scanned'), (2, 'scanned')");
            final String cursor;
            try (ResultSet rs = stmt.executeQuery("SELECT cluster_logical_timestamp()::STRING")) {
                assertThat(rs.next()).isTrue();
                cursor = rs.getString(1);
            }
            stmt.execute("CREATE CHANGEFEED FOR TABLE late_scan_testdb.public.late_scan_events INTO '"
                    + config.get("cockroachdb.changefeed.sink.uri") + "?topic_prefix=late-scan-test.' "
                    + "WITH full_table_name, envelope = 'enriched', enriched_properties = 'source,schema', updated, "
                    + "initial_scan = 'yes', cursor = '" + cursor + "'");
        }

        startTask(config);

        final List<SourceRecord> scanRows = pollForRecords(2, 45);
        assertThat(scanRows).as("Should receive both scan rows").hasSizeGreaterThanOrEqualTo(2);
        assertThat(scanRows).allSatisfy(row -> {
            assertThat(((Struct) row.value()).getString("op")).as("A scan row is a snapshot read").isEqualTo("r");
            assertThat(((Struct) row.value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        });

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO late_scan_events VALUES (3, 'inserted')");
        }

        final List<SourceRecord> inserts = pollForRecords(r -> hasId(r, 3L), 1, 60);
        assertThat(inserts).as("Should receive the inserted row").isNotEmpty();
        assertThat(inserts).allSatisfy(record -> {
            final Struct insert = (Struct) record.value();
            assertThat(insert.getString("op")).isEqualTo("c");
            assertThat(insert.getStruct("source").getString("snapshot")).as("A create is not a snapshot record").isIn(null, "false");
        });
    }

    @Test
    public void shouldCreateChangefeedsThatScanAtAnExplicitCursor() throws Exception {
        connection = openDatabase("scan_cursor_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS scan_cursor_orders (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("CREATE TABLE IF NOT EXISTS scan_cursor_items (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO scan_cursor_orders VALUES (1, 'scanned')");
            stmt.execute("UPSERT INTO scan_cursor_items VALUES (1, 'scanned')");
        }

        // CockroachDB v26.4 sends the first resolved timestamp of a changefeed over several tables only
        // with its first checkpoint, about 30 seconds after the scan.
        startTask(baseConnectorConfig("scan-cursor-test", "scan_cursor_testdb", "public.scan_cursor_orders,public.scan_cursor_items"));

        final List<SourceRecord> scanRows = pollForRecords(2, 45);
        assertThat(scanRows).as("Should receive the scan row of each table").hasSizeGreaterThanOrEqualTo(2);
        assertThat(scanRows).allSatisfy(row -> assertThat(((Struct) row.value()).getString("op")).isEqualTo("r"));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO scan_cursor_orders VALUES (2, 'inserted')");
            try (ResultSet rs = stmt.executeQuery("SELECT description FROM [SHOW CHANGEFEED JOBS] "
                    + "WHERE status = 'running' AND description LIKE '%topic_prefix=scan-cursor-test.%'")) {
                assertThat(rs.next()).as("The connector should have created a changefeed").isTrue();
                final String options = rs.getString(1).substring(rs.getString(1).indexOf("WITH OPTIONS ("));
                // The connector tells scan rows from rows inserted later by comparing the updated timestamp
                // of a row with the cursor of the changefeed.
                assertThat(options).contains("cursor = '", "initial_scan = 'yes'").containsPattern("[(,] ?updated[,)]");
            }
        }

        final List<SourceRecord> inserts = pollForRecords(r -> r.topic().endsWith(".scan_cursor_orders") && hasId(r, 2L), 1, 60);
        assertThat(inserts).as("Should receive the inserted row").isNotEmpty();
        assertThat(inserts).allSatisfy(record -> {
            final Struct insert = (Struct) record.value();
            assertThat(insert.getString("op")).as("A row inserted after the scan is a create").isEqualTo("c");
            assertThat(insert.getStruct("source").getString("snapshot")).as("A create is not a snapshot record").isIn(null, "false");
        });
    }

    @Test
    public void shouldTellScanRowsFromInsertedRowsAcrossSeveralChangefeeds() throws Exception {
        final Map<String, String> config = baseConnectorConfig("scan-groups-test", "scan_groups_testdb",
                "public.scan_groups_orders,public.scan_groups_items");
        // One changefeed per table. Each changefeed scans at its own cursor, and the first resolved
        // timestamp of either of them ends the scan phase for both.
        config.put("cockroachdb.changefeed.max.tables.per.changefeed", "1");
        connection = openDatabase("scan_groups_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS scan_groups_orders (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("CREATE TABLE IF NOT EXISTS scan_groups_items (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO scan_groups_orders VALUES (1, 'scanned'), (2, 'scanned')");
            stmt.execute("UPSERT INTO scan_groups_items VALUES (1, 'scanned'), (2, 'scanned')");
        }

        startTask(config);

        final List<SourceRecord> scanRows = pollForRecords(4, 45);
        assertThat(scanRows).as("Should receive the scan rows of both tables").hasSizeGreaterThanOrEqualTo(4);
        assertThat(scanRows).allSatisfy(row -> {
            assertThat(((Struct) row.value()).getString("op")).as("A scan row is a snapshot read").isEqualTo("r");
            assertThat(((Struct) row.value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        });

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO scan_groups_orders VALUES (3, 'inserted')");
            stmt.execute("INSERT INTO scan_groups_items VALUES (3, 'inserted')");
            try (ResultSet rs = stmt.executeQuery("SELECT count(*), count(DISTINCT substring(description FROM 'cursor = ''[0-9.]+''')) "
                    + "FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' AND description LIKE '%topic_prefix=scan-groups-test.%'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).as("The connector should have created one changefeed per table").isEqualTo(2);
                assertThat(rs.getInt(2)).as("Each changefeed has its own cursor").isEqualTo(2);
            }
        }

        final Set<String> tables = new HashSet<>();
        final List<SourceRecord> inserts = pollForRecords(r -> hasId(r, 3L) && tables.add(r.topic()), 2, 60);
        assertThat(inserts).as("Should receive the row inserted into each table").hasSize(2);
        assertThat(inserts).allSatisfy(record -> {
            final Struct insert = (Struct) record.value();
            assertThat(insert.getString("op")).as("A row inserted after the scan is a create").isEqualTo("c");
            assertThat(insert.getStruct("source").getString("snapshot")).as("A create is not a snapshot record").isIn(null, "false");
        });
    }

    @Test
    public void shouldDropEventsThatCockroachDbDeliversMoreThanOnce() throws Exception {
        final String topic = "redelivery-test.redelivery_testdb.public.redelivery_events";
        connection = openDatabase("redelivery_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS redelivery_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO redelivery_events VALUES (1, 'scanned')");
        }

        startTask(baseConnectorConfig("redelivery-test", "redelivery_testdb", "public.redelivery_events"));
        assertThat(pollForRecords(1, 45)).as("Should receive the scan row").isNotEmpty();
        final String jobId = runningChangefeedJobFor("redelivery_events");

        // After PAUSE JOB and RESUME JOB a changefeed emits again what it emitted since its last
        // checkpoint. A checkpoint can fall between the insert and the pause, so the sequence is repeated
        // with a new row until CockroachDB has written one of the inserts to the intermediate topic twice.
        final List<SourceRecord> emitted = new ArrayList<>();
        long redelivered = -1;
        for (long id = 100; id < 106 && redelivered < 0; id++) {
            final long inserted = id;
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO redelivery_events VALUES (" + inserted + ", 'inserted')");
            }
            emitted.addAll(pollForRecords(r -> hasId(r, inserted), 1, 60));
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("PAUSE JOB " + jobId);
            }
            waitForChangefeedJobStatus(jobId, "paused");
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("RESUME JOB " + jobId);
            }
            waitForChangefeedJobStatus(jobId, "running");
            for (int i = 0; i < 20 && redelivered < 0; i++) {
                if (copiesInTopic(topic, "\"after\": {\"id\": " + inserted + ",") >= 2) {
                    redelivered = inserted;
                }
                else {
                    Thread.sleep(1000);
                }
            }
        }
        assertThat(redelivered).as("CockroachDB should have written one of the inserts to the intermediate topic twice").isPositive();

        // The connector has emitted the insert already. A row that is written after the second copy tells
        // when the connector has read that copy.
        final long row = redelivered;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO redelivery_events VALUES (999, 'written after the copy')");
        }
        final List<SourceRecord> written = pollForRecords(r -> {
            if (hasId(r, row)) {
                emitted.add(r);
            }
            return hasId(r, 999L);
        }, 1, 60);
        assertThat(written).as("Should receive the row that was written after the copy").isNotEmpty();
        assertThat(emitted.stream().filter(r -> hasId(r, row))).as("The connector should emit the insert once").hasSize(1);
    }

    @Test
    public void shouldEmitTheChangesOfARowInTheirOrderWhenCockroachDbDeliversThemAgain() throws Exception {
        final String topic = "reorder-test.reorder_testdb.public.reorder_events";
        final Map<String, String> config = baseConnectorConfig("reorder-test", "reorder_testdb", "public.reorder_events,public.reorder_filler");
        config.put("snapshot.mode", "no_data");
        connection = openDatabase("reorder_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS reorder_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("CREATE TABLE IF NOT EXISTS reorder_filler (id INT8 PRIMARY KEY)");
            // When a changefeed is paused and resumed, it emits again what it emitted since its last
            // checkpoint. This one writes a checkpoint soon after it starts and then only every ten minutes.
            stmt.execute("CREATE CHANGEFEED FOR TABLE reorder_testdb.public.reorder_events, reorder_testdb.public.reorder_filler INTO '"
                    + config.get("cockroachdb.changefeed.sink.uri") + "?topic_prefix=reorder-test.' "
                    + "WITH full_table_name, envelope = 'enriched', enriched_properties = 'source,schema', updated, "
                    + "initial_scan = 'no', min_checkpoint_frequency = '10m'");
        }

        startTask(config);
        final String jobId = runningChangefeedJobFor("reorder_events");
        waitForACheckpoint(jobId);

        // The row changes twice, and between the two changes more events pass than the connector remembers.
        final List<SourceRecord> changes = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO reorder_events VALUES (1, 'older')");
        }
        changes.addAll(pollForRecords(r -> isReorderEvent(r, 1L), 1, 60));
        assertThat(changes).as("Should receive the insert").isNotEmpty();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO reorder_filler SELECT generate_series(1001, 11050)");
        }
        // CockroachDB can deliver events again on its own at any time, so the test counts rows, not records,
        // and waits for the update instead of taking the next record of the row for it.
        final Set<Long> otherRows = new HashSet<>();
        pollForRecords(r -> {
            if (r.topic().endsWith(".reorder_filler") && r.key() instanceof Struct) {
                otherRows.add(((Struct) r.key()).getInt64("id"));
            }
            return otherRows.size() >= 10_050;
        }, 1, 180);
        assertThat(otherRows).as("Should receive the rows of the other table").hasSize(10_050);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("UPDATE reorder_events SET name = 'newer' WHERE id = 1");
        }
        final List<SourceRecord> updates = pollForRecords(r -> {
            if (isReorderEvent(r, 1L)) {
                changes.add(r);
            }
            return isReorderEvent(r, 1L) && "newer".equals(nameOf(r));
        }, 1, 60);
        assertThat(updates).as("Should receive the update").isNotEmpty();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("PAUSE JOB " + jobId);
        }
        waitForChangefeedJobStatus(jobId, "paused");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("RESUME JOB " + jobId);
        }
        waitForChangefeedJobStatus(jobId, "running");
        int copies = 0;
        for (int i = 0; i < 60 && copies < 2; i++) {
            copies = copiesInTopic(topic, "\"name\": \"newer\"");
            if (copies < 2) {
                Thread.sleep(1000);
            }
        }
        assertThat(copies).as("CockroachDB should have written the update to the intermediate topic twice").isGreaterThanOrEqualTo(2);
        assertThat(copiesInTopic(topic, "\"name\": \"older\"")).as("and the insert as well").isGreaterThanOrEqualTo(2);

        // A row that is written after the copies tells when the connector has read them.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO reorder_events VALUES (2, 'written after the copies')");
        }
        final List<SourceRecord> written = pollForRecords(r -> {
            if (isReorderEvent(r, 1L)) {
                changes.add(r);
            }
            return isReorderEvent(r, 2L);
        }, 1, 120);
        assertThat(written).as("Should receive the row that was written after the copies").isNotEmpty();

        // Whatever the connector emitted again, the last event of the row must be its last change.
        assertThat(nameOfTheLastChange(changes)).as("The row must not end on its older version").isEqualTo("newer");
    }

    private static boolean isReorderEvent(SourceRecord record, long id) {
        return record.topic().endsWith(".reorder_events") && hasId(record, id);
    }

    private void waitForACheckpoint(String jobId) throws Exception {
        for (int i = 0; i < 90; i++) {
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT high_water_timestamp FROM [SHOW CHANGEFEED JOB " + jobId + "]")) {
                if (rs.next() && rs.getString(1) != null) {
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Changefeed job " + jobId + " wrote no checkpoint");
    }

    private static String nameOfTheLastChange(List<SourceRecord> changes) {
        return nameOf(changes.get(changes.size() - 1));
    }

    private static String nameOf(SourceRecord record) {
        final Struct after = ((Struct) record.value()).getStruct("after");
        return after == null ? null : after.getString("name");
    }

    @Test
    public void shouldCreateEveryChangefeedWithTheUpdatedOption() throws Exception {
        final Map<String, String> config = baseConnectorConfig("updated-option-test", "updated_option_testdb", "public.updated_option_events");
        // With no_data the changefeed has neither an initial scan nor a cursor.
        config.put("snapshot.mode", "no_data");
        connection = openDatabase("updated_option_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS updated_option_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
        }

        startTask(config);

        String options = null;
        for (int i = 0; i < 30 && options == null; i++) {
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT description FROM [SHOW CHANGEFEED JOBS] "
                            + "WHERE status = 'running' AND description LIKE '%topic_prefix=updated-option-test.%'")) {
                if (rs.next()) {
                    options = rs.getString(1).substring(rs.getString(1).indexOf("WITH OPTIONS ("));
                }
            }
            if (options == null) {
                Thread.sleep(1000);
            }
        }
        assertThat(options).as("The connector should have created a changefeed").isNotNull();
        assertThat(options).contains("initial_scan = 'no'").containsPattern("[(,] ?updated[,)]");

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO updated_option_events VALUES (1, 'inserted')");
        }
        final List<SourceRecord> inserts = pollForRecords(r -> hasId(r, 1L), 1, 60);
        assertThat(inserts).as("Should receive the inserted row").isNotEmpty();
    }

    @Test
    public void shouldUseTheCursorOfEveryReusedChangefeed() throws Exception {
        // The connector puts both tables into one changefeed, but finds one running changefeed per table,
        // for example after cockroachdb.changefeed.max.tables.per.changefeed was raised. Neither of them
        // sends resolved timestamps, so the connector stays in the scan phase.
        final Map<String, String> config = baseConnectorConfig("reused-cursors-test", "reused_cursors_testdb",
                "public.reused_cursors_orders,public.reused_cursors_items");
        connection = openDatabase("reused_cursors_testdb");
        try (Statement stmt = connection.createStatement()) {
            for (String table : List.of("reused_cursors_orders", "reused_cursors_items")) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + table + " (id INT8 PRIMARY KEY, name STRING NOT NULL)");
                stmt.execute("UPSERT INTO " + table + " VALUES (1, 'scanned')");
                final String cursor;
                try (ResultSet rs = stmt.executeQuery("SELECT cluster_logical_timestamp()::STRING")) {
                    assertThat(rs.next()).isTrue();
                    cursor = rs.getString(1);
                }
                stmt.execute("CREATE CHANGEFEED FOR TABLE reused_cursors_testdb.public." + table + " INTO '"
                        + config.get("cockroachdb.changefeed.sink.uri") + "?topic_prefix=reused-cursors-test.' "
                        + "WITH full_table_name, envelope = 'enriched', enriched_properties = 'source,schema', updated, "
                        + "initial_scan = 'yes', cursor = '" + cursor + "'");
            }
        }

        startTask(config);

        final List<SourceRecord> scanRows = pollForRecords(2, 45);
        assertThat(scanRows).as("Should receive the scan row of each table").hasSizeGreaterThanOrEqualTo(2);
        assertThat(scanRows).allSatisfy(row -> assertThat(((Struct) row.value()).getString("op")).isEqualTo("r"));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO reused_cursors_orders VALUES (2, 'inserted')");
            stmt.execute("INSERT INTO reused_cursors_items VALUES (2, 'inserted')");
        }

        final Set<String> tables = new HashSet<>();
        final List<SourceRecord> inserts = pollForRecords(r -> hasId(r, 2L) && tables.add(r.topic()), 2, 60);
        assertThat(inserts).as("Should receive the row inserted into each table").hasSize(2);
        assertThat(inserts).allSatisfy(record -> assertThat(((Struct) record.value()).getString("op"))
                .as("A row inserted after the scan is a create in " + record.topic()).isEqualTo("c"));
    }

    @Test
    public void shouldCreateChangefeedsWhenTheSessionDefaultsToReadCommitted() throws Exception {
        connection = openDatabase("read_committed_testdb");
        try (Statement stmt = connection.createStatement()) {
            // CockroachDB evaluates cluster_logical_timestamp(), which gives the changefeed its cursor, only
            // in a SERIALIZABLE transaction. Role defaults do not apply to root, so the connector gets a
            // user of its own.
            stmt.execute("CREATE USER IF NOT EXISTS read_committed_connector");
            stmt.execute("GRANT admin TO read_committed_connector");
            stmt.execute("ALTER ROLE read_committed_connector SET default_transaction_isolation = 'read committed'");
            stmt.execute("CREATE TABLE IF NOT EXISTS read_committed_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO read_committed_events VALUES (1, 'scanned')");
        }
        try (Connection session = DriverManager.getConnection(
                cockroachdb.getJdbcUrl().replace("/postgres", "/read_committed_testdb"), "read_committed_connector", "");
                Statement stmt = session.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW transaction_isolation")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).as("A session of the connector's user should default to READ COMMITTED").isEqualTo("read committed");
        }

        final Map<String, String> config = baseConnectorConfig("read-committed-test", "read_committed_testdb", "public.read_committed_events");
        config.put("database.user", "read_committed_connector");
        config.put("database.password", "");
        startTask(config);

        final List<SourceRecord> scanRows = pollForRecords(1, 45);
        assertThat(scanRows).as("Should receive the scan row").isNotEmpty();
        assertThat(scanRows).allSatisfy(row -> assertThat(((Struct) row.value()).getString("op")).isEqualTo("r"));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO read_committed_events VALUES (2, 'inserted')");
        }

        final List<SourceRecord> inserts = pollForRecords(r -> hasId(r, 2L), 1, 60);
        assertThat(inserts).as("Should receive the inserted row").isNotEmpty();
        assertThat(inserts).allSatisfy(record -> assertThat(((Struct) record.value()).getString("op")).isEqualTo("c"));
    }

    @Test
    public void shouldConvertBacklogEventsAfterAddingNotNullJsonbColumn() throws Exception {
        connection = openDatabase("jsonb_backlog_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS jsonb_events (id INT PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO jsonb_events VALUES (1, 'Alice')");
        }

        Map<String, String> config = baseConnectorConfig("jsonb-backlog", "jsonb_backlog_testdb", "public.jsonb_events");

        // First task run: creates the changefeed and intermediate topic; the event for row 1 is
        // written under the original two-column schema.
        startTask(config);
        List<SourceRecord> initialRecords = pollForRecords(1, 30);
        assertThat(initialRecords).as("Should receive the pre-DDL row").isNotEmpty();
        stopTask();

        // Row 2 lands in the intermediate topic under the old schema while no task is consuming.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("UPSERT INTO jsonb_events VALUES (2, 'Bob')");
            stmt.execute("ALTER TABLE jsonb_events ADD COLUMN IF NOT EXISTS doc JSONB NOT NULL DEFAULT '{}'::JSONB");
        }

        // Second task run registers the three-column schema and replays the backlog from the
        // earliest offset, converting the pre-DDL events against the new schema.
        startTask(config);
        List<SourceRecord> replayed = pollForRecords(2, 60);
        assertThat(replayed).as("Should replay backlog events after restart").isNotEmpty();

        boolean sawNullDoc = false;
        try (JsonConverter jsonConverter = new JsonConverter()) {
            Map<String, Object> converterConfig = new HashMap<>();
            converterConfig.put("schemas.enable", "true");
            converterConfig.put("converter.type", "value");
            jsonConverter.configure(converterConfig);

            for (SourceRecord record : replayed) {
                Struct value = (Struct) record.value();
                Struct after = value.getStruct("after");
                if (after != null) {
                    Field docField = after.schema().field("doc");
                    if (docField != null) {
                        assertThat(docField.schema().isOptional())
                                .as("doc field schema must be optional")
                                .isTrue();
                        if (after.getWithoutDefault("doc") == null) {
                            sawNullDoc = true;
                        }
                    }
                }
                // The incident failure point: serializing the full envelope with schemas enabled.
                byte[] serialized = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
                assertThat(serialized).isNotEmpty();
            }
        }

        LOGGER.info("Replayed {} records, sawNullDoc={}", replayed.size(), sawNullDoc);
        assertThat(sawNullDoc)
                .as("At least one replayed pre-DDL event should carry null for the added NOT NULL JSONB column")
                .isTrue();
    }

    @Test
    public void shouldFailFastWhenReusedChangefeedLacksDiffOption() throws Exception {
        connection = openDatabase("reuse_diff_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS reuse_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO reuse_events VALUES (1, 'Alice')");
        }

        // First run creates the changefeed without the diff option.
        startTask(baseConnectorConfig("reuse-test", "reuse_diff_testdb", "public.reuse_events"));
        assertThat(pollUntilRecordsOrError(30).records).as("First run should stream the seed row").isNotEmpty();
        stopTask();

        // Second run enables include.diff; the connector finds the existing no-diff changefeed
        // and must refuse to reuse it.
        Map<String, String> diffConfig = baseConnectorConfig("reuse-test", "reuse_diff_testdb", "public.reuse_events");
        diffConfig.put("cockroachdb.changefeed.include.diff", "true");
        startTask(diffConfig);
        PollOutcome outcome = pollUntilRecordsOrError(45);
        assertThat(outcome.error)
                .as("Second run should fail instead of silently reusing the no-diff changefeed")
                .isNotNull();
        assertThat(outcome.error.getMessage() + " " + rootCauseMessage(outcome.error))
                .contains("diff");
    }

    @Test
    public void shouldFailFastWhenMatchingChangefeedIsPaused() throws Exception {
        connection = openDatabase("reuse_paused_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS paused_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO paused_events VALUES (1, 'Alice')");
        }

        // First run creates the changefeed.
        startTask(baseConnectorConfig("paused-test", "reuse_paused_testdb", "public.paused_events"));
        assertThat(pollUntilRecordsOrError(30).records).as("First run should stream the seed row").isNotEmpty();
        stopTask();

        // Pause the job, as an operator doing maintenance would. The job is now invisible to a
        // running-only reuse check; without the guard the connector creates an identical twin
        // that double-produces once the paused job resumes (debezium/dbz#2462).
        String jobId = null;
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' AND description LIKE '%paused_events%'")) {
            if (rs.next()) {
                jobId = rs.getString(1);
            }
        }
        assertThat(jobId).as("The first run's changefeed job must exist").isNotNull();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("PAUSE JOB " + jobId);
        }
        // PAUSE is asynchronous: wait for the job to leave the running state.
        for (int i = 0; i < 30; i++) {
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT status FROM [SHOW CHANGEFEED JOBS] WHERE job_id = " + jobId)) {
                if (rs.next() && "paused".equals(rs.getString(1))) {
                    break;
                }
            }
            Thread.sleep(1000);
        }

        // Second run must fail fast instead of creating a duplicate changefeed.
        startTask(baseConnectorConfig("paused-test", "reuse_paused_testdb", "public.paused_events"));
        PollOutcome outcome = pollUntilRecordsOrError(45);
        assertThat(outcome.error)
                .as("Startup with a matching paused changefeed should fail fast, not create a twin")
                .isNotNull();
        assertThat(outcome.error.getMessage() + " " + rootCauseMessage(outcome.error))
                .contains("paused");

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status IN ('running', 'paused') AND description LIKE '%paused_events%'")) {
            rs.next();
            assertThat(rs.getInt(1)).as("No duplicate changefeed job may be created").isEqualTo(1);
        }
    }

    @Test
    public void shouldMirrorConsumerPositionsToTheKafkaGroupWhenEnabled() throws Exception {
        connection = openDatabase("offsetmirror_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS mirror_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO mirror_events VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
        }

        Map<String, String> config = baseConnectorConfig("offsetmirror-test", "offsetmirror_testdb", "public.mirror_events");
        config.put("cockroachdb.changefeed.kafka.consumer.group.prefix", "offsetmirror-group");
        config.put("cockroachdb.changefeed.kafka.consumer.offset.commit.enabled", "true");
        startTask(config);

        List<SourceRecord> records = pollForRecords(3, 45);
        assertThat(records).as("Should receive the seed rows").hasSizeGreaterThanOrEqualTo(3);

        // The mirror commit is asynchronous and only exists for external lag tooling, so poll the
        // group offsets the way kafka-consumer-groups would until they appear.
        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        for (int i = 0; i < 30 && committed.isEmpty(); i++) {
            committed = committedGroupOffsets("offsetmirror-group");
            if (committed.isEmpty()) {
                task.poll();
                Thread.sleep(1000);
            }
        }

        assertThat(committed)
                .as("The consumer group should carry committed offsets for the intermediate topic")
                .isNotEmpty();
        assertThat(committed.keySet())
                .anyMatch(tp -> tp.topic().contains("mirror_events"));
        long mirroredPositions = committed.values().stream().mapToLong(OffsetAndMetadata::offset).sum();
        assertThat(mirroredPositions)
                .as("The mirrored position should cover the consumed events")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    public void shouldNotCommitGroupOffsetsByDefault() throws Exception {
        connection = openDatabase("offsetdefault_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS default_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO default_events VALUES (1, 'Alice')");
        }

        Map<String, String> config = baseConnectorConfig("offsetdefault-test", "offsetdefault_testdb", "public.default_events");
        config.put("cockroachdb.changefeed.kafka.consumer.group.prefix", "offsetdefault-group");
        startTask(config);

        List<SourceRecord> records = pollForRecords(1, 45);
        assertThat(records).as("Should receive the seed row").isNotEmpty();

        // Give the consumer a few more poll cycles; without the option no commit may ever happen.
        for (int i = 0; i < 3; i++) {
            task.poll();
            Thread.sleep(1000);
        }

        assertThat(committedGroupOffsets("offsetdefault-group"))
                .as("Without the option the connector must never commit group offsets")
                .isEmpty();
    }

    private Map<TopicPartition, OffsetAndMetadata> committedGroupOffsets(String groupId) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", ""));
        try (Admin admin = Admin.create(props)) {
            return admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    public void shouldCreateChangefeedWithKafkaSinkConfig() throws Exception {
        connection = openDatabase("sinkcfg_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS sinkcfg_events (id INT8 PRIMARY KEY, name STRING NOT NULL)");
            stmt.execute("UPSERT INTO sinkcfg_events VALUES (1, 'seed')");
        }

        Map<String, String> config = baseConnectorConfig("sinkcfg-test", "sinkcfg_testdb", "public.sinkcfg_events");
        config.put("cockroachdb.changefeed.kafka.sink.config", "{\"Flush\": {\"Messages\": 100, \"Frequency\": \"500ms\"}}");
        startTask(config);

        List<SourceRecord> initial = pollForRecords(1, 45);
        assertThat(initial).as("Should receive the seed row").isNotEmpty();

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT description FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'")) {
            boolean found = false;
            while (rs.next()) {
                String description = rs.getString(1);
                if (description != null && description.contains("kafka_sink_config")) {
                    LOGGER.info("Changefeed description: {}", description);
                    found = true;
                }
            }
            assertThat(found)
                    .as("The running changefeed must carry the kafka_sink_config option")
                    .isTrue();
        }
    }

    @Test
    public void shouldStreamQuotedIdentifiersInKafkaMode() throws Exception {
        connection = openDatabase("quoted_kafka_testdb");
        createQuotedIdentifierTable(connection);

        Map<String, String> config = baseConnectorConfig(
                "quoted-kafka-test", "quoted_kafka_testdb", "stock-trading\\.order");
        startTask(config);

        List<SourceRecord> records = pollForRecords(1, 45);
        assertThat(records).as("Kafka mode should stream the table whose identifiers require quoting").isNotEmpty();
        assertThat(((Struct) records.get(0).value()).getStruct("after").getInt64("id")).isEqualTo(1L);
    }

    @Test
    public void shouldStreamQuotedIdentifiersInSinklessMode() throws Exception {
        connection = openDatabase("quoted_sinkless_testdb");
        createQuotedIdentifierTable(connection);

        Map<String, String> config = baseConnectorConfig(
                "quoted-sinkless-test", "quoted_sinkless_testdb", "stock-trading\\.order");
        config.put("cockroachdb.changefeed.sink.type", "sinkless");
        config.remove("cockroachdb.changefeed.sink.uri");
        config.remove("cockroachdb.changefeed.kafka.bootstrap.servers");
        config.remove("cockroachdb.changefeed.kafka.auto.offset.reset");
        config.remove("cockroachdb.changefeed.kafka.poll.timeout.ms");
        startTask(config);

        List<SourceRecord> records = pollForRecords(1, 45);
        assertThat(records).as("Sinkless mode should stream the table whose identifiers require quoting").isNotEmpty();
        assertThat(((Struct) records.get(0).value()).getStruct("after").getInt64("id")).isEqualTo(1L);
    }

    private static void createQuotedIdentifierTable(Connection connection) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS \"stock-trading\"");
            stmt.execute("CREATE TABLE IF NOT EXISTS \"stock-trading\".\"order\" "
                    + "(id INT8 PRIMARY KEY, qty INT8 NOT NULL)");
            stmt.execute("UPSERT INTO \"stock-trading\".\"order\" VALUES (1, 10)");
        }
    }

    @Test
    public void shouldCarrySchemaAndTableInTheSourceBlock() throws Exception {
        connection = openDatabase("srcblock_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS accounts (id INT8 PRIMARY KEY, balance INT8 NOT NULL)");
            stmt.execute("UPSERT INTO accounts VALUES (1, 100)");
        }

        Map<String, String> config = baseConnectorConfig(
                "srcblock-test", "srcblock_testdb", "public\\.accounts");
        startTask(config);

        List<SourceRecord> records = pollForRecords(1, 45);
        assertThat(records).isNotEmpty();
        Struct source = ((Struct) records.get(0).value()).getStruct("source");
        assertThat(source.getString("schema"))
                .as("source.schema enables JDBC sink routing via ${source.schema} (debezium/dbz#2432)")
                .isEqualTo("public");
        assertThat(source.getString("table")).isEqualTo("accounts");
    }

    private String runningChangefeedJobFor(String tableName) throws Exception {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' AND description LIKE '%" + tableName + "%'")) {
            assertThat(rs.next()).as("A running changefeed job for " + tableName + " must exist").isTrue();
            return rs.getString(1);
        }
    }

    private void waitForChangefeedJobStatus(String jobId, String status) throws Exception {
        for (int i = 0; i < 30; i++) {
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT status FROM [SHOW CHANGEFEED JOBS] WHERE job_id = " + jobId)) {
                if (rs.next() && status.equals(rs.getString(1))) {
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("Changefeed job " + jobId + " did not reach status " + status);
    }

    /** Counts the messages of an intermediate topic whose value contains the given text. */
    private static int copiesInTopic(String topic, String text) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", ""));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            final List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .collect(Collectors.toList());
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            final Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            int copies = 0;
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value() != null && record.value().contains(text)) {
                        copies++;
                    }
                }
            }
            return copies;
        }
    }

    private static boolean hasId(SourceRecord record, long id) {
        return record.value() != null && record.key() instanceof Struct && Long.valueOf(id).equals(((Struct) record.key()).getInt64("id"));
    }

    private boolean isDelete(SourceRecord record) {
        if (record.value() == null) {
            return false;
        }
        Struct value = (Struct) record.value();
        return value.schema().field("op") != null && "d".equals(value.getString("op"));
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null) {
            cur = cur.getCause();
        }
        return cur.getMessage() == null ? "" : cur.getMessage();
    }

    private static final class PollOutcome {
        final List<SourceRecord> records = new ArrayList<>();
        Throwable error;
    }

    private PollOutcome pollUntilRecordsOrError(int attempts) throws InterruptedException {
        PollOutcome outcome = new PollOutcome();
        for (int i = 0; i < attempts; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null) {
                    for (SourceRecord r : records) {
                        if (r.topic() != null && !r.topic().contains("__debezium-heartbeat")) {
                            outcome.records.add(r);
                        }
                    }
                }
            }
            catch (Throwable t) {
                outcome.error = t;
                return outcome;
            }
            if (!outcome.records.isEmpty()) {
                return outcome;
            }
            Thread.sleep(1000);
        }
        return outcome;
    }

    @Test
    public void shouldWarnWhenReusedChangefeedCapturesRemovedTables() throws Exception {
        connection = openDatabase("reuse_extra_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS keep_t (id INT8 PRIMARY KEY, v STRING)");
            stmt.execute("CREATE TABLE IF NOT EXISTS drop_t (id INT8 PRIMARY KEY, v STRING)");
            stmt.execute("UPSERT INTO keep_t VALUES (1, 'a')");
            stmt.execute("UPSERT INTO drop_t VALUES (1, 'a')");
        }

        // First run creates one changefeed covering both tables.
        startTask(baseConnectorConfig("reuse-extra-test", "reuse_extra_testdb", "public.keep_t,public.drop_t"));
        assertThat(pollForRecords(1, 45)).as("First run should stream").isNotEmpty();
        stopTask();

        // Second run drops drop_t from the include list; the connector reuses the existing
        // job and must name the table the job still captures (debezium/dbz#2319).
        Logger sourceLogger = (Logger) org.slf4j.LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        ListAppender<ILoggingEvent> warnings = new ListAppender<>();
        warnings.start();
        sourceLogger.addAppender(warnings);
        try {
            startTask(baseConnectorConfig("reuse-extra-test", "reuse_extra_testdb", "public.keep_t"));
            assertThat(pollForRecords(0, 5)).isNotNull();
            assertThat(warnings.list)
                    .as("Reuse with a shrunk include list must warn about the extra captured table")
                    .anyMatch(e -> "WARN".equals(e.getLevel().toString())
                            && e.getFormattedMessage().contains("drop_t")
                            && e.getFormattedMessage().contains("CANCEL JOB"));
        }
        finally {
            sourceLogger.detachAppender(warnings);
        }
    }

    @Test
    public void shouldNotTriggerSchemaRefreshForDeleteEvents() throws Exception {
        connection = openDatabase("delete_drift_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS dd_events (id INT8 PRIMARY KEY, v STRING NOT NULL)");
            stmt.execute("UPSERT INTO dd_events VALUES (7, 'x')");
        }

        // A delete without a diff image carries after: null; the drift check must not read
        // that as missing columns and refresh the schema per delete (debezium/dbz#2322).
        Logger sourceLogger = (Logger) org.slf4j.LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        sourceLogger.addAppender(logs);
        try {
            startTask(baseConnectorConfig("delete-drift-test", "delete_drift_testdb", "public.dd_events"));
            assertThat(pollForRecords(1, 45)).as("Should receive the seed row").isNotEmpty();

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DELETE FROM dd_events WHERE id = 7");
            }
            List<SourceRecord> deletes = pollForRecords(
                    r -> r.value() != null && "d".equals(((Struct) r.value()).getString("op")), 1, 60);
            assertThat(deletes).as("The delete event must arrive").isNotEmpty();

            assertThat(logs.list)
                    .as("A delete must not trigger a schema-drift refresh")
                    .noneMatch(e -> e.getFormattedMessage().contains("Schema change detected")
                            && e.getFormattedMessage().contains("dd_events"));
        }
        finally {
            sourceLogger.detachAppender(logs);
        }
    }
}
