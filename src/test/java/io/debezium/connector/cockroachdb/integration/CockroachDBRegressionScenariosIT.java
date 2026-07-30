/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
