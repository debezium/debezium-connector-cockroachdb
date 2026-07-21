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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
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
 * Integration test for converting events that predate a schema change adding a
 * {@code NOT NULL JSONB} column.
 *
 * <p>Scenario: rows are written and their changefeed events land in the intermediate topic, the
 * connector is restarted after {@code ALTER TABLE ... ADD COLUMN doc JSONB NOT NULL DEFAULT},
 * and the backlog is replayed against the new table schema. The replayed events carry no value
 * for the new column, so the emitted structs hold null for it. The JSONB field schema must be
 * optional; a required field with no default fails {@link JsonConverter} with "Conversion error:
 * null value for field that is required and has no default value".</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBNotNullJsonbSchemaChangeIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBNotNullJsonbSchemaChangeIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.13");
    private static final String DATABASE_NAME = "jsonb_backlog_testdb";
    private static final String TABLE_NAME = "jsonb_events";

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
                    + "name STRING NOT NULL"
                    + ")");
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        stopTask();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    public void shouldConvertBacklogEventsAfterAddingNotNullJsonbColumn() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Alice')");
        }

        Map<String, String> config = connectorConfig();

        // First task run: creates the changefeed and intermediate topic; the event for row 1 is
        // written under the original two-column schema.
        startTask(config);
        List<SourceRecord> initialRecords = pollForRecords(1, 30);
        assertThat(initialRecords).as("Should receive the pre-DDL row").isNotEmpty();
        stopTask();

        // Row 2 lands in the intermediate topic under the old schema while no task is consuming.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 'Bob')");
        }

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("ALTER TABLE " + TABLE_NAME + " ADD COLUMN doc JSONB NOT NULL DEFAULT '{}'::JSONB");
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

    private Map<String, String> connectorConfig() {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");
        Map<String, String> config = new HashMap<>();
        config.put("name", "jsonb-backlog-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "jsonb-backlog");
        config.put("topic.prefix", "jsonb-backlog");
        config.put("table.include.list", "public." + TABLE_NAME);
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

    private void startTask(Map<String, String> config) throws Exception {
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

    private List<SourceRecord> pollForRecords(int minimum, int attempts) throws Exception {
        List<SourceRecord> collected = new ArrayList<>();
        for (int i = 0; i < attempts; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null) {
                    for (SourceRecord r : records) {
                        if (r.topic() != null && !r.topic().contains("__debezium-heartbeat")) {
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
