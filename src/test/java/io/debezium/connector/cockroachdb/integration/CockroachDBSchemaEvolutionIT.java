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
 * Integration test for schema evolution detection.
 *
 * <p>Validates that ALTER TABLE ADD COLUMN is detected automatically
 * and the new column appears in emitted records without a connector restart.</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBSchemaEvolutionIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSchemaEvolutionIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.6");
    private static final String DATABASE_NAME = "schema_evo_testdb";
    private static final String TABLE_NAME = "evo_events";

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
    public void shouldDetectAddColumnWithoutRestart() throws Exception {
        // Insert initial row (before schema change)
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Alice')");
        }

        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", "schema-evo-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "evo-test");
        config.put("topic.prefix", "evo-test");
        config.put("table.include.list", "public." + TABLE_NAME);
        config.put("cockroachdb.skip.permission.check", "true");
        config.put("cockroachdb.schema.name", "public");
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", hostBootstrap);
        config.put("cockroachdb.changefeed.envelope", "enriched");
        config.put("cockroachdb.changefeed.enriched.properties", "source,schema");
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("cockroachdb.changefeed.resolved.interval", "2s");
        config.put("snapshot.mode", "initial");
        config.put("heartbeat.interval.ms", "1000");
        config.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

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

        // Poll to get the initial row
        List<SourceRecord> preSchemaChangeRecords = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null) {
                    for (SourceRecord r : records) {
                        if (r.topic() != null && !r.topic().contains("__debezium-heartbeat")) {
                            preSchemaChangeRecords.add(r);
                            LOGGER.info("Pre-DDL record: topic={}, key={}", r.topic(), r.key());
                        }
                    }
                }
            }
            catch (Exception e) {
                LOGGER.warn("Poll failed: {}", e.getMessage());
            }
            if (!preSchemaChangeRecords.isEmpty()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(preSchemaChangeRecords).as("Should receive initial row").isNotEmpty();

        // Verify initial record does NOT have "email" field
        SourceRecord initialRecord = preSchemaChangeRecords.get(0);
        Struct initialValue = (Struct) initialRecord.value();
        Struct initialAfter = initialValue.getStruct("after");
        assertThat(initialAfter.schema().field("email")).as("Initial record should not have email field").isNull();
        LOGGER.info("Initial record verified: no 'email' field");

        // Now perform ALTER TABLE ADD COLUMN
        LOGGER.info("Executing ALTER TABLE ADD COLUMN email STRING...");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("ALTER TABLE " + TABLE_NAME + " ADD COLUMN email STRING");
        }

        // Insert a new row with the new column
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 'Bob', 'bob@test.com')");
        }

        // Poll until a record with the new "email" field appears (backfill + new insert)
        List<SourceRecord> postSchemaChangeRecords = new ArrayList<>();
        boolean foundEmailField = false;
        for (int i = 0; i < 45; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null) {
                    for (SourceRecord r : records) {
                        if (r.topic() != null && !r.topic().contains("__debezium-heartbeat")) {
                            postSchemaChangeRecords.add(r);
                            Struct value = (Struct) r.value();
                            Struct after = value.getStruct("after");
                            if (after != null && after.schema().field("email") != null) {
                                foundEmailField = true;
                                LOGGER.info("Post-DDL record with email field: key={}, email={}",
                                        r.key(), after.get("email"));
                            }
                            else {
                                LOGGER.info("Post-DDL record without email: key={}", r.key());
                            }
                        }
                    }
                }
            }
            catch (Exception e) {
                LOGGER.warn("Poll failed: {}", e.getMessage());
            }
            if (foundEmailField) {
                break;
            }
            Thread.sleep(1000);
        }

        LOGGER.info("Schema evolution IT results: {} pre-DDL records, {} post-DDL records, foundEmail={}",
                preSchemaChangeRecords.size(), postSchemaChangeRecords.size(), foundEmailField);

        assertThat(foundEmailField).as("Should detect ADD COLUMN and emit record with new 'email' field").isTrue();
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
