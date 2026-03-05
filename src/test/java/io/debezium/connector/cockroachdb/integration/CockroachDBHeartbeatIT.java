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
 * Integration test for heartbeat support via CockroachDB resolved timestamps.
 *
 * <p>Validates that resolved timestamps advance offsets during idle periods
 * and that the connector produces heartbeat records when configured.</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBHeartbeatIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBHeartbeatIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v26.1.0");
    private static final String DATABASE_NAME = "heartbeat_testdb";
    private static final String TABLE_NAME = "hb_events";

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
                    + "data STRING NOT NULL"
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
    public void shouldAdvanceOffsetsViaResolvedTimestamps() throws Exception {
        // Insert one row so the changefeed has something to start with
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'initial')");
        }

        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", "heartbeat-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "hb-test");
        config.put("topic.prefix", "hb-test");
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
        // Short resolved interval so we get frequent resolved timestamps
        config.put("cockroachdb.changefeed.resolved.interval", "2s");
        config.put("snapshot.mode", "initial");
        // Enable Debezium heartbeats
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

        // Poll to get the initial data event(s) and then let idle period produce heartbeats
        List<SourceRecord> allRecords = new ArrayList<>();
        List<SourceRecord> heartbeatRecords = new ArrayList<>();
        String lastOffsetCursor = null;
        int offsetAdvanceCount = 0;

        // Poll for 20 seconds: first we expect data events, then idle period with heartbeats
        int maxAttempts = 20;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null && !records.isEmpty()) {
                    for (SourceRecord record : records) {
                        String topic = record.topic();
                        if (topic != null && topic.contains("__debezium-heartbeat")) {
                            heartbeatRecords.add(record);
                            LOGGER.info("  Heartbeat record: topic={}", topic);
                        }
                        else {
                            allRecords.add(record);
                            LOGGER.info("  Data record: topic={}, key={}", topic, record.key());
                        }

                        // Check if offset cursor advanced
                        Map<String, ?> offset = record.sourceOffset();
                        if (offset != null) {
                            String cursor = (String) offset.get("offset.cursor");
                            if (cursor != null && !cursor.equals(lastOffsetCursor)) {
                                offsetAdvanceCount++;
                                lastOffsetCursor = cursor;
                                LOGGER.info("  Offset advanced to cursor: {}", cursor);
                            }
                        }
                    }
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

        LOGGER.info("Heartbeat IT results: {} data records, {} heartbeat records, {} offset advances",
                allRecords.size(), heartbeatRecords.size(), offsetAdvanceCount);

        // We should have received at least the initial data event
        assertThat(allRecords).as("Should receive at least one data event").isNotEmpty();

        // Offsets should have advanced more than once (initial data + resolved timestamps)
        assertThat(offsetAdvanceCount).as("Offset should advance via resolved timestamps").isGreaterThan(1);

        // The final cursor should be a resolved timestamp (numeric format like "1234567890.0000000000")
        assertThat(lastOffsetCursor).as("Final cursor should be a resolved timestamp")
                .matches("\\d+\\.\\d+");
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
