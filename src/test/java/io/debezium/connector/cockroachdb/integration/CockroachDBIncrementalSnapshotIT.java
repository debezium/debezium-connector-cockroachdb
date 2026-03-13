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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterAll;
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
 * Integration test for signal-based incremental snapshots.
 * Verifies that:
 * <ul>
 *   <li>The connector accepts incremental snapshot configuration</li>
 *   <li>A signaling table can trigger an incremental snapshot</li>
 *   <li>Snapshot records (op=r) are emitted for the target table</li>
 * </ul>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBIncrementalSnapshotIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBIncrementalSnapshotIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.6");
    private static final String DATABASE_NAME = "inc_snap_testdb";
    private static final String TABLE_NAME = "snap_products";
    private static final String SIGNAL_TABLE = "debezium_signal";

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

    @AfterAll
    static void tearDownNetwork() {
        NETWORK.close();
    }

    @BeforeEach
    public void setUp() throws Exception {
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
            stmt.execute("DROP TABLE IF EXISTS " + SIGNAL_TABLE);
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " ("
                    + "id INT PRIMARY KEY, "
                    + "name STRING NOT NULL, "
                    + "price DECIMAL(10,2)"
                    + ")");
            stmt.execute("CREATE TABLE " + SIGNAL_TABLE + " ("
                    + "id STRING PRIMARY KEY, "
                    + "type STRING NOT NULL, "
                    + "data STRING"
                    + ")");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Widget', 9.99)");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 'Gadget', 19.99)");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 'Gizmo', 29.99)");
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
    public void shouldPerformIncrementalSnapshotViaSignalingTable() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", "inc-snap-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "inc-snap-test");
        config.put("topic.prefix", "inc-snap");
        config.put("table.include.list", "public." + TABLE_NAME + ",public." + SIGNAL_TABLE);
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
        config.put("snapshot.mode", "no_data");
        config.put("heartbeat.interval.ms", "1000");
        config.put("signal.data.collection", DATABASE_NAME + ".public." + SIGNAL_TABLE);
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

        // Let streaming settle -- with no_data snapshot, changefeed starts with cursor=now
        Thread.sleep(5000);

        // Insert a streaming record first (op=c) so we know streaming is working
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (4, 'NewItem', 39.99)");
        }

        // Poll for the streaming record
        List<SourceRecord> streamingRecords = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null) {
                    for (SourceRecord r : records) {
                        if (r.topic() != null && !r.topic().contains("__debezium-heartbeat")) {
                            streamingRecords.add(r);
                            LOGGER.info("Streaming record: topic={}, op={}", r.topic(),
                                    ((Struct) r.value()).getString("op"));
                        }
                    }
                }
            }
            catch (Exception e) {
                LOGGER.warn("Poll failed: {}", e.getMessage());
            }
            if (!streamingRecords.isEmpty()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(streamingRecords).as("Should receive streaming record").isNotEmpty();
        LOGGER.info("Streaming is active, received {} records", streamingRecords.size());

        // Now trigger incremental snapshot via the signaling table
        LOGGER.info("Triggering incremental snapshot via signaling table...");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + SIGNAL_TABLE + " VALUES ("
                    + "'inc-snap-1', "
                    + "'execute-snapshot', "
                    + "'{\"data-collections\": [\"" + DATABASE_NAME + ".public." + TABLE_NAME + "\"]}'::STRING"
                    + ")");
        }
        LOGGER.info("Signal inserted, waiting for incremental snapshot records...");

        // Poll for snapshot records (op=r) from the incremental snapshot
        List<SourceRecord> snapshotRecords = new ArrayList<>();
        for (int i = 0; i < 45; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null) {
                    for (SourceRecord r : records) {
                        if (r.topic() != null && !r.topic().contains("__debezium-heartbeat")) {
                            Struct value = (Struct) r.value();
                            String op = value.getString("op");
                            if ("r".equals(op)) {
                                snapshotRecords.add(r);
                                Struct after = value.getStruct("after");
                                LOGGER.info("Snapshot record: op={}, id={}, name={}",
                                        op, after.get("id"), after.get("name"));
                            }
                        }
                    }
                }
            }
            catch (Exception e) {
                LOGGER.warn("Poll failed: {}", e.getMessage());
            }
            if (snapshotRecords.size() >= 4) {
                break;
            }
            Thread.sleep(1000);
        }

        LOGGER.info("Incremental snapshot IT results: {} streaming records, {} snapshot records",
                streamingRecords.size(), snapshotRecords.size());

        // We should have the 3 original rows + 1 new row = 4 snapshot records
        assertThat(snapshotRecords).as("Should receive incremental snapshot records (op=r)")
                .hasSizeGreaterThanOrEqualTo(4);
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
                        return Collections.emptyMap();
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
                return new PluginMetrics() {
                    @Override
                    public MetricName metricName(String name, String description, LinkedHashMap<String, String> tags) {
                        return new MetricName(name, "test", description, tags);
                    }

                    @Override
                    public void addMetric(MetricName name, MetricValueProvider<?> provider) {
                    }

                    @Override
                    public void removeMetric(MetricName name) {
                    }

                    @Override
                    public Sensor addSensor(String name) {
                        return null;
                    }

                    @Override
                    public void removeSensor(String name) {
                    }
                };
            }
        };
    }
}
