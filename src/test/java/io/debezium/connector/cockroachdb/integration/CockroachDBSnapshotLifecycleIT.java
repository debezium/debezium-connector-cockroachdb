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
 * End-to-end verification of the snapshot lifecycle (dbz#2155). With {@code snapshot.mode=initial}
 * the changefeed initial scan backfills existing rows; this asserts those backfill records carry
 * {@code source.snapshot}, and that once the scan completes (first resolved timestamp) the offset
 * reports {@code snapshot_completed=true} and steady-state records are not marked as snapshot.
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBSnapshotLifecycleIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSnapshotLifecycleIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.11");
    private static final String DATABASE_NAME = "snaplifecycle_testdb";
    private static final String TABLE_NAME = "snaplifecycle_orders";

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
                defaultJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = defaultConn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
        }

        String testJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/" + DATABASE_NAME);
        connection = DriverManager.getConnection(testJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword());
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (id INT PRIMARY KEY, name STRING)");
            // Seed rows BEFORE the connector starts so the initial scan backfills them.
            for (int i = 1; i <= 3; i++) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (" + i + ", 'seed-" + i + "')");
            }
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
    public void shouldReportSnapshotLifecycleForInitialScan() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", "snaplifecycle-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "snaplifecycle-test");
        config.put("topic.prefix", "snaplifecycle-test");
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", hostBootstrap);
        config.put("cockroachdb.changefeed.resolved.interval", "5s");
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("snapshot.mode", "initial");
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
                LOGGER.error("Task start failed: {}", e.getMessage(), e);
            }
        });
        taskThread.setDaemon(true);
        taskThread.start();
        assertThat(started.await(30, TimeUnit.SECONDS)).as("Task should start within 30 seconds").isTrue();
        assertThat(taskError.get()).as("Task should start without error").isNull();

        // Phase 1: collect the backfill reads (op=r) and confirm they are marked as snapshot records.
        List<SourceRecord> all = new ArrayList<>();
        boolean sawSnapshotRead = false;
        for (int i = 0; i < 60 && !sawSnapshotRead; i++) {
            List<SourceRecord> polled = task.poll();
            if (polled != null) {
                all.addAll(polled);
            }
            for (SourceRecord r : all) {
                if ("r".equals(op(r)) && snapshot(r) != null && !"false".equals(snapshot(r))) {
                    sawSnapshotRead = true;
                    break;
                }
            }
            Thread.sleep(1000);
        }
        assertThat(sawSnapshotRead)
                .as("Initial-scan backfill reads (op=r) must carry source.snapshot")
                .isTrue();

        // Phase 2: after the scan completes, new DML is a normal create that is not a snapshot record,
        // and the offset reports snapshot_completed=true.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (100, 'live')");
        }

        SourceRecord live = null;
        for (int i = 0; i < 60 && live == null; i++) {
            List<SourceRecord> polled = task.poll();
            if (polled != null) {
                for (SourceRecord r : polled) {
                    if ("c".equals(op(r)) && after(r) != null && intId(after(r)) == 100) {
                        live = r;
                        break;
                    }
                }
            }
            Thread.sleep(1000);
        }
        assertThat(live).as("Should receive the live create after the snapshot").isNotNull();
        assertThat(snapshot(live))
                .as("Steady-state records must not be marked as snapshot")
                .satisfiesAnyOf(s -> assertThat(s).isNull(), s -> assertThat(s).isEqualTo("false"));
        assertThat(String.valueOf(live.sourceOffset().get("snapshot_completed")))
                .as("snapshot_completed must be true once the initial scan has finished")
                .isEqualTo("true");
    }

    private static String op(SourceRecord r) {
        if (r.value() instanceof Struct v && v.schema().field("op") != null) {
            return v.getString("op");
        }
        return null;
    }

    private static Struct after(SourceRecord r) {
        if (r.value() instanceof Struct v && v.schema().field("after") != null) {
            return v.getStruct("after");
        }
        return null;
    }

    private static int intId(Struct after) {
        return ((Number) after.get("id")).intValue();
    }

    private static String snapshot(SourceRecord r) {
        if (r.value() instanceof Struct v && v.schema().field("source") != null) {
            Struct source = v.getStruct("source");
            if (source != null && source.schema().field("snapshot") != null && source.get("snapshot") != null) {
                return String.valueOf(source.get("snapshot"));
            }
        }
        return null;
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
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
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
