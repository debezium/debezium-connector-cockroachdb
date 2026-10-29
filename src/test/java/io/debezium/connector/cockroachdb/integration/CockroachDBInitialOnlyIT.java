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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.cockroachdb.CockroachDBConnectorTask;

/**
 * End-to-end verification of {@code snapshot.mode=initial_only} (debezium/dbz#2966). CockroachDB
 * rejects {@code resolved}, {@code updated}, and {@code diff} together with
 * {@code initial_scan='only'}, so the connector must leave them out, emit every existing row as a
 * snapshot read, mark the final record as the last snapshot record with
 * {@code snapshot_completed=true}, stop, and not scan again when restarted from that offset.
 * Covers the kafka and sinkless modes.
 */
@Testcontainers
public class CockroachDBInitialOnlyIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBInitialOnlyIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.18");
    private static final String DATABASE_NAME = "initial_only_db";
    private static final int ROWS = 200;

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(
            DockerImageName.parse("cockroachdb/cockroach:" + COCKROACHDB_VERSION))
            .withNetwork(NETWORK)
            .withNetworkAliases("cockroachdb");

    private Connection connection;
    private final List<CockroachDBConnectorTask> tasks = new ArrayList<>();

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
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        for (CockroachDBConnectorTask task : tasks) {
            try {
                task.stop();
            }
            catch (Exception e) {
                LOGGER.warn("Error stopping task: {}", e.getMessage());
            }
        }
        tasks.clear();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    public void shouldSnapshotEveryRowAndStopWithKafkaSink() throws Exception {
        String table = "kafka_orders";
        seed(table);
        Map<String, String> config = initialOnlyConfig("initial-only-kafka", table);
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:19092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", ""));
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");

        verifyInitialOnlyLifecycle(config, table);
    }

    @Test
    public void shouldSnapshotEveryRowAndStopWithSinklessChangefeed() throws Exception {
        String table = "sinkless_orders";
        seed(table);
        Map<String, String> config = initialOnlyConfig("initial-only-sinkless", table);
        config.put("cockroachdb.changefeed.sink.type", "sinkless");

        verifyInitialOnlyLifecycle(config, table);
    }

    private void verifyInitialOnlyLifecycle(Map<String, String> config, String table) throws Exception {
        CockroachDBConnectorTask task = startTask(config, null);
        List<SourceRecord> snapshot = pollFor(task, table, records -> !records.isEmpty() && "last".equals(snapshot(last(records))), 120);

        assertThat(ids(snapshot))
                .as("every existing row is emitted exactly once")
                .hasSize(ROWS)
                .containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, ROWS).boxed().collect(Collectors.toList()));
        assertThat(snapshot).allSatisfy(r -> assertThat(op(r)).as("initial_only rows are snapshot reads").isEqualTo("r"));
        assertThat(snapshot.subList(0, snapshot.size() - 1))
                .allSatisfy(r -> assertThat(snapshot(r)).isEqualTo("true"));
        SourceRecord lastRecord = last(snapshot);
        assertThat(snapshot(lastRecord)).isEqualTo("last");
        assertThat(String.valueOf(lastRecord.sourceOffset().get("snapshot_completed")))
                .as("the last snapshot record persists the completed snapshot")
                .isEqualTo("true");

        // The connector stops after the snapshot, so later changes are not captured.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + table + " VALUES (" + (ROWS + 1) + ", 'after-snapshot')");
        }
        assertThat(pollFor(task, table, records -> false, 10)).as("no events after the initial_only snapshot").isEmpty();
        task.stop();
        tasks.remove(task);

        // A restart from the committed offset must neither scan again nor create another changefeed.
        int jobsBeforeRestart = changefeedJobCount();
        CockroachDBConnectorTask restarted = startTask(config, lastRecord);
        assertThat(pollFor(restarted, table, records -> false, 15)).as("no events after restarting a completed initial_only run").isEmpty();
        assertThat(changefeedJobCount()).as("no changefeed is created after a completed initial_only run").isEqualTo(jobsBeforeRestart);
    }

    private void seed(String table) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + table + " (id INT PRIMARY KEY, name STRING)");
            stmt.execute("INSERT INTO " + table + " SELECT i, 'seed-' || i::STRING FROM generate_series(1, " + ROWS + ") AS g(i)");
        }
    }

    private Map<String, String> initialOnlyConfig(String name, String table) {
        Map<String, String> config = new HashMap<>();
        config.put("name", name);
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", name);
        config.put("topic.prefix", name);
        config.put("table.include.list", "public." + table);
        config.put("snapshot.mode", "initial_only");
        // CockroachDB rejects updated and diff together with initial_scan='only'. Enabling them proves
        // the connector leaves them out instead of failing to create the changefeed.
        config.put("cockroachdb.changefeed.include.updated", "true");
        config.put("cockroachdb.changefeed.include.diff", "true");
        config.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        return config;
    }

    private CockroachDBConnectorTask startTask(Map<String, String> config, SourceRecord resumeFrom) throws Exception {
        CockroachDBConnectorTask task = new CockroachDBConnectorTask();
        task.initialize(taskContext(resumeFrom));
        tasks.add(task);

        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread starter = new Thread(() -> {
            try {
                task.start(config);
            }
            catch (Throwable e) {
                error.set(e);
                LOGGER.error("Task start failed: {}", e.getMessage(), e);
            }
            finally {
                started.countDown();
            }
        });
        starter.setDaemon(true);
        starter.start();
        assertThat(started.await(60, TimeUnit.SECONDS)).as("task should start within 60 seconds").isTrue();
        assertThat(error.get()).as("task should start without error").isNull();
        return task;
    }

    private static List<SourceRecord> pollFor(CockroachDBConnectorTask task, String table, Predicate<List<SourceRecord>> done, int maxSeconds)
            throws InterruptedException {
        List<SourceRecord> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(maxSeconds);
        while (System.currentTimeMillis() < deadline && !done.test(collected)) {
            List<SourceRecord> polled = task.poll();
            if (polled == null || polled.isEmpty()) {
                Thread.sleep(200);
                continue;
            }
            for (SourceRecord r : polled) {
                if (r.topic() != null && r.topic().endsWith("." + table) && op(r) != null) {
                    collected.add(r);
                }
            }
        }
        return collected;
    }

    private int changefeedJobCount() throws Exception {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT count(*) FROM [SHOW CHANGEFEED JOBS]")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static SourceRecord last(List<SourceRecord> records) {
        return records.get(records.size() - 1);
    }

    private static List<Integer> ids(List<SourceRecord> records) {
        return records.stream().map(CockroachDBInitialOnlyIT::id).collect(Collectors.toList());
    }

    private static Integer id(SourceRecord r) {
        Struct after = ((Struct) r.value()).getStruct("after");
        return after == null ? null : ((Number) after.get("id")).intValue();
    }

    private static String op(SourceRecord r) {
        if (r.value() instanceof Struct v && v.schema().field("op") != null) {
            return v.getString("op");
        }
        return null;
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

    private static SourceTaskContext taskContext(SourceRecord resumeFrom) {
        Map<String, ?> storedPartition = resumeFrom == null ? null : resumeFrom.sourcePartition();
        Map<String, ?> storedOffset = resumeFrom == null ? null : resumeFrom.sourceOffset();
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return new HashMap<>();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return storedPartition != null && storedPartition.equals(partition) ? (Map<String, Object>) storedOffset : null;
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                        Map<Map<String, T>, Map<String, Object>> result = new HashMap<>();
                        for (Map<String, T> partition : partitions) {
                            Map<String, Object> offset = offset(partition);
                            if (offset != null) {
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
