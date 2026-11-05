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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.data.Struct;
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
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.cockroachdb.CockroachDBConnectorTask;
import io.debezium.connector.cockroachdb.CockroachDBStreamingChangeEventSource;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.junit.logging.LogInterceptor;

/**
 * End-to-end verification of {@code snapshot.mode=initial_only} (debezium/dbz#2966). CockroachDB
 * rejects {@code resolved}, {@code updated}, and {@code diff} together with
 * {@code initial_scan='only'}, so the connector must leave them out, emit every existing row as a
 * snapshot read, mark the final record as the last snapshot record with
 * {@code snapshot_completed=true}, stop, and not scan again when restarted from that offset.
 * Covers the kafka and sinkless modes, a restart during the scan, and a failing job status query.
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
        config.put("cockroachdb.changefeed.kafka.consumer.group.prefix", "initial-only-kafka-group");
        config.put("cockroachdb.changefeed.kafka.consumer.offset.commit.enabled", "true");

        verifyInitialOnlyLifecycle(config, table);

        // The record held back until the end of the scan is dispatched last, so the mirrored group
        // position must still reach the end of the intermediate topic.
        TopicPartition intermediate = new TopicPartition("initial-only-kafka." + DATABASE_NAME + ".public." + table, 0);
        Properties admin = new Properties();
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", ""));
        try (AdminClient client = AdminClient.create(admin)) {
            Map<TopicPartition, OffsetAndMetadata> committed = client.listConsumerGroupOffsets("initial-only-kafka-group")
                    .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
            long end = client.listOffsets(Map.of(intermediate, OffsetSpec.latest())).partitionResult(intermediate)
                    .get(30, TimeUnit.SECONDS).offset();
            assertThat(committed.get(intermediate)).as("mirrored group position for " + intermediate).isNotNull();
            assertThat(committed.get(intermediate).offset()).isEqualTo(end);
        }
    }

    @Test
    public void shouldPersistTheCompletedSnapshotOfEmptyTablesWithKafkaSink() throws Exception {
        String table = "kafka_empty";
        createTable(table);
        Map<String, String> config = initialOnlyConfig("initial-only-kafka-empty", table);
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:19092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", ""));
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("heartbeat.interval.ms", "500");

        verifyEmptySnapshotPersistsCompletion(config, table);
    }

    @Test
    public void shouldPersistTheCompletedSnapshotOfEmptyTablesWithSinklessChangefeed() throws Exception {
        String table = "sinkless_empty";
        createTable(table);
        Map<String, String> config = initialOnlyConfig("initial-only-sinkless-empty", table);
        config.put("cockroachdb.changefeed.sink.type", "sinkless");
        config.put("heartbeat.interval.ms", "500");

        verifyEmptySnapshotPersistsCompletion(config, table);
    }

    @Test
    public void shouldSnapshotEveryRowAndStopWithSinklessChangefeed() throws Exception {
        String table = "sinkless_orders";
        seed(table);
        Map<String, String> config = initialOnlyConfig("initial-only-sinkless", table);
        config.put("cockroachdb.changefeed.sink.type", "sinkless");

        verifyInitialOnlyLifecycle(config, table);
    }

    /**
     * When the job status query keeps failing, the task fails and the runtime restarts it. The restarted
     * task must reuse the job that is still scanning and wait for it, without another changefeed and
     * without losing or repeating rows.
     */
    @Test
    public void shouldReuseTheRunningJobWhenRestartedDuringTheScan() throws Exception {
        String table = "kafka_restart_mid_scan";
        seed(table);
        Map<String, String> config = kafkaInitialOnlyConfig("initial-only-kafka-restart", table);
        throttleChangefeeds(connection, 20);
        try {
            CockroachDBConnectorTask task = startTask(config, null);
            List<SourceRecord> beforeRestart = pollFor(task, table, records -> records.size() >= 20, 60);
            assertThat(beforeRestart).as("records before the restart").hasSizeGreaterThanOrEqualTo(20);
            assertThat(jobStatuses(connection, table)).as("the job is still scanning when the task restarts").containsExactly("running");
            task.stop();
            tasks.remove(task);

            CockroachDBConnectorTask restarted = startTask(config, last(beforeRestart));
            List<SourceRecord> afterRestart = pollFor(restarted, table, records -> !records.isEmpty() && "last".equals(snapshot(last(records))), 120);

            List<Integer> all = new ArrayList<>(ids(beforeRestart));
            all.addAll(ids(afterRestart));
            assertThat(all)
                    .as("every row is emitted exactly once across the restart")
                    .containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, ROWS).boxed().collect(Collectors.toList()));
            assertThat(String.valueOf(last(afterRestart).sourceOffset().get("snapshot_completed"))).isEqualTo("true");
            assertThat(jobStatuses(connection, table)).as("the restarted task reused the job").containsExactly("succeeded");
        }
        finally {
            resetChangefeedThrottle(connection);
        }
    }

    /**
     * A restart of the gateway node ends the connector's sessions while it waits for the job. The next
     * job status query fails, and the connector reconnects and retries it instead of failing the task.
     */
    @Test
    public void shouldReconnectWhenTheJobStatusQueryFailsOnce() throws Exception {
        String table = "kafka_status_query_retry";
        seed(table);
        Map<String, String> config = kafkaInitialOnlyConfig("initial-only-kafka-retry", table);
        LogInterceptor source = new LogInterceptor(CockroachDBStreamingChangeEventSource.class);
        LogInterceptor runtime = new LogInterceptor(BaseSourceTask.class);
        throttleChangefeeds(connection, 20);
        try {
            CockroachDBConnectorTask task = startTask(config, null);
            List<SourceRecord> records = pollFor(task, table, polled -> polled.size() >= 20, 60);
            assertThat(jobStatuses(connection, table)).as("the job is still scanning when the sessions end").containsExactly("running");
            assertThat(cancelOtherSessions(connection)).as("connector sessions ended").isPositive();
            records.addAll(pollFor(task, table, polled -> !polled.isEmpty() && "last".equals(snapshot(last(polled))), 120));

            assertThat(ids(records))
                    .as("every row is emitted exactly once")
                    .containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, ROWS).boxed().collect(Collectors.toList()));
            assertThat(source.containsWarnMessage("reconnecting and retrying")).as("the failed status query was retried").isTrue();
            assertThat(source.containsMessage("Reconnected the schema refresh connection")).isTrue();
            assertThat(runtime.containsWarnMessage("after a retriable exception")).as("the task was not restarted").isFalse();
        }
        finally {
            resetChangefeedThrottle(connection);
        }
    }

    /**
     * When the job status query keeps failing, the task must fail so that the runtime's error handling
     * (retriable restart with backoff, errors.max.retries) applies, instead of logging a warning every
     * second while the task looks healthy.
     */
    @Test
    public void shouldFailTheTaskWhenTheJobStatusQueryKeepsFailing() throws Exception {
        String table = "kafka_status_query_down";
        try (CockroachContainer doomed = new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:" + COCKROACHDB_VERSION))
                .withNetwork(NETWORK)) {
            doomed.start();
            try (Connection admin = DriverManager.getConnection(
                    doomed.getJdbcUrl().replace("/postgres", "/defaultdb"), doomed.getUsername(), doomed.getPassword());
                    Statement stmt = admin.createStatement()) {
                stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
                stmt.execute("CREATE DATABASE " + DATABASE_NAME);
                stmt.execute("CREATE TABLE " + DATABASE_NAME + ".public." + table + " (id INT PRIMARY KEY, name STRING)");
                stmt.execute("INSERT INTO " + DATABASE_NAME + ".public." + table
                        + " SELECT i, 'seed-' || i::STRING FROM generate_series(1, " + ROWS + ") AS g(i)");
                throttleChangefeeds(admin, 10);
            }
            Map<String, String> config = kafkaInitialOnlyConfig("initial-only-kafka-down", table);
            config.put("database.hostname", doomed.getHost());
            config.put("database.port", String.valueOf(doomed.getMappedPort(26257)));
            LogInterceptor source = new LogInterceptor(CockroachDBStreamingChangeEventSource.class);
            LogInterceptor runtime = new LogInterceptor(BaseSourceTask.class);

            CockroachDBConnectorTask task = startTask(config, null);
            assertThat(pollFor(task, table, records -> records.size() >= 10, 60)).hasSizeGreaterThanOrEqualTo(10);
            doomed.stop();

            RetriableException failure = pollForRetriableFailure(task, 60);
            assertThat(failure).as("the task failed with a retriable exception, so the runtime restarts it").isNotNull();
            assertThat(failure).hasStackTraceContaining("failed after reconnecting");
            assertThat(runtime.containsWarnMessage("after a retriable exception")).isTrue();
            assertThat(source.countOccurrences("reconnecting and retrying")).as("one retry instead of a warning every second").isEqualTo(1);
        }
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

    /**
     * With every captured table empty no record can carry the completed snapshot, so the heartbeat sent
     * when the scan completes must store it, and a restart from it must not scan again.
     */
    private void verifyEmptySnapshotPersistsCompletion(Map<String, String> config, String table) throws Exception {
        CockroachDBConnectorTask task = startTask(config, null);
        SourceRecord completed = pollForCompletedHeartbeat(task, 90);
        assertThat(completed).as("a heartbeat stores the completed snapshot when every captured table is empty").isNotNull();
        task.stop();
        tasks.remove(task);

        int jobsBeforeRestart = changefeedJobCount();
        CockroachDBConnectorTask restarted = startTask(config, completed);
        assertThat(pollFor(restarted, table, records -> false, 15)).as("no events after restarting a completed initial_only run").isEmpty();
        assertThat(changefeedJobCount()).as("no changefeed is created after a completed initial_only run").isEqualTo(jobsBeforeRestart);
    }

    private static SourceRecord pollForCompletedHeartbeat(CockroachDBConnectorTask task, int maxSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(maxSeconds);
        while (System.currentTimeMillis() < deadline) {
            List<SourceRecord> polled = task.poll();
            if (polled == null || polled.isEmpty()) {
                Thread.sleep(200);
                continue;
            }
            for (SourceRecord r : polled) {
                if (r.topic() != null && r.topic().startsWith("__debezium-heartbeat")
                        && "true".equals(String.valueOf(r.sourceOffset().get("snapshot_completed")))) {
                    return r;
                }
            }
        }
        return null;
    }

    private void createTable(String table) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + table + " (id INT PRIMARY KEY, name STRING)");
        }
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

    private Map<String, String> kafkaInitialOnlyConfig(String name, String table) {
        Map<String, String> config = initialOnlyConfig(name, table);
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:19092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", ""));
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        return config;
    }

    /**
     * Limits how many messages per second each node's changefeeds emit, so that the initial scan is
     * still running while a test interferes with it.
     */
    private static void throttleChangefeeds(Connection conn, int messagesPerSecond) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET CLUSTER SETTING changefeed.node_throttle_config = '{\"MessageRate\": " + messagesPerSecond
                    + ", \"MessageBurst\": " + messagesPerSecond + "}'");
        }
    }

    private static void resetChangefeedThrottle(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("RESET CLUSTER SETTING changefeed.node_throttle_config");
        }
    }

    private static List<String> jobStatuses(Connection conn, String table) throws Exception {
        List<String> statuses = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT status FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%" + table + "%'")) {
            while (rs.next()) {
                statuses.add(rs.getString(1));
            }
        }
        return statuses;
    }

    /**
     * Ends every client session except the caller's, the way a restart of the gateway node does.
     */
    private static int cancelOtherSessions(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate("CANCEL SESSIONS (SELECT session_id FROM [SHOW CLUSTER SESSIONS] "
                    + "WHERE session_id != (SELECT session_id FROM [SHOW session_id]) AND user_name = 'root' "
                    + "AND application_name NOT LIKE '$ internal%')");
        }
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

    /**
     * Polls the task until it fails with the retriable exception that Kafka Connect would answer by
     * polling again, so that the task restarts itself after its backoff.
     */
    private static RetriableException pollForRetriableFailure(CockroachDBConnectorTask task, int maxSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(maxSeconds);
        while (System.currentTimeMillis() < deadline) {
            try {
                task.poll();
            }
            catch (RetriableException e) {
                return e;
            }
            Thread.sleep(200);
        }
        return null;
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
