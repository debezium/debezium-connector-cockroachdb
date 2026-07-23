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
 * Integration test for changefeed reuse option validation.
 *
 * <p>CockroachDB fixes changefeed options at creation time. When the connector finds an
 * existing changefeed for its tables and topic prefix that was created without the {@code diff}
 * option while {@code cockroachdb.changefeed.include.diff} is enabled, it must fail with a
 * clear message instead of silently reusing a changefeed that can never deliver before
 * images.</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBChangefeedReuseDiffIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBChangefeedReuseDiffIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.13");
    private static final String DATABASE_NAME = "reuse_diff_testdb";
    private static final String TABLE_NAME = "reuse_events";

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
                    + "id INT8 PRIMARY KEY, "
                    + "name STRING NOT NULL"
                    + ")");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Alice')");
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
    public void shouldFailFastWhenReusedChangefeedLacksDiffOption() throws Exception {
        // First run creates the changefeed without the diff option.
        startTask(connectorConfig(false));
        assertThat(pollUntilRecordsOrError(30).records).as("First run should stream the seed row").isNotEmpty();
        stopTask();

        // Second run enables include.diff; the connector finds the existing no-diff changefeed
        // and must refuse to reuse it.
        startTask(connectorConfig(true));
        PollOutcome outcome = pollUntilRecordsOrError(45);
        assertThat(outcome.error)
                .as("Second run should fail instead of silently reusing the no-diff changefeed")
                .isNotNull();
        assertThat(outcome.error.getMessage() + " " + rootCauseMessage(outcome.error))
                .contains("diff");
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

    private Map<String, String> connectorConfig(boolean includeDiff) {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");
        Map<String, String> config = new HashMap<>();
        config.put("name", "reuse-diff-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "reuse-test");
        config.put("topic.prefix", "reuse-test");
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
        if (includeDiff) {
            config.put("cockroachdb.changefeed.include.diff", "true");
        }
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
