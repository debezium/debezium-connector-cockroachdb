/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
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
 * End-to-end verification that CockroachDB temporal column types are emitted with the correct
 * Debezium logical schema types and non-null values: {@code TIMESTAMP -> MicroTimestamp},
 * {@code TIMESTAMPTZ -> ZonedTimestamp}, {@code TIME -> MicroTime}, {@code TIMETZ -> ZonedTime},
 * {@code DATE -> Date}. Guards the temporal type contract for downstream sinks.
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBTemporalTypesIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBTemporalTypesIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.11");
    private static final String DATABASE_NAME = "temporal_testdb";
    private static final String TABLE_NAME = "temporal_types";

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
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ("
                    + "id INT PRIMARY KEY, "
                    + "ts TIMESTAMP, "
                    + "tstz TIMESTAMPTZ, "
                    + "tm TIME, "
                    + "tmtz TIMETZ, "
                    + "d DATE"
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
    public void shouldEmitTemporalTypesWithDebeziumLogicalSchemas() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", "temporal-types-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "temporal-test");
        config.put("topic.prefix", "temporal-test");
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", hostBootstrap);
        config.put("cockroachdb.changefeed.include.diff", "true");
        config.put("cockroachdb.changefeed.resolved.interval", "5s");
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("snapshot.mode", "no_data");
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

        waitForRunningChangefeed(30);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, "
                    + "'2026-06-08 11:01:45.883', "
                    + "'2026-06-08 11:01:45.883+02:00', "
                    + "'11:01:45.883', "
                    + "'11:01:45.883+02:00', "
                    + "'2026-06-08')");
        }

        List<SourceRecord> records = new ArrayList<>();
        Struct after = null;
        for (int i = 0; i < 60 && after == null; i++) {
            List<SourceRecord> polled = task.poll();
            if (polled != null) {
                records.addAll(polled);
            }
            for (SourceRecord record : records) {
                if (record.value() instanceof Struct value && value.schema().field("after") != null) {
                    Struct a = value.getStruct("after");
                    if (a != null && a.get("ts") != null) {
                        after = a;
                        break;
                    }
                }
            }
            Thread.sleep(1000);
        }

        assertThat(after).as("Should receive a create record with the temporal row").isNotNull();
        LOGGER.info("Temporal after-struct: {}", after);

        // Schema names must be the Debezium time logical types.
        assertThat(after.schema().field("ts").schema().name()).isEqualTo("io.debezium.time.MicroTimestamp");
        assertThat(after.schema().field("tstz").schema().name()).isEqualTo("io.debezium.time.ZonedTimestamp");
        assertThat(after.schema().field("tm").schema().name()).isEqualTo("io.debezium.time.MicroTime");
        assertThat(after.schema().field("tmtz").schema().name()).isEqualTo("io.debezium.time.ZonedTime");
        assertThat(after.schema().field("d").schema().name()).isEqualTo("io.debezium.time.Date");

        // Values are populated with the right Java types (the original bug emitted null).
        assertThat(after.get("ts")).isInstanceOf(Long.class);
        assertThat(after.get("tstz")).isInstanceOf(String.class);
        assertThat(after.get("tm")).isInstanceOf(Long.class);
        assertThat(after.get("tmtz")).isInstanceOf(String.class);
        assertThat(after.get("d")).isInstanceOf(Integer.class);

        // The zoned strings must satisfy the logical-type formatters a downstream sink uses. This is
        // the contract that the raw CockroachDB values violate: TIMESTAMPTZ arrives offset-qualified
        // but TIMETZ arrives with an hour-only "+02" offset that ISO_OFFSET_TIME rejects.
        String tstz = (String) after.get("tstz");
        String tmtz = (String) after.get("tmtz");
        assertThatNoException()
                .as("tstz '%s' must parse as ZonedTimestamp (ISO_OFFSET_DATE_TIME)", tstz)
                .isThrownBy(() -> DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(tstz));
        assertThatNoException()
                .as("tmtz '%s' must parse as ZonedTime (ISO_OFFSET_TIME)", tmtz)
                .isThrownBy(() -> DateTimeFormatter.ISO_OFFSET_TIME.parse(tmtz));
    }

    private void waitForRunningChangefeed(int maxSeconds) throws Exception {
        for (int i = 0; i < maxSeconds; i++) {
            try (Statement stmt = connection.createStatement();
                    var rs = stmt.executeQuery(
                            "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' AND description LIKE '%" + TABLE_NAME + "%'")) {
                if (rs.next() && rs.getInt(1) > 0) {
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Connector did not start a running changefeed within " + maxSeconds + "s");
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
