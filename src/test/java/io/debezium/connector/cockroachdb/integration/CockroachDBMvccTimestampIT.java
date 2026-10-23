/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

/**
 * Verifies that {@code cockroachdb.changefeed.include.mvcc.timestamp} emits each row's own MVCC
 * commit timestamp as {@code source.ts_hlc} on initial scan, create, update and delete events
 * (debezium/dbz#2919). Uses a sinkless changefeed under the embedded engine, so only a CockroachDB
 * container is needed.
 * <p>
 * The {@code source.mvcc_timestamp} and {@code source.ts_hlc} fields this relies on were verified
 * against CockroachDB v25.2 (the minimum supported version) and v25.4.
 */
@Testcontainers
public class CockroachDBMvccTimestampIT {

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.18");
    private static final String DATABASE_NAME = "mvcc_ts_db";
    private static final String TABLE_NAME = "mvcc_orders";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(
            DockerImageName.parse("cockroachdb/cockroach:" + COCKROACHDB_VERSION));

    private Connection connection;
    private Path offsetFile;
    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;

    @BeforeEach
    public void setUp() throws Exception {
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
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (id INT PRIMARY KEY, amount DECIMAL(10,2))");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100.00)");
        }

        offsetFile = Files.createTempFile("mvcc-ts-offsets", ".dat");
        Files.deleteIfExists(offsetFile);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (offsetFile != null) {
            Files.deleteIfExists(offsetFile);
        }
    }

    @Test
    public void shouldEmitRowMvccTimestampAsTsHlcForEveryOperation() throws Exception {
        String seededRowMvcc;
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT crdb_internal_mvcc_timestamp FROM " + TABLE_NAME + " WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            seededRowMvcc = rs.getBigDecimal(1).toPlainString();
        }

        List<JsonNode> events = new CopyOnWriteArrayList<>();
        engine = DebeziumEngine.create(Json.class)
                .using(connectorProperties())
                .notifying((ChangeEvent<String, String> record) -> {
                    if (record.value() != null) {
                        try {
                            events.add(MAPPER.readTree(record.value()));
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .build();
        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        waitUntil(() -> ops(events).contains("r"), 60);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200.00)");
            stmt.execute("UPDATE " + TABLE_NAME + " SET amount = 150.00 WHERE id = 1");
            stmt.execute("DELETE FROM " + TABLE_NAME + " WHERE id = 2");
        }
        waitUntil(() -> ops(events).containsAll(List.of("r", "c", "u", "d")), 60);

        // Collectors.toMap rejects null values, so collect by hand to let a missing ts_hlc fail the assertion below.
        Map<String, String> tsHlcByOp = new HashMap<>();
        for (JsonNode event : events) {
            tsHlcByOp.putIfAbsent(event.path("op").asText(), event.path("source").path("ts_hlc").asText(null));
        }
        assertThat(tsHlcByOp).containsKeys("r", "c", "u", "d");
        assertThat(tsHlcByOp.values()).as("every event carries source.ts_hlc").doesNotContainNull();

        // The initial scan row carries the row's own MVCC timestamp, not the scan time.
        assertThat(new BigDecimal(tsHlcByOp.get("r"))).isEqualByComparingTo(seededRowMvcc);
        // Separate transactions commit at distinct, increasing HLC timestamps.
        assertThat(new BigDecimal(tsHlcByOp.get("c"))).isGreaterThan(new BigDecimal(tsHlcByOp.get("r")));
        assertThat(new BigDecimal(tsHlcByOp.get("u"))).isGreaterThan(new BigDecimal(tsHlcByOp.get("c")));
        assertThat(new BigDecimal(tsHlcByOp.get("d"))).isGreaterThan(new BigDecimal(tsHlcByOp.get("u")));
    }

    private Properties connectorProperties() {
        Properties props = new Properties();
        props.setProperty("name", "mvcc-ts");
        props.setProperty("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetFile.toString());
        props.setProperty("offset.flush.interval.ms", "1000");
        props.setProperty("converter.schemas.enable", "false");

        props.setProperty("database.hostname", cockroachdb.getHost());
        props.setProperty("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        props.setProperty("database.user", cockroachdb.getUsername());
        props.setProperty("database.password", cockroachdb.getPassword());
        props.setProperty("database.dbname", DATABASE_NAME);
        props.setProperty("database.sslmode", "disable");
        props.setProperty("database.server.name", "mvcc-ts");
        props.setProperty("topic.prefix", "mvccts");

        props.setProperty("cockroachdb.changefeed.sink.type", "sinkless");
        props.setProperty("cockroachdb.changefeed.include.mvcc.timestamp", "true");
        props.setProperty("cockroachdb.changefeed.resolved.interval", "2s");
        props.setProperty("snapshot.mode", "initial");
        return props;
    }

    private static List<String> ops(List<JsonNode> events) {
        return events.stream().map(e -> e.path("op").asText()).collect(Collectors.toList());
    }

    private static void waitUntil(BooleanSupplier condition, int maxSeconds) throws InterruptedException {
        for (int i = 0; i < maxSeconds && !condition.getAsBoolean(); i++) {
            Thread.sleep(1000);
        }
    }
}
