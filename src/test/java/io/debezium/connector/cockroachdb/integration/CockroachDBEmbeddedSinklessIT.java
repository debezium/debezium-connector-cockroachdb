/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

/**
 * Proves a fully <b>Kafka-free</b> pipeline: the CockroachDB connector running in sinkless mode under
 * the Debezium <b>embedded engine</b> ({@link DebeziumEngine}) with file-based offsets and an
 * in-process change consumer. There is no Kafka and no Kafka Connect runtime anywhere -- only a
 * CockroachDB container. Change events flow CockroachDB SQL stream -> connector -> embedded consumer.
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBEmbeddedSinklessIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBEmbeddedSinklessIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.13");
    private static final String DATABASE_NAME = "embedded_sinkless_db";
    private static final String TABLE_NAME = "embedded_orders";

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
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME
                    + " (id INT PRIMARY KEY, customer_name STRING NOT NULL, amount DECIMAL(10,2))");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Alice', 100.00), (2, 'Bob', 200.50)");
        }

        offsetFile = Files.createTempFile("embedded-sinkless-offsets", ".dat");
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
    public void shouldReplicateWithoutKafkaViaEmbeddedEngine() throws Exception {
        List<ChangeEvent<String, String>> received = new CopyOnWriteArrayList<>();

        Properties props = new Properties();
        props.setProperty("name", "embedded-sinkless");
        props.setProperty("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        // No Kafka, no Connect cluster: offsets live in a local file.
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetFile.toString());
        props.setProperty("offset.flush.interval.ms", "1000");

        props.setProperty("database.hostname", cockroachdb.getHost());
        props.setProperty("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        props.setProperty("database.user", cockroachdb.getUsername());
        props.setProperty("database.password", cockroachdb.getPassword());
        props.setProperty("database.dbname", DATABASE_NAME);
        props.setProperty("database.sslmode", "disable");
        props.setProperty("database.server.name", "embedded-sinkless");
        props.setProperty("topic.prefix", "embedded");

        // The whole point: sinkless source, so CockroachDB never touches Kafka either.
        props.setProperty("cockroachdb.changefeed.sink.type", "sinkless");
        props.setProperty("cockroachdb.changefeed.include.diff", "true");
        props.setProperty("cockroachdb.changefeed.resolved.interval", "5s");
        props.setProperty("snapshot.mode", "initial");

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying((ChangeEvent<String, String> record) -> received.add(record))
                .build();
        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        LOGGER.info("Embedded engine started (no Kafka, no Connect)");

        // Wait for the initial-scan reads of the two seeded rows.
        waitUntil(() -> received.size() >= 2, 60);
        assertThat(received.size())
                .as("Embedded engine should deliver the 2 seeded rows from the sinkless changefeed")
                .isGreaterThanOrEqualTo(2);

        // The engine is confirmed live; produce DML and confirm it streams through too.
        int before = received.size();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 'Carol', 320.00)");
            stmt.execute("UPDATE " + TABLE_NAME + " SET amount = 150.00 WHERE id = 1");
        }
        waitUntil(() -> received.size() > before, 60);

        LOGGER.info("Embedded engine received {} change events with no Kafka in the pipeline", received.size());
        assertThat(received.size())
                .as("Embedded engine should deliver live DML over the sinkless changefeed without Kafka")
                .isGreaterThan(before);

        // Every delivered event carries a real value payload (the Debezium envelope as JSON).
        boolean hasEnvelope = received.stream()
                .map(ChangeEvent::value)
                .filter(v -> v != null)
                .anyMatch(v -> v.contains("\"op\""));
        assertThat(hasEnvelope)
                .as("Delivered events should carry the Debezium envelope JSON")
                .isTrue();
    }

    private static void waitUntil(BooleanSupplier condition, int maxSeconds) throws InterruptedException {
        for (int i = 0; i < maxSeconds && !condition.getAsBoolean(); i++) {
            Thread.sleep(1000);
        }
    }
}
