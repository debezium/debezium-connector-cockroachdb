/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.EmbeddedEngineConfig;

/**
 * Integration test for CockroachDB connector with changefeed support.
 * Tests the full workflow from database setup to change event consumption.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnectorIT.class);

    private static final String COCKROACHDB_HOST = System.getProperty("cockroachdb.host", "localhost");
    private static final int COCKROACHDB_PORT = Integer.parseInt(System.getProperty("cockroachdb.port", "26257"));
    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092");

    private static final String DATABASE_NAME = "testdb";
    private static final String TABLE_NAME = "users";
    private static final String TOPIC_PREFIX = "test-cockroachdb";

    private Connection connection;
    private EmbeddedEngine engine;
    private KafkaConsumer<String, String> consumer;

    @Before
    public void setUp() throws SQLException {
        // Connect to CockroachDB
        String url = String.format("jdbc:postgresql://%s:%d/%s?sslmode=disable",
                COCKROACHDB_HOST, COCKROACHDB_PORT, DATABASE_NAME);
        connection = DriverManager.getConnection(url, "root", "");

        // Enable changefeeds
        enableChangefeeds();

        // Create test table
        createTestTable();

        // Set up Kafka consumer
        setupKafkaConsumer();

        // Start the connector
        startConnector();
    }

    @After
    public void tearDown() throws SQLException {
        if (engine != null) {
            engine.stop();
        }
        if (consumer != null) {
            consumer.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldCaptureInsertEvents() throws Exception {
        // Insert a record
        insertRecord(1, "John Doe", "john@example.com");

        // Wait for and verify the change event
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            ConsumerRecord<String, String> record = consumer.poll(Duration.ofMillis(100)).iterator().next();
            if (record != null) {
                String value = record.value();
                LOGGER.info("Received change event: {}", value);

                // Verify it contains the inserted data
                assertThat(value).contains("John Doe");
                assertThat(value).contains("john@example.com");
                assertThat(value).contains("\"after\"");
                return true;
            }
            return false;
        });
    }

    @Test
    public void shouldCaptureUpdateEvents() throws Exception {
        // Insert initial record
        insertRecord(1, "John Doe", "john@example.com");

        // Wait a bit for the insert to be processed
        Thread.sleep(2000);

        // Update the record
        updateRecord(1, "Jane Doe", "jane@example.com");

        // Wait for and verify the update event
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            ConsumerRecord<String, String> record = consumer.poll(Duration.ofMillis(100)).iterator().next();
            if (record != null) {
                String value = record.value();
                LOGGER.info("Received update event: {}", value);

                // Verify it contains both before and after data
                assertThat(value).contains("Jane Doe");
                assertThat(value).contains("jane@example.com");
                assertThat(value).contains("\"before\"");
                assertThat(value).contains("\"after\"");
                return true;
            }
            return false;
        });
    }

    @Test
    public void shouldCaptureDeleteEvents() throws Exception {
        // Insert initial record
        insertRecord(1, "John Doe", "john@example.com");

        // Wait a bit for the insert to be processed
        Thread.sleep(2000);

        // Delete the record
        deleteRecord(1);

        // Wait for and verify the delete event
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            ConsumerRecord<String, String> record = consumer.poll(Duration.ofMillis(100)).iterator().next();
            if (record != null) {
                String value = record.value();
                LOGGER.info("Received delete event: {}", value);

                // Verify it contains the before data (deleted record)
                assertThat(value).contains("John Doe");
                assertThat(value).contains("john@example.com");
                assertThat(value).contains("\"before\"");
                return true;
            }
            return false;
        });
    }

    @Test
    public void shouldHandleEnrichedEnvelope() throws Exception {
        // Insert a record with complex data
        insertRecord(1, "John Doe", "john@example.com");

        // Wait for and verify the enriched envelope structure
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            ConsumerRecord<String, String> record = consumer.poll(Duration.ofMillis(100)).iterator().next();
            if (record != null) {
                String value = record.value();
                LOGGER.info("Received enriched envelope: {}", value);

                // Verify enriched envelope fields
                assertThat(value).contains("\"after\"");
                assertThat(value).contains("\"updated\"");
                // Note: diff and resolved fields may not be present in all events
                return true;
            }
            return false;
        });
    }

    private void enableChangefeeds() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Enable changefeeds for the database
            stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
            LOGGER.info("Enabled changefeeds for CockroachDB");
        }
    }

    private void createTestTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Create test table
            stmt.execute(String.format(
                    "CREATE TABLE IF NOT EXISTS %s (" +
                            "id INT PRIMARY KEY, " +
                            "name STRING NOT NULL, " +
                            "email STRING UNIQUE, " +
                            "created_at TIMESTAMP DEFAULT NOW()" +
                            ")",
                    TABLE_NAME));

            // Create changefeed with enriched envelope
            stmt.execute(String.format(
                    "CREATE CHANGEFEED FOR TABLE %s INTO 'kafka://%s' " +
                            "WITH updated, diff, resolved = '10s', " +
                            "envelope = 'enriched'",
                    TABLE_NAME, KAFKA_BOOTSTRAP_SERVERS));

            LOGGER.info("Created test table and changefeed: {}", TABLE_NAME);
        }
    }

    private void setupKafkaConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(java.util.Arrays.asList(TOPIC_PREFIX + "." + DATABASE_NAME + "." + TABLE_NAME));
    }

    private void startConnector() {
        Map<String, String> config = new HashMap<>();
        config.put("name", "cockroachdb-connector");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", COCKROACHDB_HOST);
        config.put("database.port", String.valueOf(COCKROACHDB_PORT));
        config.put("database.user", "root");
        config.put("database.password", "");
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.server.name", "test-server");
        config.put("topic.prefix", TOPIC_PREFIX);
        config.put("snapshot.mode", "never");
        config.put("database.history.kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        config.put("database.history.kafka.topic", "dbhistory.cockroachdb");

        EmbeddedEngineConfig engineConfig = EmbeddedEngineConfig.fromConfig(config);
        engine = EmbeddedEngine.create()
                .using(engineConfig)
                .notifying(this::handleEvent)
                .build();

        engine.start();
        LOGGER.info("Started CockroachDB connector");
    }

    private void handleEvent(org.apache.kafka.connect.source.SourceRecord record) {
        LOGGER.info("Received source record: {}", record);
    }

    private void insertRecord(int id, String name, String email) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "INSERT INTO %s (id, name, email) VALUES (%d, '%s', '%s')",
                    TABLE_NAME, id, name, email));
            LOGGER.info("Inserted record: id={}, name={}, email={}", id, name, email);
        }
    }

    private void updateRecord(int id, String name, String email) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "UPDATE %s SET name = '%s', email = '%s' WHERE id = %d",
                    TABLE_NAME, name, email, id));
            LOGGER.info("Updated record: id={}, name={}, email={}", id, name, email);
        }
    }

    private void deleteRecord(int id) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("DELETE FROM %s WHERE id = %d", TABLE_NAME, id));
            LOGGER.info("Deleted record: id={}", id);
        }
    }
}
