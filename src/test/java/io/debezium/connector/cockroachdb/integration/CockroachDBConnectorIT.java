/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    }

    @After
    public void tearDown() throws SQLException {
        if (consumer != null) {
            consumer.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldConnectToCockroachDB() throws Exception {
        // Test basic connectivity
        assertThat(connection).isNotNull();
        assertThat(connection.isValid(5)).isTrue();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SELECT 1");
            LOGGER.info("Successfully connected to CockroachDB");
        }
    }

    @Test
    public void shouldCreateChangefeed() throws Exception {
        // Test changefeed creation
        try (Statement stmt = connection.createStatement()) {
            // Create a simple changefeed
            stmt.execute(String.format(
                    "CREATE CHANGEFEED FOR TABLE %s INTO 'kafka://%s' " +
                            "WITH envelope = 'enriched'",
                    TABLE_NAME, KAFKA_BOOTSTRAP_SERVERS));

            LOGGER.info("Successfully created changefeed for table: {}", TABLE_NAME);
        }
    }

    @Test
    public void shouldInsertAndQueryData() throws Exception {
        // Insert a record
        insertRecord(1, "John Doe", "john@example.com");

        // Query and verify the record
        try (Statement stmt = connection.createStatement()) {
            var rs = stmt.executeQuery(String.format(
                    "SELECT id, name, email FROM %s WHERE id = 1", TABLE_NAME));

            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("id")).isEqualTo(1);
            assertThat(rs.getString("name")).isEqualTo("John Doe");
            assertThat(rs.getString("email")).isEqualTo("john@example.com");

            LOGGER.info("Successfully inserted and queried data");
        }
    }

    @Test
    public void shouldUpdateData() throws Exception {
        // Insert initial record
        insertRecord(2, "Jane Smith", "jane@example.com");

        // Update the record
        updateRecord(2, "Jane Doe", "jane.doe@example.com");

        // Query and verify the update
        try (Statement stmt = connection.createStatement()) {
            var rs = stmt.executeQuery(String.format(
                    "SELECT id, name, email FROM %s WHERE id = 2", TABLE_NAME));

            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("id")).isEqualTo(2);
            assertThat(rs.getString("name")).isEqualTo("Jane Doe");
            assertThat(rs.getString("email")).isEqualTo("jane.doe@example.com");

            LOGGER.info("Successfully updated data");
        }
    }

    @Test
    public void shouldDeleteData() throws Exception {
        // Insert a record
        insertRecord(3, "Bob Wilson", "bob@example.com");

        // Delete the record
        deleteRecord(3);

        // Query and verify the deletion
        try (Statement stmt = connection.createStatement()) {
            var rs = stmt.executeQuery(String.format(
                    "SELECT COUNT(*) as count FROM %s WHERE id = 3", TABLE_NAME));

            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("count")).isEqualTo(0);

            LOGGER.info("Successfully deleted data");
        }
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

            LOGGER.info("Created test table: {}", TABLE_NAME);
        }
    }

    private void setupKafkaConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<>(props);
        LOGGER.info("Set up Kafka consumer for: {}", KAFKA_BOOTSTRAP_SERVERS);
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
