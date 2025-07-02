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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for CockroachDB connector with changefeed support.
 * Tests the full workflow from database setup to change event consumption.
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnectorIT.class);

    private static final String DATABASE_NAME = "testdb";
    private static final String TABLE_NAME = "users";
    private static final String TOPIC_PREFIX = "test-cockroachdb";

    // Create a shared network for all containers
    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    private static final GenericContainer<?> cockroachdb = new GenericContainer<>(DockerImageName.parse("cockroachdb/cockroach:v25.2.2"))
            .withNetwork(NETWORK)
            .withNetworkAliases("cockroachdb")
            .withExposedPorts(26257, 8080)
            .withCommand("start-single-node", "--insecure")
            .withEnv("COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", "true");

    private Connection connection;
    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    public void setUp() throws SQLException {
        // Wait for containers to be ready
        kafka.start();
        cockroachdb.start();

        // First connect to default database to create our test database
        String defaultUrl = String.format("jdbc:postgresql://%s:%d/defaultdb?sslmode=disable",
                cockroachdb.getHost(), cockroachdb.getMappedPort(26257));
        try (Connection defaultConnection = DriverManager.getConnection(defaultUrl, "root", "")) {
            try (Statement stmt = defaultConnection.createStatement()) {
                stmt.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
                LOGGER.info("Created database: {}", DATABASE_NAME);
            }
        }

        // Connect to our test database
        String url = String.format("jdbc:postgresql://%s:%d/%s?sslmode=disable",
                cockroachdb.getHost(), cockroachdb.getMappedPort(26257), DATABASE_NAME);
        connection = DriverManager.getConnection(url, "root", "");

        // Enable changefeeds
        enableChangefeeds();

        // Create test table
        createTestTable();

        // Set up Kafka consumer
        setupKafkaConsumer();
    }

    @AfterEach
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
            // Create a changefeed with enriched envelope (required for Debezium compatibility)
            stmt.execute(String.format(
                    "CREATE CHANGEFEED FOR TABLE %s INTO 'kafka://kafka:9092' " +
                            "WITH envelope = 'enriched', " +
                            "enriched_properties = 'source,schema', " +
                            "diff, " +
                            "updated, " +
                            "resolved = '10s'",
                    TABLE_NAME));

            LOGGER.info("Successfully created changefeed for table: {}", TABLE_NAME);

            // Wait a moment for the changefeed to start
            Thread.sleep(2000);

            // Verify the changefeed is running
            var rs = stmt.executeQuery("SHOW CHANGEFEED JOBS");
            assertThat(rs.next()).isTrue();
            String status = rs.getString("status");
            assertThat(status).isEqualTo("running");

            LOGGER.info("Changefeed job is running with status: {}", status);
        }
    }

    @Test
    public void shouldProduceChangeEvents() throws Exception {
        // Create changefeed first
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE CHANGEFEED FOR TABLE %s INTO 'kafka://kafka:9092' " +
                            "WITH envelope = 'enriched', " +
                            "enriched_properties = 'source,schema', " +
                            "diff, " +
                            "updated, " +
                            "resolved = '10s'",
                    TABLE_NAME));

            // Wait for changefeed to start
            Thread.sleep(3000);
        }

        // Insert a record to trigger change event
        insertRecord(4, "Test User", "test@example.com");

        // Wait for change event to be produced
        Thread.sleep(2000);

        // Subscribe to the topic
        String topicName = TABLE_NAME; // CockroachDB uses table name as topic name
        consumer.subscribe(java.util.Arrays.asList(topicName));

        // Poll for messages
        var records = consumer.poll(java.time.Duration.ofSeconds(10));

        // Verify we received change events
        assertThat(records).isNotEmpty();

        for (var record : records) {
            String value = record.value();
            LOGGER.info("Received change event: {}", value);

            // Verify it's a valid JSON with enriched envelope
            assertThat(value).contains("\"op\":");
            assertThat(value).contains("\"source\":");
            assertThat(value).contains("\"after\":");
        }

        LOGGER.info("Successfully received {} change events", records.count());
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
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<>(props);
        LOGGER.info("Set up Kafka consumer for: {}", kafka.getBootstrapServers());
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
