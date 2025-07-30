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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Integration test for CockroachDB connector snapshot functionality.
 * Tests initial snapshot capture and streaming mode transitions.
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBSnapshotIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSnapshotIT.class);

    private static final String DATABASE_NAME = "snapshot_testdb";
    private static final String TABLE_NAME = "products";
    private static final String TOPIC_PREFIX = "snapshot-test";

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(
            DockerImageName.parse("cockroachdb/cockroach:v25.2.3"))
            .withNetwork(NETWORK)
            .withNetworkAliases("cockroachdb");

    private Connection connection;
    private KafkaConsumer<String, String> consumer;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() throws Exception {
        kafka.start();
        cockroachdb.start();

        // Create test database
        String defaultJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb");
        try (Connection defaultConnection = DriverManager.getConnection(defaultJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword())) {
            try (Statement stmt = defaultConnection.createStatement()) {
                stmt.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
            }
        }

        // Connect to test database
        String testJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/" + DATABASE_NAME);
        connection = DriverManager.getConnection(testJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword());

        // Enable changefeeds
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
        }

        // Create test table and populate with initial data
        createTestTable();
        populateInitialData();

        // Setup Kafka consumer
        setupKafkaConsumer();
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @Disabled("Snapshot functionality not yet implemented - TODO: implement snapshot mode")
    public void shouldCaptureInitialSnapshot() throws Exception {
        // Create changefeed for snapshot
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE CHANGEFEED FOR TABLE %s INTO 'kafka://kafka:9092' " +
                            "WITH envelope = 'enriched', " +
                            "enriched_properties = 'source,schema', " +
                            "diff, " +
                            "updated, " +
                            "resolved = '5s', " +
                            "initial_scan = 'yes'",
                    TABLE_NAME));

            LOGGER.info("Created changefeed with initial scan for table: {}", TABLE_NAME);
        }

        // Wait for initial scan to complete
        Thread.sleep(5000);

        // Subscribe to topic
        String topicName = TABLE_NAME;
        consumer.subscribe(java.util.Arrays.asList(topicName));

        // Poll for snapshot records
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));

        // Verify we received snapshot records
        assertThat(records).isNotEmpty();
        LOGGER.info("Received {} snapshot records", records.count());

        int snapshotRecordCount = 0;
        for (ConsumerRecord<String, String> record : records) {
            String value = record.value();
            LOGGER.info("Received snapshot record: {}", value); // Log the raw record for debugging
            JsonNode jsonNode = objectMapper.readTree(value);

            // Verify snapshot record structure with null checks
            JsonNode opNode = jsonNode.get("op");
            JsonNode sourceNode = jsonNode.get("source");
            JsonNode afterNode = jsonNode.get("after");

            if (opNode != null && sourceNode != null && afterNode != null) {
                String operation = opNode.asText();
                if ("r".equals(operation)) { // Read operation for snapshot
                    snapshotRecordCount++;

                    // Verify data integrity with null checks
                    JsonNode idNode = afterNode.get("id");
                    JsonNode nameNode = afterNode.get("name");
                    JsonNode priceNode = afterNode.get("price");

                    if (idNode != null && nameNode != null && priceNode != null) {
                        assertThat(idNode.asInt()).isGreaterThan(0);
                        assertThat(nameNode.asText()).isNotEmpty();
                        assertThat(priceNode.asDouble()).isGreaterThan(0);
                    }
                    else {
                        LOGGER.warn("Snapshot record missing expected fields: id={}, name={}, price={}",
                                idNode, nameNode, priceNode);
                    }
                }
            }
            else {
                LOGGER.warn("Snapshot record missing required fields: op={}, source={}, after={}",
                        opNode, sourceNode, afterNode);
            }
        }

        // Should have received snapshot records for all initial data
        assertThat(snapshotRecordCount).isGreaterThan(0);
        LOGGER.info("Successfully captured {} snapshot records", snapshotRecordCount);
    }

    @Test
    @Disabled("Snapshot functionality not yet implemented - TODO: implement snapshot mode")
    public void shouldHandleSnapshotWithLargeDataset() throws Exception {
        // Insert large dataset
        insertLargeDataset(100);

        // Create changefeed with initial scan
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE CHANGEFEED FOR TABLE %s INTO 'kafka://kafka:9092' " +
                            "WITH envelope = 'enriched', " +
                            "enriched_properties = 'source,schema', " +
                            "diff, " +
                            "updated, " +
                            "resolved = '5s', " +
                            "initial_scan = 'yes'",
                    TABLE_NAME));
        }

        // Wait for initial scan
        Thread.sleep(10000);

        // Subscribe and consume
        String topicName = TABLE_NAME;
        consumer.subscribe(java.util.Arrays.asList(topicName));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));

        // Should receive records for large dataset
        assertThat(records.count()).isGreaterThanOrEqualTo(100);
        LOGGER.info("Successfully handled snapshot with {} records", records.count());
    }

    @Test
    @Disabled("Snapshot functionality not yet implemented - TODO: implement snapshot mode")
    public void shouldTransitionToStreamingAfterSnapshot() throws Exception {
        // Create changefeed
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE CHANGEFEED FOR TABLE %s INTO 'kafka://kafka:9092' " +
                            "WITH envelope = 'enriched', " +
                            "enriched_properties = 'source,schema', " +
                            "diff, " +
                            "updated, " +
                            "resolved = '5s', " +
                            "initial_scan = 'yes'",
                    TABLE_NAME));
        }

        // Wait for initial scan
        Thread.sleep(5000);

        // Subscribe to topic
        String topicName = TABLE_NAME;
        consumer.subscribe(java.util.Arrays.asList(topicName));

        // Consume initial snapshot
        ConsumerRecords<String, String> snapshotRecords = consumer.poll(Duration.ofSeconds(30));
        assertThat(snapshotRecords).isNotEmpty();

        // Insert new data to test streaming
        insertRecord(999, "Streaming Test Product", 99.99);

        // Wait for streaming event
        Thread.sleep(3000);

        // Poll for streaming records
        ConsumerRecords<String, String> streamingRecords = consumer.poll(Duration.ofSeconds(10));

        // Should receive streaming event
        assertThat(streamingRecords).isNotEmpty();

        boolean foundStreamingEvent = false;
        for (ConsumerRecord<String, String> record : streamingRecords) {
            String value = record.value();
            LOGGER.info("Received streaming record: {}", value); // Log the raw record for debugging
            JsonNode jsonNode = objectMapper.readTree(value);

            JsonNode opNode = jsonNode.get("op");
            if (opNode != null) {
                String operation = opNode.asText();
                if ("c".equals(operation)) { // Create operation
                    JsonNode after = jsonNode.get("after");
                    if (after != null) {
                        JsonNode idNode = after.get("id");
                        if (idNode != null && idNode.asInt() == 999) {
                            foundStreamingEvent = true;
                            break;
                        }
                    }
                    else {
                        LOGGER.warn("Streaming record missing 'after' field: {}", value);
                    }
                }
            }
            else {
                LOGGER.warn("Streaming record missing 'op' field: {}", value);
            }
        }

        assertThat(foundStreamingEvent).isTrue();
        LOGGER.info("Successfully transitioned from snapshot to streaming mode");
    }

    @Test
    @Disabled("Snapshot functionality not yet implemented - TODO: implement snapshot mode")
    public void shouldHandleSnapshotInterruption() throws Exception {
        // Start changefeed
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE CHANGEFEED FOR TABLE %s INTO 'kafka://kafka:9092' " +
                            "WITH envelope = 'enriched', " +
                            "enriched_properties = 'source,schema', " +
                            "diff, " +
                            "updated, " +
                            "resolved = '5s', " +
                            "initial_scan = 'yes'",
                    TABLE_NAME));
        }

        // Wait a bit for snapshot to start
        Thread.sleep(2000);

        // Stop the changefeed
        try (Statement stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SHOW CHANGEFEED JOBS");
            if (rs.next()) {
                long jobId = rs.getLong("job_id");
                stmt.execute("CANCEL JOB " + jobId);
                LOGGER.info("Cancelled changefeed job: {}", jobId);
            }
        }

        // Wait for cancellation
        Thread.sleep(3000);

        // Verify job is cancelled
        try (Statement stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SHOW CHANGEFEED JOBS");
            if (rs.next()) {
                String status = rs.getString("status");
                assertThat(status).isEqualTo("canceled");
                LOGGER.info("Changefeed job successfully cancelled");
            }
        }
    }

    private void createTestTable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE TABLE IF NOT EXISTS %s (" +
                            "id INT PRIMARY KEY, " +
                            "name STRING NOT NULL, " +
                            "price DECIMAL(10,2), " +
                            "category STRING, " +
                            "created_at TIMESTAMP DEFAULT NOW()" +
                            ")",
                    TABLE_NAME));
        }
    }

    private void populateInitialData() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            // Clear existing data first
            stmt.execute(String.format("DELETE FROM %s", TABLE_NAME));

            stmt.execute(String.format(
                    "INSERT INTO %s (id, name, price, category) VALUES " +
                            "(1, 'Laptop', 999.99, 'Electronics'), " +
                            "(2, 'Mouse', 29.99, 'Electronics'), " +
                            "(3, 'Keyboard', 79.99, 'Electronics'), " +
                            "(4, 'Monitor', 299.99, 'Electronics'), " +
                            "(5, 'Desk', 199.99, 'Furniture')",
                    TABLE_NAME));
        }
    }

    private void insertLargeDataset(int count) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            for (int i = 1; i <= count; i++) {
                stmt.execute(String.format(
                        "INSERT INTO %s (id, name, price, category) VALUES (%d, 'Product %d', %d.99, 'Category %d')",
                        TABLE_NAME, i + 100, i, i, (i % 5) + 1));
            }
        }
    }

    private void insertRecord(int id, String name, double price) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "INSERT INTO %s (id, name, price) VALUES (%d, '%s', %.2f)",
                    TABLE_NAME, id, name, price));
        }
    }

    private void setupKafkaConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "snapshot-test-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<>(props);
    }
}
