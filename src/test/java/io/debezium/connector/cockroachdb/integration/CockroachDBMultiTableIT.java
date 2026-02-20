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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Integration test for multi-table concurrent changefeed support.
 *
 * <p>Validates that the connector creates a single multi-table changefeed
 * ({@code CREATE CHANGEFEED FOR table1, table2}) and correctly routes events
 * from per-table Kafka topics to the appropriate Debezium output topics.</p>
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBMultiTableIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBMultiTableIT.class);

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v26.1.0");
    private static final String DATABASE_NAME = "multi_table_testdb";
    private static final String ORDERS_TABLE = "mt_orders";
    private static final String CUSTOMERS_TABLE = "mt_customers";

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

            stmt.execute("CREATE TABLE IF NOT EXISTS " + ORDERS_TABLE + " ("
                    + "id INT PRIMARY KEY, "
                    + "customer_name STRING NOT NULL, "
                    + "amount DECIMAL(10,2), "
                    + "status STRING DEFAULT 'PENDING'"
                    + ")");

            stmt.execute("CREATE TABLE IF NOT EXISTS " + CUSTOMERS_TABLE + " ("
                    + "id INT PRIMARY KEY, "
                    + "name STRING NOT NULL, "
                    + "email STRING, "
                    + "tier STRING DEFAULT 'standard'"
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
    public void shouldCaptureEventsFromMultipleTables() throws Exception {
        // Insert data into both tables before starting the connector
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + ORDERS_TABLE + " VALUES (1, 'Alice', 100.00, 'PENDING')");
            stmt.execute("INSERT INTO " + ORDERS_TABLE + " VALUES (2, 'Bob', 200.50, 'PROCESSING')");
            stmt.execute("INSERT INTO " + CUSTOMERS_TABLE + " VALUES (1, 'Alice', 'alice@example.com', 'gold')");
            stmt.execute("INSERT INTO " + CUSTOMERS_TABLE + " VALUES (2, 'Bob', 'bob@example.com', 'standard')");
        }
        LOGGER.info("Inserted test data into both tables");

        Thread.sleep(2000);

        // CockroachDB uses internal Docker address to publish to Kafka
        // The connector JVM uses the host-mapped address to consume
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", "multi-table-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "mt-test");
        config.put("topic.prefix", "mt-test");
        config.put("table.include.list", "public." + ORDERS_TABLE + ",public." + CUSTOMERS_TABLE);
        config.put("cockroachdb.skip.permission.check", "true");
        config.put("cockroachdb.schema.name", "public");
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", hostBootstrap);
        config.put("cockroachdb.changefeed.envelope", "enriched");
        config.put("cockroachdb.changefeed.enriched.properties", "source,schema");
        config.put("cockroachdb.changefeed.include.diff", "true");
        config.put("cockroachdb.changefeed.include.updated", "true");
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("cockroachdb.changefeed.resolved.interval", "5s");
        config.put("snapshot.mode", "initial");
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

        boolean didStart = started.await(30, TimeUnit.SECONDS);
        assertThat(didStart).as("Task should start within 30 seconds").isTrue();
        assertThat(taskError.get()).as("Task should start without error").isNull();

        // Poll for records from both tables
        List<SourceRecord> allRecords = new ArrayList<>();
        Set<String> topicsSeen = new HashSet<>();
        int maxAttempts = 40;

        for (int i = 0; i < maxAttempts; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null && !records.isEmpty()) {
                    allRecords.addAll(records);
                    for (SourceRecord record : records) {
                        topicsSeen.add(record.topic());
                        LOGGER.info("  Record: topic={}, key={}, op={}",
                                record.topic(), record.key(),
                                record.value() instanceof Struct ? ((Struct) record.value()).get("op") : "?");
                    }
                    LOGGER.info("Poll attempt {}: {} records (total: {}, topics: {})",
                            i + 1, records.size(), allRecords.size(), topicsSeen);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            catch (Exception e) {
                LOGGER.warn("Poll attempt {} failed: {}", i + 1, e.getMessage());
            }
            Thread.sleep(1000);
        }

        LOGGER.info("Multi-table IT collected {} SourceRecords from {} topic(s): {}",
                allRecords.size(), topicsSeen.size(), topicsSeen);

        // Verify we received records
        assertThat(allRecords).as("Should receive SourceRecords from multi-table changefeed").isNotEmpty();

        // Verify records come from both tables (topic names contain the table name)
        boolean hasOrderRecords = topicsSeen.stream().anyMatch(t -> t.contains(ORDERS_TABLE));
        boolean hasCustomerRecords = topicsSeen.stream().anyMatch(t -> t.contains(CUSTOMERS_TABLE));
        assertThat(hasOrderRecords).as("Should have records from " + ORDERS_TABLE).isTrue();
        assertThat(hasCustomerRecords).as("Should have records from " + CUSTOMERS_TABLE).isTrue();

        // Verify each record has required fields
        for (SourceRecord record : allRecords) {
            assertThat(record.topic()).isNotNull();
            assertThat(record.sourcePartition()).isNotNull();
            assertThat(record.sourceOffset()).isNotNull();
        }

        LOGGER.info("Multi-table IT passed: {} records from {} topic(s)", allRecords.size(), topicsSeen.size());
    }

    @Test
    public void shouldCaptureInsertsAfterChangefeedCreation() throws Exception {
        String hostBootstrap = kafka.getBootstrapServers().replaceFirst("^PLAINTEXT://", "");

        Map<String, String> config = new HashMap<>();
        config.put("name", "multi-table-insert-test");
        config.put("connector.class", "io.debezium.connector.cockroachdb.CockroachDBConnector");
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", DATABASE_NAME);
        config.put("database.sslmode", "disable");
        config.put("database.server.name", "mt-insert-test");
        config.put("topic.prefix", "mt-insert-test");
        config.put("table.include.list", "public." + ORDERS_TABLE + ",public." + CUSTOMERS_TABLE);
        config.put("cockroachdb.skip.permission.check", "true");
        config.put("cockroachdb.schema.name", "public");
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        config.put("cockroachdb.changefeed.kafka.bootstrap.servers", hostBootstrap);
        config.put("cockroachdb.changefeed.envelope", "enriched");
        config.put("cockroachdb.changefeed.enriched.properties", "source,schema");
        config.put("cockroachdb.changefeed.include.diff", "true");
        config.put("cockroachdb.changefeed.include.updated", "true");
        config.put("cockroachdb.changefeed.kafka.auto.offset.reset", "earliest");
        config.put("cockroachdb.changefeed.kafka.poll.timeout.ms", "1000");
        config.put("cockroachdb.changefeed.resolved.interval", "5s");
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

        boolean didStart = started.await(30, TimeUnit.SECONDS);
        assertThat(didStart).as("Task should start within 30 seconds").isTrue();
        assertThat(taskError.get()).as("Task should start without error").isNull();

        // Wait for changefeed to be ready, then insert
        Thread.sleep(5000);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO " + ORDERS_TABLE + " VALUES (10, 'NewOrder', 99.99, 'NEW')");
            stmt.execute("INSERT INTO " + CUSTOMERS_TABLE + " VALUES (10, 'NewCustomer', 'new@test.com', 'premium')");
        }

        // Poll for the insert events
        List<SourceRecord> allRecords = new ArrayList<>();
        Set<String> topicsSeen = new HashSet<>();
        int maxAttempts = 30;

        for (int i = 0; i < maxAttempts; i++) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null && !records.isEmpty()) {
                    allRecords.addAll(records);
                    for (SourceRecord record : records) {
                        topicsSeen.add(record.topic());
                        LOGGER.info("  Record: topic={}, key={}", record.topic(), record.key());
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            catch (Exception e) {
                LOGGER.warn("Poll attempt {} failed: {}", i + 1, e.getMessage());
            }
            Thread.sleep(1000);
        }

        LOGGER.info("Insert test collected {} SourceRecords from topics: {}", allRecords.size(), topicsSeen);

        assertThat(allRecords).as("Should receive insert events").isNotEmpty();

        boolean hasOrderRecords = topicsSeen.stream().anyMatch(t -> t.contains(ORDERS_TABLE));
        boolean hasCustomerRecords = topicsSeen.stream().anyMatch(t -> t.contains(CUSTOMERS_TABLE));
        assertThat(hasOrderRecords).as("Should have insert from " + ORDERS_TABLE).isTrue();
        assertThat(hasCustomerRecords).as("Should have insert from " + CUSTOMERS_TABLE).isTrue();
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
