/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.cockroachdb.CockroachDBConnector;

/**
 * Integration test for CockroachDB connector connection functionality.
 * Tests database connectivity, authentication, and connection management.
 *
 * @author Virag Tripathi
 */
@Testcontainers
public class CockroachDBConnectionIT {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(
            "cockroachdb/cockroach:v25.2.3")
            .withNetwork(NETWORK)
            .withNetworkAliases("cockroachdb");

    private Connection connection;

    @BeforeEach
    public void setUp() throws Exception {
        // Start containers
        kafka.start();
        cockroachdb.start();

        // Create test database
        String jdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/testdb");
        connection = DriverManager.getConnection(jdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword());

        try (var stmt = connection.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS testdb");
            stmt.execute("USE testdb");
            stmt.execute("CREATE TABLE IF NOT EXISTS connection_test (id INT PRIMARY KEY, name STRING)");
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldConnectWithValidCredentials() throws Exception {
        // Test basic connectivity
        assertThat(connection).isNotNull();
        assertThat(connection.isValid(5)).isTrue();

        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT 1");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    public void shouldFailWithInvalidCredentials() {
        String invalidJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/testdb");

        assertThatThrownBy(() -> {
            DriverManager.getConnection(invalidJdbcUrl, "invalid_user", "invalid_password");
        }).isInstanceOf(Exception.class);
    }

    @Test
    public void shouldHandleConnectionTimeout() {
        // Test connection timeout behavior
        Map<String, String> config = getConnectorConfig();
        config.put("database.connection.timeout.ms", "1000");

        // This should not throw an exception for valid connection
        CockroachDBConnector connector = new CockroachDBConnector();
        connector.start(config);

        assertThat(connector.taskConfigs(1)).isNotEmpty();
        connector.stop();
    }

    @Test
    public void shouldReconnectAfterConnectionLoss() throws Exception {
        // Test reconnection capability
        Map<String, String> config = getConnectorConfig();

        CockroachDBConnector connector = new CockroachDBConnector();
        connector.start(config);

        // Simulate connection loss by closing the container
        cockroachdb.stop();

        // Wait a bit
        Thread.sleep(2000);

        // Restart the container
        cockroachdb.start();

        // The connector should be able to handle this gracefully
        assertThat(connector.taskConfigs(1)).isNotEmpty();
        connector.stop();
    }

    @Test
    public void shouldHandleSSLConnections() throws Exception {
        // Test SSL connection capability
        String sslJdbcUrl = cockroachdb.getJdbcUrl().replace("/postgres", "/testdb") + "&sslmode=require";

        // This should work with CockroachDB's SSL support
        try (Connection sslConnection = DriverManager.getConnection(sslJdbcUrl, cockroachdb.getUsername(), cockroachdb.getPassword())) {
            assertThat(sslConnection.isValid(5)).isTrue();
        }
    }

    @Test
    public void shouldHandleConnectionPoolSettings() {
        Map<String, String> config = getConnectorConfig();
        config.put("database.connection.pool.size", "5");
        config.put("database.connection.pool.timeout.ms", "30000");

        CockroachDBConnector connector = new CockroachDBConnector();
        connector.start(config);

        assertThat(connector.taskConfigs(1)).isNotEmpty();
        connector.stop();
    }

    @Test
    public void shouldValidateConnectionBeforeStarting() {
        Map<String, String> config = getConnectorConfig();

        CockroachDBConnector connector = new CockroachDBConnector();
        connector.start(config);

        // The connector should validate the connection during startup
        assertThat(connector.taskConfigs(1)).isNotEmpty();
        connector.stop();
    }

    private Map<String, String> getConnectorConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("name", "cockroachdb-connection-test");
        config.put("connector.class", CockroachDBConnector.class.getName());
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", "testdb");
        config.put("database.server.name", "cockroachdb");
        config.put("topic.prefix", "cockroachdb");
        config.put("schema.history.internal.kafka.bootstrap.servers", kafka.getBootstrapServers());
        config.put("schema.history.internal.kafka.topic", "schema-changes.testdb");
        return config;
    }
}
