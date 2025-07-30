/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.CockroachDBConnectorConfig;

/**
 * Tests for CockroachDB connection handling.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectionTest {

    private CockroachDBConnection connection;
    private CockroachDBConnectorConfig config;

    @BeforeEach
    public void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        config = new CockroachDBConnectorConfig(Configuration.from(props));
        connection = new CockroachDBConnection(config);
    }

    @Test
    public void shouldCreateConnection() {
        // This test would require a real database connection
        // For now, we'll test the configuration
        assertThat(connection).isNotNull();
        assertThat(config.getHostname()).isEqualTo("localhost");
        assertThat(config.getPort()).isEqualTo(26257);
        assertThat(config.getUser()).isEqualTo("root");
        assertThat(config.getDatabaseName()).isEqualTo("testdb");
    }

    @Test
    public void shouldHandleConnectionFailure() {
        // Test with invalid connection parameters
        Map<String, String> invalidProps = new HashMap<>();
        invalidProps.put("database.hostname", "invalid-host");
        invalidProps.put("database.port", "26257");
        invalidProps.put("database.user", "root");
        invalidProps.put("database.password", "");
        invalidProps.put("database.dbname", "testdb");
        invalidProps.put("database.server.name", "test-server");
        invalidProps.put("topic.prefix", "test");

        CockroachDBConnectorConfig invalidConfig = new CockroachDBConnectorConfig(Configuration.from(invalidProps));
        CockroachDBConnection invalidConnection = new CockroachDBConnection(invalidConfig);

        // The connection should be created but may fail when actually connecting
        assertThat(invalidConnection).isNotNull();
    }

    @Test
    public void shouldHandleNullPassword() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", null);
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnectorConfig configWithNullPassword = new CockroachDBConnectorConfig(Configuration.from(props));
        CockroachDBConnection connectionWithNullPassword = new CockroachDBConnection(configWithNullPassword);

        assertThat(connectionWithNullPassword).isNotNull();
    }

    @Test
    public void shouldHandleEmptyPassword() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnectorConfig configWithEmptyPassword = new CockroachDBConnectorConfig(Configuration.from(props));
        CockroachDBConnection connectionWithEmptyPassword = new CockroachDBConnection(configWithEmptyPassword);

        assertThat(connectionWithEmptyPassword).isNotNull();
    }

    @Test
    public void shouldHandleSSLConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("database.sslmode", "require");

        CockroachDBConnectorConfig sslConfig = new CockroachDBConnectorConfig(Configuration.from(props));
        CockroachDBConnection sslConnection = new CockroachDBConnection(sslConfig);

        assertThat(sslConnection).isNotNull();
        assertThat(sslConfig.getSslMode()).isEqualTo("require");
    }

    @Test
    public void shouldHandleConnectionTimeout() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("connection.timeout.ms", "5000");

        CockroachDBConnectorConfig timeoutConfig = new CockroachDBConnectorConfig(Configuration.from(props));
        CockroachDBConnection timeoutConnection = new CockroachDBConnection(timeoutConfig);

        assertThat(timeoutConnection).isNotNull();
        assertThat(timeoutConfig.getConnectionTimeoutMs()).isEqualTo(5000L);
    }

    @Test
    public void shouldHandleOnConnectStatements() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("database.initial.statements", "SET timezone='UTC'; SET application_name='debezium'");

        CockroachDBConnectorConfig onConnectConfig = new CockroachDBConnectorConfig(Configuration.from(props));
        CockroachDBConnection onConnectConnection = new CockroachDBConnection(onConnectConfig);

        assertThat(onConnectConnection).isNotNull();
        assertThat(onConnectConfig.getOnConnectStatements())
                .isEqualTo("SET timezone='UTC'; SET application_name='debezium'");
    }

    @Test
    public void shouldHandleReadOnlyConnection() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("read.only", "true");

        CockroachDBConnectorConfig readOnlyConfig = new CockroachDBConnectorConfig(Configuration.from(props));
        CockroachDBConnection readOnlyConnection = new CockroachDBConnection(readOnlyConfig);

        assertThat(readOnlyConnection).isNotNull();
        assertThat(readOnlyConfig.isReadOnlyConnection()).isTrue();
    }

    @Test
    public void shouldHandleTCPKeepAlive() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("database.tcpKeepAlive", "true");

        CockroachDBConnectorConfig tcpKeepAliveConfig = new CockroachDBConnectorConfig(Configuration.from(props));
        CockroachDBConnection tcpKeepAliveConnection = new CockroachDBConnection(tcpKeepAliveConfig);

        assertThat(tcpKeepAliveConnection).isNotNull();
        assertThat(tcpKeepAliveConfig.isTcpKeepAlive()).isTrue();
    }
}