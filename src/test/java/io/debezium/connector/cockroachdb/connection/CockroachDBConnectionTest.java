/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

    @Test
    public void shouldClassifyRetryableTransactionStatesAsTransient() {
        // 40001 serialization_failure, 40P01 deadlock_detected, 40003 statement_completion_unknown
        // (ambiguous result; safe to retry at connection establishment where nothing is in flight).
        assertThat(CockroachDBConnection.isTransientError(new SQLException("retry", "40001"))).isTrue();
        assertThat(CockroachDBConnection.isTransientError(new SQLException("deadlock", "40P01"))).isTrue();
        assertThat(CockroachDBConnection.isTransientError(new SQLException("ambiguous", "40003"))).isTrue();
    }

    @Test
    public void shouldClassifyWholeConnectionExceptionClassAsTransient() {
        // pgjdbc raises 08001 for both connection-refused and unknown-host (debezium/dbz#2285),
        // so the whole 08 class must be transient, not an enumerated subset.
        for (String state : new String[]{ "08000", "08001", "08003", "08004", "08006", "08007", "08P01", "08S01" }) {
            assertThat(CockroachDBConnection.isTransientError(new SQLException("conn", state)))
                    .as("SQL state %s", state)
                    .isTrue();
        }
    }

    @Test
    public void shouldClassifyNodeShutdownAsTransient() {
        // 57P01 admin_shutdown is raised while a CockroachDB node drains during a rolling restart.
        assertThat(CockroachDBConnection.isTransientError(new SQLException("draining", "57P01"))).isTrue();
    }

    @Test
    public void shouldNotClassifyNonTransientStatesAsTransient() {
        assertThat(CockroachDBConnection.isTransientError(new SQLException("syntax", "42601"))).isFalse();
        assertThat(CockroachDBConnection.isTransientError(new SQLException("auth", "28P01"))).isFalse();
        assertThat(CockroachDBConnection.isTransientError(new SQLException("no state", (String) null))).isFalse();
    }

    private CockroachDBConnectorConfig configWith(Map<String, String> extra) {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "db.example.com");
        props.put("database.port", "26257");
        props.put("database.user", "cdc_user");
        props.put("database.password", "secret");
        props.put("database.dbname", "appdb");
        props.put("topic.prefix", "crdb");
        props.putAll(extra);
        return new CockroachDBConnectorConfig(Configuration.from(props));
    }

    @Test
    public void shouldBuildPlainUrlWhenSslDisabled() {
        CockroachDBConnectorConfig config = configWith(Map.of("database.sslmode", "disable"));
        assertThat(CockroachDBConnection.buildConnectionUrl(config))
                .isEqualTo("jdbc:postgresql://db.example.com:26257/appdb");
    }

    @Test
    public void shouldAppendSslModeToUrlForSecureModes() {
        assertThat(CockroachDBConnection.buildConnectionUrl(configWith(Map.of("database.sslmode", "require"))))
                .isEqualTo("jdbc:postgresql://db.example.com:26257/appdb?sslmode=require");
        assertThat(CockroachDBConnection.buildConnectionUrl(configWith(Map.of("database.sslmode", "verify-full"))))
                .isEqualTo("jdbc:postgresql://db.example.com:26257/appdb?sslmode=verify-full");
    }

    @Test
    public void shouldBuildCredentialAndTimeoutProperties() {
        CockroachDBConnectorConfig config = configWith(Map.of(
                "database.sslmode", "disable",
                "connection.timeout.ms", "30000"));
        Properties props = CockroachDBConnection.buildConnectionProperties(config);
        assertThat(props.getProperty("user")).isEqualTo("cdc_user");
        assertThat(props.getProperty("password")).isEqualTo("secret");
        assertThat(props.getProperty("connectTimeout")).as("milliseconds convert to seconds").isEqualTo("30");
        assertThat(props.getProperty("tcpKeepAlive")).as("keepalive defaults on").isEqualTo("true");
    }

    @Test
    public void shouldMapTlsFilesOnlyForSecureModes() {
        Map<String, String> tls = Map.of(
                "database.sslmode", "verify-full",
                "database.sslrootcert", "/certs/ca.crt",
                "database.sslcert", "/certs/client.crt",
                "database.sslkey", "/certs/client.key",
                "database.sslpassword", "keypass");
        Properties secure = CockroachDBConnection.buildConnectionProperties(configWith(tls));
        assertThat(secure.getProperty("sslrootcert")).isEqualTo("/certs/ca.crt");
        assertThat(secure.getProperty("sslcert")).isEqualTo("/certs/client.crt");
        assertThat(secure.getProperty("sslkey")).isEqualTo("/certs/client.key");
        assertThat(secure.getProperty("sslpassword")).isEqualTo("keypass");

        Map<String, String> disabled = new HashMap<>(tls);
        disabled.put("database.sslmode", "disable");
        Properties plain = CockroachDBConnection.buildConnectionProperties(configWith(disabled));
        assertThat(plain.getProperty("sslrootcert")).as("TLS files are ignored when SSL is disabled").isNull();
        assertThat(plain.getProperty("sslcert")).isNull();
    }

    @Test
    public void shouldOmitKeepAliveWhenDisabled() {
        CockroachDBConnectorConfig config = configWith(Map.of(
                "database.sslmode", "disable",
                "database.tcpKeepAlive", "false"));
        assertThat(CockroachDBConnection.buildConnectionProperties(config).getProperty("tcpKeepAlive")).isNull();
    }
}
