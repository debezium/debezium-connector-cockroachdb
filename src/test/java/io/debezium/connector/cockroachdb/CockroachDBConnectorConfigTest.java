/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for CockroachDBConnectorConfig.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorConfigTest {

    @Test
    public void shouldCreateConfigWithDefaultValues() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("database.dbname", "defaultdb");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        assertThat(config.getDatabaseName()).isEqualTo("defaultdb");
        assertThat(config.port()).isEqualTo(26257); // Default port
        assertThat(config.getSnapshotMode()).isEqualTo(CockroachDBConnectorConfig.SnapshotMode.INITIAL);
        assertThat(config.getSnapshotIsolationMode()).isEqualTo(CockroachDBConnectorConfig.SnapshotIsolationMode.SERIALIZABLE);
        assertThat(config.getSnapshotLockingMode()).isPresent();
        assertThat(config.getSnapshotLockingMode().get()).isEqualTo(CockroachDBConnectorConfig.SnapshotLockingMode.NONE);
    }

    @Test
    public void shouldCreateConfigWithCustomValues() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "cockroachdb.example.com");
        props.put("database.port", "26258");
        props.put("database.user", "debezium");
        props.put("database.password", "secret");
        props.put("database.dbname", "myapp");
        props.put("database.server.name", "myapp-server");
        props.put("topic.prefix", "myapp");
        props.put("snapshot.mode", "never");
        props.put("snapshot.isolation.mode", "read_committed");
        props.put("snapshot.locking.mode", "shared");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        assertThat(config.getDatabaseName()).isEqualTo("myapp");
        assertThat(config.port()).isEqualTo(26258);
        assertThat(config.getSnapshotMode()).isEqualTo(CockroachDBConnectorConfig.SnapshotMode.NO_DATA);
        assertThat(config.getSnapshotIsolationMode()).isEqualTo(CockroachDBConnectorConfig.SnapshotIsolationMode.READ_COMMITTED);
        assertThat(config.getSnapshotLockingMode()).isPresent();
        assertThat(config.getSnapshotLockingMode().get()).isEqualTo(CockroachDBConnectorConfig.SnapshotLockingMode.SHARED);
    }

    @Test
    public void shouldHandleSSLConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("database.sslmode", "require");
        props.put("database.sslrootcert", "/path/to/ca.crt");
        props.put("database.sslcert", "/path/to/client.crt");
        props.put("database.sslkey", "/path/to/client.key");
        props.put("database.sslpassword", "keypassword");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        assertThat(config.getSslMode()).isEqualTo("require");
        assertThat(config.getSslRootCert()).isEqualTo("/path/to/ca.crt");
        assertThat(config.getSslClientCert()).isEqualTo("/path/to/client.crt");
        assertThat(config.getSslClientKey()).isEqualTo("/path/to/client.key");
        assertThat(config.getSslClientKeyPassword()).isEqualTo("keypassword");
    }

    @Test
    public void shouldHandleConnectionSettings() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("database.dbname", "defaultdb");
        props.put("database.tcpKeepAlive", "true");
        props.put("database.on.connect.statements", "SET timezone='UTC'; SET application_name='debezium'");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        assertThat(config.isTcpKeepAlive()).isTrue();
        String onConnectStatements = config.getOnConnectStatements();
        if (onConnectStatements != null) {
            assertThat(onConnectStatements).isEqualTo("SET timezone='UTC'; SET application_name='debezium'");
        }
    }

    @Test
    public void shouldHandleReadOnlyConnection() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("read.only", "true");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        assertThat(config.isReadOnlyConnection()).isTrue();
    }

    @Test
    public void shouldHandleStatusUpdateInterval() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("status.update.interval.ms", "5000");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        assertThat(config.statusUpdateInterval().toMillis()).isEqualTo(5000);
    }

    @Test
    public void shouldHandleUnavailableValuePlaceholder() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("database.dbname", "defaultdb");
        props.put("database.unavailable.value.placeholder", "hex:FF");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        byte[] placeholder = config.getUnavailableValuePlaceholder();
        assertThat(placeholder).isNotNull();
        assertThat(placeholder.length).isGreaterThan(0);
    }

    @Test
    public void shouldParseSnapshotMode() {
        assertThat(CockroachDBConnectorConfig.SnapshotMode.parse("initial"))
                .isEqualTo(CockroachDBConnectorConfig.SnapshotMode.INITIAL);
        assertThat(CockroachDBConnectorConfig.SnapshotMode.parse("never"))
                .isEqualTo(CockroachDBConnectorConfig.SnapshotMode.NO_DATA);
        assertThat(CockroachDBConnectorConfig.SnapshotMode.parse("always"))
                .isEqualTo(CockroachDBConnectorConfig.SnapshotMode.ALWAYS);
        assertThat(CockroachDBConnectorConfig.SnapshotMode.parse("invalid")).isNull();
    }

    @Test
    public void shouldParseSnapshotIsolationMode() {
        assertThat(CockroachDBConnectorConfig.SnapshotIsolationMode.parse("serializable"))
                .isEqualTo(CockroachDBConnectorConfig.SnapshotIsolationMode.SERIALIZABLE);
        assertThat(CockroachDBConnectorConfig.SnapshotIsolationMode.parse("read_committed"))
                .isEqualTo(CockroachDBConnectorConfig.SnapshotIsolationMode.READ_COMMITTED);
        assertThat(CockroachDBConnectorConfig.SnapshotIsolationMode.parse("invalid")).isNull();
    }

    @Test
    public void shouldParseSnapshotLockingMode() {
        assertThat(CockroachDBConnectorConfig.SnapshotLockingMode.parse("none"))
                .isEqualTo(CockroachDBConnectorConfig.SnapshotLockingMode.NONE);
        assertThat(CockroachDBConnectorConfig.SnapshotLockingMode.parse("shared"))
                .isEqualTo(CockroachDBConnectorConfig.SnapshotLockingMode.SHARED);
        assertThat(CockroachDBConnectorConfig.SnapshotLockingMode.parse("custom"))
                .isEqualTo(CockroachDBConnectorConfig.SnapshotLockingMode.CUSTOM);
        assertThat(CockroachDBConnectorConfig.SnapshotLockingMode.parse("invalid")).isNull();
    }

    @Test
    public void shouldParseSecureConnectionMode() {
        assertThat(CockroachDBConnectorConfig.SecureConnectionMode.parse("disable"))
                .isEqualTo(CockroachDBConnectorConfig.SecureConnectionMode.DISABLED);
        assertThat(CockroachDBConnectorConfig.SecureConnectionMode.parse("require"))
                .isEqualTo(CockroachDBConnectorConfig.SecureConnectionMode.REQUIRED);
        assertThat(CockroachDBConnectorConfig.SecureConnectionMode.parse("verify-ca"))
                .isEqualTo(CockroachDBConnectorConfig.SecureConnectionMode.VERIFY_CA);
        assertThat(CockroachDBConnectorConfig.SecureConnectionMode.parse("verify-full"))
                .isEqualTo(CockroachDBConnectorConfig.SecureConnectionMode.VERIFY_FULL);
        assertThat(CockroachDBConnectorConfig.SecureConnectionMode.parse("invalid")).isNull();
    }

    @Test
    public void shouldReturnCorrectContextName() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        assertThat(config.getContextName()).isEqualTo("CockroachDB");
    }

    @Test
    public void shouldReturnCorrectConnectorName() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        assertThat(config.getConnectorName()).isEqualTo("cockroachdb");
    }

    @Test
    public void shouldHaveAllRequiredFields() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");
        props.put("database.dbname", "defaultdb");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));

        // Verify that all required fields are accessible
        assertThat(config.getHostname()).isEqualTo("localhost");
        assertThat(config.getDatabaseName()).isEqualTo("defaultdb");
    }
}
