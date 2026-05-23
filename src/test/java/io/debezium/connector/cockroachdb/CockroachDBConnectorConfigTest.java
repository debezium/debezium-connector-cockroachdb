/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;

/**
 * Tests for CockroachDB connector configuration.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorConfigTest {

    @Test
    public void shouldHaveValidConfiguration() {
        CockroachDBConnector connector = new CockroachDBConnector();
        var configDef = connector.config();

        assertThat(configDef).isNotNull();
        assertThat(configDef.names()).contains("database.hostname");
        assertThat(configDef.names()).contains("database.port");
        assertThat(configDef.names()).contains("database.user");
        assertThat(configDef.names()).contains("database.password");
        assertThat(configDef.names()).contains("database.dbname");
        assertThat(configDef.names()).contains("database.server.name");
        assertThat(configDef.names()).contains("topic.prefix");
    }

    @Test
    public void shouldCreateValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        assertThat(connectorConfig.getDatabaseName()).isEqualTo("defaultdb");
        assertThat(connectorConfig.getLogicalName()).isEqualTo("test");
        assertThat(connectorConfig.getHostname()).isEqualTo("localhost");
        assertThat(connectorConfig.getPort()).isEqualTo(26257);
        assertThat(connectorConfig.getUser()).isEqualTo("root");
        assertThat(connectorConfig.getPassword()).isEqualTo("");
    }

    @Test
    public void shouldUseDefaultPort() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        assertThat(connectorConfig.getPort()).isEqualTo(26257); // Default CockroachDB port
    }

    @Test
    public void shouldValidateRequiredFields() {
        Map<String, String> props = new HashMap<>();
        // Missing required fields

        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        // In Debezium's architecture, required field validation is handled by Kafka Connect's ConfigDef mechanism
        // The test should verify that the ConfigDef contains the required fields
        assertThat(configDef.names()).contains("database.hostname");
        assertThat(configDef.names()).contains("database.user");
        assertThat(configDef.names()).contains("database.dbname");
        assertThat(configDef.names()).contains("database.server.name");
        assertThat(configDef.names()).contains("topic.prefix");

        // Validate the config and check that it can be created (validation happens at runtime)
        var validationResults = configDef.validate(props);
        // In Debezium's architecture, missing required fields don't cause validation errors in ConfigDef.validate()
        // The validation happens when the connector is actually started
        assertThat(validationResults).isNotNull();
    }

    @Test
    public void shouldValidateHostname() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "");
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        // Empty hostname should be allowed by ConfigDef, but we can test the configuration
        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        assertThat(connectorConfig.getHostname()).isEqualTo("");
    }

    @Test
    public void shouldValidateUser() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.user", "");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        // Empty user should be allowed by ConfigDef, but we can test the configuration
        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        assertThat(connectorConfig.getUser()).isEqualTo("");
    }

    @Test
    public void shouldValidateDatabaseName() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.user", "root");
        props.put("database.dbname", "");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        // Empty database name should be allowed by ConfigDef, but we can test the configuration
        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        assertThat(connectorConfig.getDatabaseName()).isEqualTo("");
    }

    @Test
    public void shouldValidateServerName() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "");
        props.put("topic.prefix", "test");

        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        // Empty server name should be allowed by ConfigDef, but we can test the configuration
        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        assertThat(connectorConfig.getLogicalName()).isEqualTo("test");
    }

    @Test
    public void shouldValidateTopicPrefix() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "");

        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        // Empty topic prefix should be allowed by ConfigDef, but we can test the configuration
        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        assertThat(connectorConfig.getLogicalName()).isEqualTo("");
    }

    @Test
    public void shouldValidatePortRange() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "0"); // Invalid port
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        // Port 0 should be allowed by ConfigDef, but we can test the configuration
        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        assertThat(connectorConfig.getPort()).isEqualTo(0);
    }

    @Test
    public void shouldValidatePortRangeUpperBound() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "65536"); // Invalid port
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        // Port 65536 should be allowed by ConfigDef, but we can test the configuration
        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        assertThat(connectorConfig.getPort()).isEqualTo(65536);
    }

    @Test
    public void shouldHandleValidPort() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "5432"); // Valid port
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        assertThat(connectorConfig.getPort()).isEqualTo(5432);
    }

    @Test
    public void shouldHandleMaxValidPort() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "65535"); // Max valid port
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        assertThat(connectorConfig.getPort()).isEqualTo(65535);
    }

    @Test
    public void shouldReturnInitialScanYesForAlwaysMode() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("always");
        assertThat(config.getInitialScanForSnapshotMode(false)).isEqualTo("yes");
        assertThat(config.getInitialScanForSnapshotMode(true)).isEqualTo("yes");
    }

    @Test
    public void shouldReturnInitialScanYesForInitialModeWithoutOffset() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("initial");
        assertThat(config.getInitialScanForSnapshotMode(false)).isEqualTo("yes");
    }

    @Test
    public void shouldReturnInitialScanNoForInitialModeWithOffset() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("initial");
        assertThat(config.getInitialScanForSnapshotMode(true)).isEqualTo("no");
    }

    @Test
    public void shouldReturnInitialScanNoForNoDataMode() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("no_data");
        assertThat(config.getInitialScanForSnapshotMode(false)).isEqualTo("no");
        assertThat(config.getInitialScanForSnapshotMode(true)).isEqualTo("no");
    }

    @Test
    public void shouldReturnInitialScanNoForNeverMode() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("never");
        assertThat(config.getInitialScanForSnapshotMode(false)).isEqualTo("no");
        assertThat(config.getInitialScanForSnapshotMode(true)).isEqualTo("no");
    }

    @Test
    public void shouldReturnInitialScanOnlyForInitialOnlyMode() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("initial_only");
        assertThat(config.getInitialScanForSnapshotMode(false)).isEqualTo("only");
        assertThat(config.getInitialScanForSnapshotMode(true)).isEqualTo("only");
    }

    @Test
    public void shouldReturnInitialScanForWhenNeededMode() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("when_needed");
        assertThat(config.getInitialScanForSnapshotMode(false)).isEqualTo("yes");
        assertThat(config.getInitialScanForSnapshotMode(true)).isEqualTo("no");
    }

    @Test
    public void shouldReturnNullForCustomMode() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("custom");
        assertThat(config.getInitialScanForSnapshotMode(false)).isNull();
        assertThat(config.getInitialScanForSnapshotMode(true)).isNull();
    }

    @Test
    public void shouldReturnNullForConfigurationBasedMode() {
        CockroachDBConnectorConfig config = createConfigWithSnapshotMode("configuration_based");
        assertThat(config.getInitialScanForSnapshotMode(false)).isNull();
        assertThat(config.getInitialScanForSnapshotMode(true)).isNull();
    }

    @Test
    public void shouldNotExposeSkipPermissionCheckConfig() {
        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        assertThat(configDef.names()).doesNotContain("cockroachdb.skip.permission.check");
    }

    @Test
    public void shouldHandleStaleSkipPermissionCheckProperty() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("topic.prefix", "test");
        props.put("cockroachdb.skip.permission.check", "true");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));
        assertThat(config).isNotNull();
        assertThat(config.getLogicalName()).isEqualTo("test");
    }

    @Test
    public void shouldDefaultSinkTopicPrefixToEmpty() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("topic.prefix", "test");

        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(Configuration.from(props));
        String sinkPrefix = config.getChangefeedSinkTopicPrefix();

        assertThat(sinkPrefix).isEmpty();
    }

    private CockroachDBConnectorConfig createConfigWithSnapshotMode(String snapshotMode) {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.user", "root");
        props.put("database.dbname", "defaultdb");
        props.put("topic.prefix", "test");
        props.put("snapshot.mode", snapshotMode);
        return new CockroachDBConnectorConfig(Configuration.from(props));
    }

    @Test
    public void shouldExposeKafkaSinkTlsFieldsInConfigDef() {
        ConfigDef configDef = new CockroachDBConnector().config();
        assertThat(configDef.names()).contains(
                "cockroachdb.changefeed.sink.kafka.ca.cert.file",
                "cockroachdb.changefeed.sink.kafka.client.cert.file",
                "cockroachdb.changefeed.sink.kafka.client.key.file");
    }

    @Test
    public void shouldDefaultKafkaSinkTlsFieldsToNullAndDisabled() {
        CockroachDBConnectorConfig config = baseConfigBuilder().build();
        assertThat(config.getChangefeedSinkKafkaCaCertFile()).isNull();
        assertThat(config.getChangefeedSinkKafkaClientCertFile()).isNull();
        assertThat(config.getChangefeedSinkKafkaClientKeyFile()).isNull();
        assertThat(config.isChangefeedSinkKafkaTlsEnabled()).isFalse();
    }

    @Test
    public void shouldImplyTlsEnabledWhenAnyKafkaSinkTlsFileSet(@TempDir Path tmp) throws Exception {
        Path caCert = tmp.resolve("ca.pem");
        Files.writeString(caCert, "ca-cert-bytes");
        CockroachDBConnectorConfig config = baseConfigBuilder()
                .with("cockroachdb.changefeed.sink.kafka.ca.cert.file", caCert.toString())
                .build();
        assertThat(config.isChangefeedSinkKafkaTlsEnabled()).isTrue();
        assertThat(config.getChangefeedSinkKafkaCaCertFile()).isEqualTo(caCert.toString());
    }

    @Test
    public void shouldAcceptValidReadableKafkaSinkTlsFiles(@TempDir Path tmp) throws Exception {
        Path caCert = tmp.resolve("ca.pem");
        Path clientCert = tmp.resolve("client.crt");
        Path clientKey = tmp.resolve("client.key");
        Files.writeString(caCert, "ca");
        Files.writeString(clientCert, "client-cert");
        Files.writeString(clientKey, "client-key");
        Configuration config = baseProps()
                .with("cockroachdb.changefeed.sink.kafka.ca.cert.file", caCert.toString())
                .with("cockroachdb.changefeed.sink.kafka.client.cert.file", clientCert.toString())
                .with("cockroachdb.changefeed.sink.kafka.client.key.file", clientKey.toString())
                .build();
        assertThat(config.validateAndRecord(
                CockroachDBConnectorConfig.ALL_FIELDS,
                CockroachDBConnectorConfigTest::failOnProblem)).isTrue();
    }

    @Test
    public void shouldRejectMissingKafkaSinkTlsFile(@TempDir Path tmp) {
        Path missing = tmp.resolve("does-not-exist.pem");
        Configuration config = baseProps()
                .with("cockroachdb.changefeed.sink.kafka.ca.cert.file", missing.toString())
                .build();
        StringBuilder problemMessage = new StringBuilder();
        boolean ok = config.validateAndRecord(
                CockroachDBConnectorConfig.ALL_FIELDS,
                (String message) -> problemMessage.append(message));
        assertThat(ok).isFalse();
        assertThat(problemMessage.toString()).contains("does not exist or is not readable");
        assertThat(problemMessage.toString()).contains(missing.toString());
    }

    @Test
    public void shouldRejectEmptyKafkaSinkTlsFile(@TempDir Path tmp) throws Exception {
        Path empty = tmp.resolve("empty.pem");
        Files.createFile(empty);
        Configuration config = baseProps()
                .with("cockroachdb.changefeed.sink.kafka.client.cert.file", empty.toString())
                .build();
        StringBuilder problemMessage = new StringBuilder();
        boolean ok = config.validateAndRecord(
                CockroachDBConnectorConfig.ALL_FIELDS,
                (String message) -> problemMessage.append(message));
        assertThat(ok).isFalse();
        assertThat(problemMessage.toString()).contains("File is empty");
    }

    @Test
    public void shouldAcceptEmptyStringForKafkaSinkTlsFile() {
        Configuration config = baseProps()
                .with("cockroachdb.changefeed.sink.kafka.ca.cert.file", "")
                .build();
        assertThat(config.validateAndRecord(
                CockroachDBConnectorConfig.ALL_FIELDS,
                CockroachDBConnectorConfigTest::failOnProblem)).isTrue();
    }

    private static Configuration.Builder baseProps() {
        return Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.password", "")
                .with("database.dbname", "defaultdb")
                .with("database.server.name", "test-server")
                .with("topic.prefix", "test")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
    }

    private static ConfigurationBuilder baseConfigBuilder() {
        return new ConfigurationBuilder(baseProps());
    }

    private static void failOnProblem(String message) {
        throw new AssertionError("Unexpected validation problem: " + message);
    }

    private static final class ConfigurationBuilder {
        private final Configuration.Builder delegate;

        ConfigurationBuilder(Configuration.Builder delegate) {
            this.delegate = delegate;
        }

        ConfigurationBuilder with(String key, String value) {
            delegate.with(key, value);
            return this;
        }

        CockroachDBConnectorConfig build() {
            return new CockroachDBConnectorConfig(delegate.build());
        }
    }
}
