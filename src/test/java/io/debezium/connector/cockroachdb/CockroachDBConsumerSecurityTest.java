/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;

/**
 * Unit tests for the changefeed consumer's security configuration.
 *
 * <p>The connector's intermediate-topic consumer is a separate Kafka client from the changefeed
 * push, so it needs its own security settings. These tests cover the two layers: auto-derived SSL
 * from {@code cockroachdb.changefeed.sink.tls.*}, and the
 * {@code cockroachdb.changefeed.kafka.consumer.override.*} passthrough.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConsumerSecurityTest {

    private static Configuration.Builder baseProps() {
        return Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "defaultdb")
                .with("database.server.name", "test-server")
                .with("topic.prefix", "test")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9093");
    }

    private static Properties apply(Configuration config) {
        Properties props = new Properties();
        CockroachDBStreamingChangeEventSource.applyConsumerSecurity(new CockroachDBConnectorConfig(config), props);
        return props;
    }

    @Test
    public void shouldAddNoSecurityByDefault() {
        Properties props = apply(baseProps().build());
        assertThat(props.getProperty("security.protocol")).isNull();
        assertThat(props).isEmpty();
    }

    @Test
    public void shouldDeriveMutualTlsFromSinkTlsFiles(@TempDir Path tmp) throws Exception {
        Path ca = tmp.resolve("ca.crt");
        Path cert = tmp.resolve("client.crt");
        Path key = tmp.resolve("client.key");
        Files.writeString(ca, "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n");
        Files.writeString(cert, "-----BEGIN CERTIFICATE-----\nCLIENT\n-----END CERTIFICATE-----\n");
        Files.writeString(key, "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----\n");

        Properties props = apply(baseProps()
                .with("cockroachdb.changefeed.sink.tls.ca.cert.file", ca.toString())
                .with("cockroachdb.changefeed.sink.tls.client.cert.file", cert.toString())
                .with("cockroachdb.changefeed.sink.tls.client.key.file", key.toString())
                .build());

        assertThat(props.getProperty("security.protocol")).isEqualTo("SSL");
        assertThat(props.getProperty("ssl.truststore.type")).isEqualTo("PEM");
        assertThat(props.getProperty("ssl.truststore.location")).isEqualTo(ca.toString());
        assertThat(props.getProperty("ssl.keystore.type")).isEqualTo("PEM");
        assertThat(props.getProperty("ssl.keystore.certificate.chain")).contains("CLIENT");
        assertThat(props.getProperty("ssl.keystore.key")).contains("KEY");
    }

    @Test
    public void shouldDeriveServerOnlyTlsFromCaOnly(@TempDir Path tmp) throws Exception {
        Path ca = tmp.resolve("ca.crt");
        Files.writeString(ca, "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n");

        Properties props = apply(baseProps()
                .with("cockroachdb.changefeed.sink.tls.ca.cert.file", ca.toString())
                .build());

        assertThat(props.getProperty("security.protocol")).isEqualTo("SSL");
        assertThat(props.getProperty("ssl.truststore.location")).isEqualTo(ca.toString());
        // No client cert/key -> no keystore.
        assertThat(props.getProperty("ssl.keystore.type")).isNull();
    }

    @Test
    public void shouldApplyConsumerOverridePassthrough() {
        Properties props = apply(baseProps()
                .with("cockroachdb.changefeed.kafka.consumer.override.security.protocol", "SASL_SSL")
                .with("cockroachdb.changefeed.kafka.consumer.override.sasl.mechanism", "SCRAM-SHA-512")
                .build());

        assertThat(props.getProperty("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(props.getProperty("sasl.mechanism")).isEqualTo("SCRAM-SHA-512");
    }

    @Test
    public void shouldLetOverrideWinOverDerivedTls(@TempDir Path tmp) throws Exception {
        Path ca = tmp.resolve("ca.crt");
        Files.writeString(ca, "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n");

        Properties props = apply(baseProps()
                .with("cockroachdb.changefeed.sink.tls.ca.cert.file", ca.toString())
                .with("cockroachdb.changefeed.kafka.consumer.override.security.protocol", "SASL_SSL")
                .build());

        // Auto-derive set SSL, but the explicit override wins.
        assertThat(props.getProperty("security.protocol")).isEqualTo("SASL_SSL");
    }

    @Test
    public void shouldNotTreatConsumerGroupPrefixAsPassthrough() {
        // The existing cockroachdb.changefeed.kafka.consumer.group.prefix field must not leak into
        // the consumer as a Kafka property via the override passthrough.
        Properties props = apply(baseProps()
                .with("cockroachdb.changefeed.kafka.consumer.group.prefix", "my-group")
                .build());

        assertThat(props.getProperty("group.prefix")).isNull();
        assertThat(props).isEmpty();
    }
}
