/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

/**
 * Unit tests for the {@code cockroachdb.changefeed.kafka.sink.config} property, which passes
 * CockroachDB's {@code kafka_sink_config} changefeed option through to the generated
 * {@code CREATE CHANGEFEED} statement. The value is a JSON document and must be emitted as a
 * single-quoted SQL literal; the general sink options passthrough cannot carry it because it
 * escapes the required quotes.
 *
 * @author Virag Tripathi
 */
public class CockroachDBKafkaSinkConfigTest {

    private static final String FLUSH_CONFIG = "{\"Flush\": {\"Messages\": 100, \"Frequency\": \"500ms\"}}";

    private CockroachDBStreamingChangeEventSource source(Configuration config) {
        return new CockroachDBStreamingChangeEventSource(new CockroachDBConnectorConfig(config), null, null, null, null);
    }

    private Configuration.Builder baseConfig() {
        return Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("database.server.name", "test")
                .with("topic.prefix", "crdb")
                .with("cockroachdb.changefeed.sink.type", "kafka")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
    }

    @Test
    void appendsKafkaSinkConfigAsQuotedLiteral() {
        Configuration config = baseConfig()
                .with("cockroachdb.changefeed.kafka.sink.config", FLUSH_CONFIG)
                .build();
        String query = source(config).buildSinkChangefeedQuery(
                List.of(new TableId("testdb", "public", "orders")), null, false);
        assertThat(query).contains("kafka_sink_config='" + FLUSH_CONFIG + "'");
    }

    @Test
    void omitsKafkaSinkConfigWhenUnset() {
        String query = source(baseConfig().build()).buildSinkChangefeedQuery(
                List.of(new TableId("testdb", "public", "orders")), null, false);
        assertThat(query).doesNotContain("kafka_sink_config");
    }

    @Test
    void escapesSingleQuotesInsideJsonValues() {
        Configuration config = baseConfig()
                .with("cockroachdb.changefeed.kafka.sink.config", "{\"Flush\": {\"Frequency\": \"1s\"}, \"note\": \"o'brien\"}")
                .build();
        String query = source(config).buildSinkChangefeedQuery(
                List.of(new TableId("testdb", "public", "orders")), null, false);
        assertThat(query).contains("o''brien");
        assertThat(query).doesNotContain("o'brien'}");
    }

    @Test
    void rejectsValuesThatAreNotJson() {
        Configuration config = baseConfig()
                .with("cockroachdb.changefeed.kafka.sink.config", "not json at all")
                .build();
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        assertThat(connectorConfig.validateAndRecord(
                List.of(CockroachDBConnectorConfig.CHANGEFEED_KAFKA_SINK_CONFIG), s -> {
                })).isFalse();
    }
}
