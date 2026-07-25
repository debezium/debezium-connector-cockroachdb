/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

/**
 * Unit tests for the statically testable pieces of {@link CockroachDBStreamingChangeEventSource}:
 * the deduplication event identifier, changefeed reuse detection, and the generated
 * {@code CREATE CHANGEFEED} statement.
 *
 * <p>The dedup identifier must be derived from the schema-qualified {@link TableId}, the
 * operation, the timestamp, and the changefeed message key; dropping any component silently
 * discards events for same-named tables across schemas or for different rows that share a
 * timestamp (debezium/dbz#2283).</p>
 *
 * <p>The {@code cockroachdb.changefeed.kafka.sink.config} property passes CockroachDB's
 * {@code kafka_sink_config} changefeed option through as a single-quoted JSON literal; the
 * general sink options passthrough cannot carry it because it escapes the required quotes
 * (debezium/dbz#2278).</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBStreamingChangeEventSourceTest {

    // ---------------------------------------------------------------------------------------
    // Dedup event identifier and changefeed reuse detection
    // ---------------------------------------------------------------------------------------

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode event(String op, String tsNanos, String sourceTableName) throws Exception {
        // Mimic a CockroachDB enriched event. source.table_name is intentionally the same for
        // both schemas to prove the dedup key does not rely on it.
        return MAPPER.readTree("{"
                + "\"op\": \"" + op + "\","
                + "\"ts_ns\": " + tsNanos + ","
                + "\"after\": {\"id\": 1},"
                + "\"source\": {\"table_name\": \"" + sourceTableName + "\"}"
                + "}");
    }

    @Test
    public void shouldDistinguishSameTableNameAcrossSchemas() throws Exception {
        // Same table name "orders", different schemas, identical op and MVCC timestamp.
        TableId publicOrders = new TableId("demodb", "public", "orders");
        TableId inventoryOrders = new TableId("demodb", "inventory", "orders");

        JsonNode publicEvent = event("c", "1749572623476416439", "orders");
        JsonNode inventoryEvent = event("c", "1749572623476416439", "orders");

        String publicId = CockroachDBStreamingChangeEventSource.createEventId(publicOrders, publicEvent, "[1]");
        String inventoryId = CockroachDBStreamingChangeEventSource.createEventId(inventoryOrders, inventoryEvent, "[1]");

        assertThat(publicId).isNotNull();
        assertThat(inventoryId).isNotNull();
        assertThat(publicId).isNotEqualTo(inventoryId);
    }

    @Test
    public void shouldDistinguishDifferentRowsSharingOpAndTimestamp() throws Exception {
        // Two rows changed in the same transaction share op and ts_ns. Reproduced live with the
        // bank workload (debezium/dbz#2283): a transfer updates two accounts in one transaction,
        // and the second event was dropped as a duplicate, losing the row change.
        TableId bank = new TableId("bank", "public", "bank");
        JsonNode firstRow = event("u", "1784905282711794397", "bank");
        JsonNode secondRow = event("u", "1784905282711794397", "bank");

        String firstId = CockroachDBStreamingChangeEventSource.createEventId(bank, firstRow, "[871]");
        String secondId = CockroachDBStreamingChangeEventSource.createEventId(bank, secondRow, "[883]");

        assertThat(firstId).isNotNull();
        assertThat(secondId).isNotNull();
        assertThat(firstId).isNotEqualTo(secondId);
    }

    @Test
    public void shouldProduceStableIdForSameTableOpTimestampAndKey() throws Exception {
        TableId orders = new TableId("demodb", "public", "orders");
        JsonNode first = event("u", "100", "orders");
        JsonNode second = event("u", "100", "orders");

        assertThat(CockroachDBStreamingChangeEventSource.createEventId(orders, first, "[1]"))
                .isEqualTo(CockroachDBStreamingChangeEventSource.createEventId(orders, second, "[1]"));
    }

    @Test
    public void shouldDistinguishByOperationAndTimestamp() throws Exception {
        TableId orders = new TableId("demodb", "public", "orders");

        String create = CockroachDBStreamingChangeEventSource.createEventId(orders, event("c", "100", "orders"), "[1]");
        String update = CockroachDBStreamingChangeEventSource.createEventId(orders, event("u", "100", "orders"), "[1]");
        String laterUpdate = CockroachDBStreamingChangeEventSource.createEventId(orders, event("u", "200", "orders"), "[1]");

        assertThat(create).isNotEqualTo(update);
        assertThat(update).isNotEqualTo(laterUpdate);
    }

    @Test
    public void shouldNotDependOnSourceBlockBeingPresent() throws Exception {
        // No source block at all: the key still resolves from the TableId, op, ts_ns and row key.
        TableId orders = new TableId("demodb", "public", "orders");
        JsonNode noSource = MAPPER.readTree("{\"op\": \"c\", \"ts_ns\": 100, \"after\": {\"id\": 1}}");

        String id = CockroachDBStreamingChangeEventSource.createEventId(orders, noSource, "[1]");
        assertThat(id).isNotNull();
        assertThat(id).contains(orders.identifier());
    }

    @Test
    public void shouldTolerateMissingMessageKey() throws Exception {
        // A missing key must not fail id creation; dedup degrades to table, op and ts_ns.
        TableId orders = new TableId("demodb", "public", "orders");
        JsonNode event = event("c", "100", "orders");

        assertThat(CockroachDBStreamingChangeEventSource.createEventId(orders, event, null))
                .isEqualTo(CockroachDBStreamingChangeEventSource.createEventId(orders, event, null));
        assertThat(CockroachDBStreamingChangeEventSource.createEventId(orders, event, null)).isNotNull();
    }

    @Test
    public void shouldResolveNestedPayloadEnvelope() throws Exception {
        // Some envelopes wrap the change under a "payload" node; the key must read through it.
        TableId orders = new TableId("demodb", "public", "orders");
        JsonNode nested = MAPPER.readTree("{\"payload\": {\"op\": \"c\", \"ts_ns\": 100, \"after\": {\"id\": 1}}}");
        JsonNode flat = MAPPER.readTree("{\"op\": \"c\", \"ts_ns\": 100, \"after\": {\"id\": 1}}");

        assertThat(CockroachDBStreamingChangeEventSource.createEventId(orders, nested, "[1]"))
                .isEqualTo(CockroachDBStreamingChangeEventSource.createEventId(orders, flat, "[1]"));
    }

    @Test
    public void shouldDetectEnrichedEnvelopeInChangefeedDescription() {
        // The connector reuses an existing changefeed only if it was created with envelope='enriched'.
        assertThat(CockroachDBStreamingChangeEventSource.changefeedUsesEnrichedEnvelope(
                "CREATE CHANGEFEED ... WITH OPTIONS (envelope = 'enriched', full_table_name)")).isTrue();
        assertThat(CockroachDBStreamingChangeEventSource.changefeedUsesEnrichedEnvelope(
                "CREATE CHANGEFEED ... WITH OPTIONS (envelope='enriched')")).isTrue();
    }

    @Test
    public void shouldRejectNonEnrichedEnvelopeInChangefeedDescription() {
        assertThat(CockroachDBStreamingChangeEventSource.changefeedUsesEnrichedEnvelope(
                "CREATE CHANGEFEED ... WITH OPTIONS (envelope = 'wrapped')")).isFalse();
        assertThat(CockroachDBStreamingChangeEventSource.changefeedUsesEnrichedEnvelope(
                "CREATE CHANGEFEED ... WITH OPTIONS (full_table_name)")).isFalse();
        assertThat(CockroachDBStreamingChangeEventSource.changefeedUsesEnrichedEnvelope(null)).isFalse();
    }

    // ---------------------------------------------------------------------------------------
    // kafka_sink_config emission in the generated CREATE CHANGEFEED statement
    // ---------------------------------------------------------------------------------------

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
