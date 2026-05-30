/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.relational.TableId;

/**
 * Unit tests for the deduplication event identifier.
 *
 * <p>The dedup key must be derived from the schema-qualified {@link TableId} (the topic the
 * event arrived on), not from the unqualified {@code source.table_name} field in the message.
 * Otherwise two same-named tables in different schemas can collide and have one event dropped.
 *
 * @author Virag Tripathi
 */
public class CockroachDBEventDeduplicationTest {

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

        String publicId = CockroachDBStreamingChangeEventSource.createEventId(publicOrders, publicEvent);
        String inventoryId = CockroachDBStreamingChangeEventSource.createEventId(inventoryOrders, inventoryEvent);

        assertThat(publicId).isNotNull();
        assertThat(inventoryId).isNotNull();
        assertThat(publicId).isNotEqualTo(inventoryId);
    }

    @Test
    public void shouldProduceStableIdForSameTableOpAndTimestamp() throws Exception {
        TableId orders = new TableId("demodb", "public", "orders");
        JsonNode first = event("u", "100", "orders");
        JsonNode second = event("u", "100", "orders");

        assertThat(CockroachDBStreamingChangeEventSource.createEventId(orders, first))
                .isEqualTo(CockroachDBStreamingChangeEventSource.createEventId(orders, second));
    }

    @Test
    public void shouldDistinguishByOperationAndTimestamp() throws Exception {
        TableId orders = new TableId("demodb", "public", "orders");

        String create = CockroachDBStreamingChangeEventSource.createEventId(orders, event("c", "100", "orders"));
        String update = CockroachDBStreamingChangeEventSource.createEventId(orders, event("u", "100", "orders"));
        String laterUpdate = CockroachDBStreamingChangeEventSource.createEventId(orders, event("u", "200", "orders"));

        assertThat(create).isNotEqualTo(update);
        assertThat(update).isNotEqualTo(laterUpdate);
    }

    @Test
    public void shouldNotDependOnSourceBlockBeingPresent() throws Exception {
        // No source block at all: the key still resolves from the TableId, op and ts_ns.
        TableId orders = new TableId("demodb", "public", "orders");
        JsonNode noSource = MAPPER.readTree("{\"op\": \"c\", \"ts_ns\": 100, \"after\": {\"id\": 1}}");

        String id = CockroachDBStreamingChangeEventSource.createEventId(orders, noSource);
        assertThat(id).isNotNull();
        assertThat(id).contains(orders.identifier());
    }

    @Test
    public void shouldResolveNestedPayloadEnvelope() throws Exception {
        // Some envelopes wrap the change under a "payload" node; the key must read through it.
        TableId orders = new TableId("demodb", "public", "orders");
        JsonNode nested = MAPPER.readTree("{\"payload\": {\"op\": \"c\", \"ts_ns\": 100, \"after\": {\"id\": 1}}}");
        JsonNode flat = MAPPER.readTree("{\"op\": \"c\", \"ts_ns\": 100, \"after\": {\"id\": 1}}");

        assertThat(CockroachDBStreamingChangeEventSource.createEventId(orders, nested))
                .isEqualTo(CockroachDBStreamingChangeEventSource.createEventId(orders, flat));
    }
}
