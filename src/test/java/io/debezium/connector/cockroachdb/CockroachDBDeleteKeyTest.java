/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.serialization.ChangefeedJsonMapper;
import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Regression test for delete events without a before image.
 *
 * <p>Changefeeds created without the {@code diff} option send deletes with {@code after: null}
 * and no {@code before}, so the old column values used to build the record key must fall back
 * to the changefeed message key, which always carries the primary key columns. Without the
 * fallback, deletes emit a null key against the required key schema and key conversion fails
 * with "Conversion error: null value for field that is required and has no default value".</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBDeleteKeyTest {

    @Test
    void deleteWithoutBeforeImageDerivesOldValuesFromMessageKey() throws Exception {
        Table table = table();
        JsonNode keyNode = ChangefeedJsonMapper.create()
                .readTree("{\"id\": \"eb646131-90f5-4788-a8f6-8698fb6431fd\"}");

        CockroachDBChangeRecordEmitter emitter = emitter(table, keyNode);

        Object[] oldValues = emitter.getOldColumnValues();
        assertThat(oldValues).isNotNull();
        assertThat(oldValues[0]).isEqualTo("eb646131-90f5-4788-a8f6-8698fb6431fd");
        assertThat(oldValues[1]).isNull();
    }

    @Test
    void deleteWithoutBeforeImageDerivesOldValuesFromArrayMessageKey() throws Exception {
        // Sinkless changefeeds deliver the key as a JSON array in primary key column order.
        JsonNode keyNode = ChangefeedJsonMapper.create()
                .readTree("[\"a0604d58-e0e8-419a-a2d7-1cb8c94ac4ee\"]");
        CockroachDBChangeRecordEmitter emitter = emitter(table(), keyNode);

        Object[] oldValues = emitter.getOldColumnValues();
        assertThat(oldValues).isNotNull();
        assertThat(oldValues[0]).isEqualTo("a0604d58-e0e8-419a-a2d7-1cb8c94ac4ee");
        assertThat(oldValues[1]).isNull();
    }

    @Test
    void deleteWithoutBeforeImageAndWithoutMessageKeyStaysNull() {
        CockroachDBChangeRecordEmitter emitter = emitter(table(), null);
        assertThat(emitter.getOldColumnValues()).isNull();
    }

    private static Table table() {
        return Table.editor()
                .tableId(new TableId("demodb", "public", "orders"))
                .addColumn(Column.editor().name("id").type("UUID").jdbcType(Types.OTHER).optional(false).create())
                .addColumn(Column.editor().name("name").type("STRING").jdbcType(Types.VARCHAR).optional(true).create())
                .setPrimaryKeyNames("id")
                .create();
    }

    private static CockroachDBChangeRecordEmitter emitter(Table table, JsonNode keyNode) {
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.password", "")
                .with("database.dbname", "demodb")
                .with("topic.prefix", "test")
                .build();
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        CockroachDBOffsetContext offsetContext = new CockroachDBOffsetContext(connectorConfig);
        return new CockroachDBChangeRecordEmitter(
                new CockroachDBPartition("test"), offsetContext, Clock.system(),
                connectorConfig, table, Envelope.Operation.DELETE, null, null, keyNode);
    }
}
