/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.serialization.ChangefeedJsonMapper;
import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link CockroachDBChangeRecordEmitter#extractColumnValues(JsonNode, List)}, the
 * mapping from an enriched changefeed row to Java values aligned with the column schema types.
 * Temporal value/normalization rules are covered by {@link CockroachDBTemporalConversionsTest}.
 *
 * <p>Also carries the emitter-level regression tests: DECIMAL values must retain the exact
 * source digits instead of being double-rounded (debezium/dbz#2256), and deletes without a
 * before image must derive the record key from the changefeed message key so key conversion
 * does not fail on the required key schema (debezium/dbz#2267).</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeRecordEmitterTest {

    @Test
    public void shouldExtractTemporalColumnValuesWithCorrectJavaTypes() throws Exception {
        // Real CockroachDB enriched changefeed output for each temporal type (captured from v25.4.13).
        String json = "{\"d\":\"2026-06-08\",\"id\":1,\"tm\":\"11:01:45.883\",\"tmtz\":\"11:01:45.883+02\","
                + "\"ts\":\"2026-06-08T11:01:45.883\",\"tstz\":\"2026-06-08T09:01:45.883Z\"}";
        JsonNode node = new ObjectMapper().readTree(json);
        List<Column> columns = List.of(
                Column.editor().name("ts").type("TIMESTAMP").create(),
                Column.editor().name("tstz").type("TIMESTAMPTZ").create(),
                Column.editor().name("tm").type("TIME").create(),
                Column.editor().name("tmtz").type("TIMETZ").create(),
                Column.editor().name("d").type("DATE").create());

        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(node, columns);

        // TIMESTAMP (without tz) -> MicroTimestamp (Long micros); must not be null (the original bug).
        assertThat(values[0]).isInstanceOf(Long.class);
        // TIMESTAMPTZ -> ZonedTimestamp (String); a Z value already satisfies ISO_OFFSET_DATE_TIME.
        assertThat(values[1]).isEqualTo("2026-06-08T09:01:45.883Z");
        // TIME -> MicroTime (Long micros since midnight).
        assertThat(values[2]).isEqualTo(39_705_883_000L);
        // TIMETZ -> ZonedTime (String); the CockroachDB hour-only "+02" offset is normalized to
        // "+02:00" so it parses with the ZonedTime (ISO_OFFSET_TIME) formatter downstream.
        assertThat(values[3]).isEqualTo("11:01:45.883+02:00");
        // DATE -> Date (Integer days since epoch).
        assertThat(values[4]).isInstanceOf(Integer.class);
    }

    @Test
    public void shouldReturnNullForNullJsonColumnValues() throws Exception {
        String json = "{\"id\":1,\"tstz\":null,\"tmtz\":null}";
        JsonNode node = new ObjectMapper().readTree(json);
        List<Column> columns = List.of(
                Column.editor().name("tstz").type("TIMESTAMPTZ").create(),
                Column.editor().name("tmtz").type("TIMETZ").create());

        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(node, columns);

        assertThat(values[0]).isNull();
        assertThat(values[1]).isNull();
    }

    // ---------------------------------------------------------------------------------------
    // DECIMAL precision regression (debezium/dbz#2256): changefeed JSON carries DECIMAL values
    // as JSON numbers with full precision; parsing them into a Java double silently rounds
    // anything above roughly 15 to 17 significant digits. The payloads below are exactly what
    // a CockroachDB v25.4 changefeed emits for DECIMAL(28,18), DECIMAL(16,6), and DECIMAL(14,4).
    // ---------------------------------------------------------------------------------------

    private static final String CHANGEFEED_AFTER = "{"
            + "\"id\": 1, "
            + "\"trade_dt_qty\": 9999999999.999999999000000000, "
            + "\"cost_basis\": 9999999999.999999, "
            + "\"seg_memo_qty\": 9999.9999"
            + "}";

    @Test
    public void decimalValuesRetainSourcePrecision() throws Exception {
        JsonNode after = ChangefeedJsonMapper.create().readTree(CHANGEFEED_AFTER);
        List<Column> columns = List.of(
                column("id", "INT8", Types.BIGINT),
                column("trade_dt_qty", "DECIMAL", Types.NUMERIC),
                column("cost_basis", "DECIMAL", Types.NUMERIC),
                column("seg_memo_qty", "DECIMAL", Types.NUMERIC));

        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(after, columns);

        assertThat(values[0]).isEqualTo(1L);
        assertThat(values[1]).isEqualTo("9999999999.999999999000000000");
        assertThat(values[2]).isEqualTo("9999999999.999999");
        assertThat(values[3]).isEqualTo("9999.9999");
    }

    @Test
    public void decAliasRetainsSourcePrecision() throws Exception {
        JsonNode node = ChangefeedJsonMapper.create().readTree("{\"v\": 9999999999.999999999}");
        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(
                node, List.of(column("v", "DEC", Types.NUMERIC)));
        assertThat(values[0]).isEqualTo("9999999999.999999999");
    }

    @Test
    public void decimalValuesAvoidScientificNotation() throws Exception {
        JsonNode node = ChangefeedJsonMapper.create().readTree(
                "{\"tiny\": 0.000000000000000001, \"large\": 12345678901234567890.123456789}");
        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(
                node, List.of(
                        column("tiny", "DECIMAL", Types.NUMERIC),
                        column("large", "NUMERIC", Types.NUMERIC)));
        assertThat(values[0]).isEqualTo("0.000000000000000001");
        assertThat(values[1]).isEqualTo("12345678901234567890.123456789");
    }

    // ---------------------------------------------------------------------------------------
    // Delete key regression (debezium/dbz#2267): changefeeds created without the diff option
    // send deletes with after: null and no before, so the old column values used to build the
    // record key must fall back to the changefeed message key.
    // ---------------------------------------------------------------------------------------

    @Test
    public void deleteWithoutBeforeImageDerivesOldValuesFromMessageKey() throws Exception {
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
    public void deleteWithoutBeforeImageDerivesOldValuesFromArrayMessageKey() throws Exception {
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
    public void deleteWithoutBeforeImageAndWithoutMessageKeyStaysNull() {
        CockroachDBChangeRecordEmitter emitter = emitter(table(), null);
        assertThat(emitter.getOldColumnValues()).isNull();
    }

    private static Column column(String name, String typeName, int jdbcType) {
        return Column.editor()
                .name(name)
                .type(typeName)
                .jdbcType(jdbcType)
                .optional(true)
                .create();
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

    // ---------------------------------------------------------------------------------------
    // BYTES regression (debezium/dbz#2310): enriched changefeeds encode BYTES as the bytea
    // hex literal ("\\x01ff", captured from v25.4.13). The extracted value must be the
    // decoded bytes, or Connect rejects the String against the BYTES schema and the field
    // is emitted as null.
    // ---------------------------------------------------------------------------------------

    @Test
    public void bytesValuesDecodeFromChangefeedHexLiteral() throws Exception {
        JsonNode node = new ObjectMapper().readTree(
                "{\"taxlot_id\": \"\\\\xf925e84ec3444ce383785556b60dd048\", \"parent_id\": \"\\\\x01ff\", \"empty\": \"\\\\x\"}");
        List<Column> columns = List.of(
                column("taxlot_id", "BYTES", Types.BINARY),
                column("parent_id", "BYTEA", Types.BINARY),
                column("empty", "BYTES", Types.BINARY));

        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(node, columns);

        assertThat(values[0]).isInstanceOf(byte[].class);
        assertThat((byte[]) values[0]).containsExactly(
                0xf9, 0x25, 0xe8, 0x4e, 0xc3, 0x44, 0x4c, 0xe3, 0x83, 0x78, 0x55, 0x56, 0xb6, 0x0d, 0xd0, 0x48);
        assertThat((byte[]) values[1]).containsExactly(0x01, 0xff);
        assertThat((byte[]) values[2]).isEmpty();
    }

    @Test
    public void deleteKeyWithBytesPrimaryKeyDecodesToBytes() throws Exception {
        Table table = Table.editor()
                .tableId(new TableId("demodb", "public", "tax_cost_basis"))
                .addColumn(Column.editor().name("taxlot_id").type("BYTES").jdbcType(Types.BINARY).optional(false).create())
                .addColumn(Column.editor().name("note").type("STRING").jdbcType(Types.VARCHAR).optional(true).create())
                .setPrimaryKeyNames("taxlot_id")
                .create();
        JsonNode keyNode = ChangefeedJsonMapper.create()
                .readTree("{\"taxlot_id\": \"\\\\x01ff\"}");

        CockroachDBChangeRecordEmitter emitter = emitter(table, keyNode);

        Object[] oldValues = emitter.getOldColumnValues();
        assertThat(oldValues).isNotNull();
        assertThat(oldValues[0]).isInstanceOf(byte[].class);
        assertThat((byte[]) oldValues[0]).containsExactly(0x01, 0xff);
    }
}
