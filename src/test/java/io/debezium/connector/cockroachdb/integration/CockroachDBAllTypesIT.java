/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of every CockroachDB column type the connector maps, pinned against the
 * actual enriched changefeed encodings (captured from a real cluster: bytes arrive as bytea
 * hex literals, JSONB and spatial types as JSON objects, arrays as JSON arrays, VECTOR as a
 * bracketed string). A conversion regression on any type surfaces here as a null field or a
 * failed assertion instead of reaching production (debezium/dbz#2310).
 *
 * @author Virag Tripathi
 */
public class CockroachDBAllTypesIT extends AbstractCockroachDBPipelineIT {

    private Connection connection;

    @AfterEach
    public void closeConnection() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        connection = null;
    }

    @Test
    public void shouldEmitEveryColumnTypeWithoutNulls() throws Exception {
        connection = openDatabase("alltypes_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS alltypes ("
                    + "id INT8 PRIMARY KEY, "
                    + "c_bytes BYTES NOT NULL, c_bit BIT(4), c_varbit VARBIT, c_interval INTERVAL, c_inet INET, "
                    + "c_jsonb JSONB, c_uuid UUID, c_dec DECIMAL(10,3), c_float FLOAT8, "
                    + "c_int_arr INT8[], c_str_arr STRING[], c_dec_arr DECIMAL[], "
                    + "c_vector VECTOR(3), c_geog GEOGRAPHY, c_geom GEOMETRY, "
                    + "c_bool BOOL, c_int2 INT2, c_ts TIMESTAMP, c_tstz TIMESTAMPTZ, c_date DATE, c_str STRING)");
            stmt.execute("UPSERT INTO alltypes VALUES ("
                    + "1, x'01ff', B'1010', B'011', INTERVAL '1 day 02:03:04.5', '192.168.1.10/24', "
                    + "'{\"a\": [1,2]}', 'f47ac10b-58cc-4372-a567-0e02b2c3d479', 12.345, 1.5, "
                    + "ARRAY[1,2,3], ARRAY['x','y'], ARRAY[1.25,2.5], "
                    + "'[1.5,2.5,3.5]', 'POINT(-74 40.7)', 'LINESTRING(0 0, 1 1)', "
                    + "true, 7, '2026-06-08 11:01:45.883', '2026-06-08 09:01:45.883+00', '2026-06-08', 'hello')");
        }

        startTask(baseConnectorConfig("alltypes-test", "alltypes_testdb", "public.alltypes"));

        List<SourceRecord> records = pollForRecords(1, 45);
        assertThat(records).as("Should receive the seed row").isNotEmpty();

        Struct after = ((Struct) records.get(0).value()).getStruct("after");
        assertThat(after).isNotNull();

        // The regression signal for the whole class of encoding bugs: no field of a fully
        // populated row may arrive null.
        for (org.apache.kafka.connect.data.Field field : after.schema().fields()) {
            assertThat(after.get(field)).as("Field %s must not be null", field.name()).isNotNull();
        }

        Object bytesValue = after.get("c_bytes");
        byte[] bytes = bytesValue instanceof ByteBuffer ? ((ByteBuffer) bytesValue).array() : (byte[]) bytesValue;
        assertThat(bytes).containsExactly(0x01, 0xff);

        assertThat(after.getString("c_bit")).isEqualTo("1010");
        assertThat(after.getString("c_varbit")).isEqualTo("011");
        assertThat(after.getString("c_interval")).contains("02:03:04");
        assertThat(after.getString("c_inet")).isEqualTo("192.168.1.10/24");
        assertThat(after.getString("c_jsonb")).contains("\"a\"");
        assertThat(after.getString("c_uuid")).isEqualTo("f47ac10b-58cc-4372-a567-0e02b2c3d479");
        assertThat(after.getString("c_dec")).isEqualTo("12.345");
        assertThat(after.getFloat64("c_float")).isEqualTo(1.5d);
        assertThat(after.getString("c_int_arr")).isEqualTo("[1,2,3]");
        assertThat(after.getString("c_str_arr")).isEqualTo("[\"x\",\"y\"]");
        assertThat(after.getString("c_dec_arr")).contains("1.25").contains("2.5");
        assertThat(after.getArray("c_vector")).containsExactly(1.5d, 2.5d, 3.5d);
        assertThat(after.getString("c_geog")).contains("Point");
        assertThat(after.getString("c_geom")).contains("LineString");
        assertThat(after.getBoolean("c_bool")).isTrue();
        assertThat(after.getInt16("c_int2")).isEqualTo((short) 7);
        assertThat(after.get("c_ts")).isInstanceOf(Long.class);
        assertThat(after.getString("c_tstz")).contains("2026-06-08");
        assertThat(after.get("c_date")).isInstanceOf(Integer.class);
        assertThat(after.getString("c_str")).isEqualTo("hello");

        // The full envelope must serialize with schemas enabled: this is where a type/value
        // mismatch would surface downstream.
        try (JsonConverter jsonConverter = new JsonConverter()) {
            Map<String, Object> converterConfig = new HashMap<>();
            converterConfig.put("schemas.enable", "true");
            converterConfig.put("converter.type", "value");
            jsonConverter.configure(converterConfig);
            SourceRecord r = records.get(0);
            assertThat(jsonConverter.fromConnectData(r.topic(), r.valueSchema(), r.value())).isNotEmpty();
        }
    }

    @Test
    public void shouldRoundTripBytesPrimaryKeyThroughInsertUpdateAndDelete() throws Exception {
        connection = openDatabase("bytespk_testdb");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS taxlots ("
                    + "taxlot_id BYTES NOT NULL PRIMARY KEY, note STRING NOT NULL)");
            stmt.execute("UPSERT INTO taxlots VALUES (x'f925e84ec3444ce383785556b60dd048', 'open')");
        }

        startTask(baseConnectorConfig("bytespk-test", "bytespk_testdb", "public.taxlots"));

        List<SourceRecord> initial = pollForRecords(1, 45);
        assertThat(initial).as("Should receive the seed row").isNotEmpty();
        Struct seedKey = (Struct) initial.get(0).key();
        assertThat(keyBytes(seedKey)).containsExactly(
                0xf9, 0x25, 0xe8, 0x4e, 0xc3, 0x44, 0x4c, 0xe3, 0x83, 0x78, 0x55, 0x56, 0xb6, 0x0d, 0xd0, 0x48);
        Struct seedAfter = ((Struct) initial.get(0).value()).getStruct("after");
        assertThat(seedAfter.get("taxlot_id")).as("BYTES primary key column must not be null in the row image").isNotNull();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("UPDATE taxlots SET note = 'closed' WHERE taxlot_id = x'f925e84ec3444ce383785556b60dd048'");
        }
        List<SourceRecord> updates = pollForRecords(
                r -> r.value() != null && "u".equals(((Struct) r.value()).getString("op")), 1, 60);
        assertThat(updates).as("Should receive the update event").isNotEmpty();
        assertThat(((Struct) updates.get(0).value()).getStruct("after").getString("note")).isEqualTo("closed");

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DELETE FROM taxlots WHERE taxlot_id = x'f925e84ec3444ce383785556b60dd048'");
        }
        List<SourceRecord> deletes = pollForRecords(
                r -> r.value() != null && "d".equals(((Struct) r.value()).getString("op")), 1, 60);
        assertThat(deletes).as("Should receive the delete event").isNotEmpty();

        SourceRecord delete = deletes.get(0);
        assertThat(delete.key()).as("Delete record must carry its BYTES primary key").isNotNull();
        assertThat(keyBytes((Struct) delete.key())).containsExactly(
                0xf9, 0x25, 0xe8, 0x4e, 0xc3, 0x44, 0x4c, 0xe3, 0x83, 0x78, 0x55, 0x56, 0xb6, 0x0d, 0xd0, 0x48);

        // The production failure point from the report: key conversion with schemas enabled.
        try (JsonConverter jsonConverter = new JsonConverter()) {
            Map<String, Object> converterConfig = new HashMap<>();
            converterConfig.put("schemas.enable", "true");
            converterConfig.put("converter.type", "key");
            jsonConverter.configure(converterConfig);
            assertThat(jsonConverter.fromConnectData(delete.topic(), delete.keySchema(), delete.key())).isNotEmpty();
        }
    }

    private static byte[] keyBytes(Struct key) {
        Object value = key.get("taxlot_id");
        assertThat(value).as("taxlot_id in the record key must not be null").isNotNull();
        return value instanceof ByteBuffer ? ((ByteBuffer) value).array() : (byte[]) value;
    }
}
