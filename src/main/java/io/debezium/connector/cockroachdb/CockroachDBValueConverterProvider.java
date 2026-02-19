/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.Locale;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Json;
import io.debezium.data.vector.DoubleVector;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;

/**
 * Value converter provider for CockroachDB.
 *
 * <p>Maps CockroachDB column types to Kafka Connect schema types and provides
 * converters that transform changefeed JSON values into the appropriate wire format.
 * Supports standard SQL types, CockroachDB-specific types, and the pgvector-compatible
 * {@code VECTOR} type (CockroachDB 24.2+).</p>
 *
 * <p>CockroachDB changefeeds deliver values as JSON, so all incoming data arrives
 * as strings. The converters parse these strings into the target types.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBValueConverterProvider implements ValueConverterProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBValueConverterProvider.class);

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        if (column == null) {
            return SchemaBuilder.string().optional();
        }

        String typeName = column.typeName();
        if (typeName == null) {
            return SchemaBuilder.string().optional();
        }

        switch (typeName.toUpperCase(Locale.ROOT)) {
            // Boolean
            case "BOOL":
            case "BOOLEAN":
                return SchemaBuilder.bool().optional();

            // Integer types
            case "INT2":
            case "SMALLINT":
            case "INT16":
                return SchemaBuilder.int16().optional();
            case "INT4":
            case "INT":
            case "INTEGER":
            case "INT32":
                return SchemaBuilder.int32().optional();
            case "INT8":
            case "BIGINT":
            case "INT64":
            case "SERIAL":
                return SchemaBuilder.int64().optional();

            // Floating-point types
            case "FLOAT4":
            case "REAL":
                return SchemaBuilder.float32().optional();
            case "FLOAT8":
            case "DOUBLE PRECISION":
            case "FLOAT":
                return SchemaBuilder.float64().optional();

            // Fixed-precision
            case "NUMERIC":
            case "DECIMAL":
            case "DEC":
                return SchemaBuilder.string().optional();

            // String types
            case "VARCHAR":
            case "CHAR":
            case "CHARACTER VARYING":
            case "TEXT":
            case "STRING":
            case "NAME":
                return SchemaBuilder.string().optional();

            // Binary
            case "BYTEA":
            case "BYTES":
            case "BLOB":
                return SchemaBuilder.bytes().optional();

            // Date/time types
            case "DATE":
                return SchemaBuilder.int32()
                        .name("io.debezium.time.Date")
                        .optional();
            case "TIME":
            case "TIMETZ":
            case "TIME WITHOUT TIME ZONE":
            case "TIME WITH TIME ZONE":
                return SchemaBuilder.string().optional();
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
            case "TIMESTAMP WITHOUT TIME ZONE":
            case "TIMESTAMP WITH TIME ZONE":
                return SchemaBuilder.int64()
                        .name("io.debezium.time.MicroTimestamp")
                        .optional();
            case "INTERVAL":
                return SchemaBuilder.string().optional();

            // JSON types
            case "JSON":
            case "JSONB":
                return Json.builder();

            // UUID
            case "UUID":
                return SchemaBuilder.string()
                        .name("io.debezium.data.Uuid")
                        .optional();

            // Network types
            case "INET":
                return SchemaBuilder.string().optional();

            // Array types (CockroachDB supports various array types)
            case "INT2[]":
            case "INT4[]":
            case "INT8[]":
            case "FLOAT4[]":
            case "FLOAT8[]":
            case "TEXT[]":
            case "VARCHAR[]":
            case "BOOL[]":
            case "UUID[]":
            case "STRING[]":
                return SchemaBuilder.string().optional();

            // Enum types
            case "ENUM":
                return SchemaBuilder.string().optional();

            // Bit types
            case "BIT":
            case "VARBIT":
            case "BIT VARYING":
                return SchemaBuilder.string().optional();

            // pgvector-compatible VECTOR type (CockroachDB 24.2+)
            case "VECTOR":
                return DoubleVector.builder();

            // Geography/Geometry (CockroachDB spatial types)
            case "GEOGRAPHY":
            case "GEOMETRY":
                return SchemaBuilder.string().optional();

            default:
                if (typeName.toUpperCase(Locale.ROOT).endsWith("[]")) {
                    return SchemaBuilder.string().optional();
                }
                LOGGER.debug("Unknown CockroachDB type '{}', mapping to optional string", typeName);
                return SchemaBuilder.string().optional();
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        if (column == null || fieldDefn == null) {
            return value -> value != null ? value.toString() : null;
        }

        String typeName = column.typeName();
        if (typeName == null) {
            return value -> value != null ? value.toString() : null;
        }

        switch (typeName.toUpperCase(Locale.ROOT)) {
            case "BOOL":
            case "BOOLEAN":
                return value -> {
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof Boolean) {
                        return value;
                    }
                    String s = value.toString().trim().toLowerCase(Locale.ROOT);
                    return "true".equals(s) || "t".equals(s) || "1".equals(s) || "yes".equals(s);
                };

            case "INT2":
            case "SMALLINT":
            case "INT16":
                return value -> value == null ? null : Short.parseShort(value.toString().trim());

            case "INT4":
            case "INT":
            case "INTEGER":
            case "INT32":
                return value -> value == null ? null : Integer.parseInt(value.toString().trim());

            case "INT8":
            case "BIGINT":
            case "INT64":
            case "SERIAL":
                return value -> value == null ? null : Long.parseLong(value.toString().trim());

            case "FLOAT4":
            case "REAL":
                return value -> value == null ? null : Float.parseFloat(value.toString().trim());

            case "FLOAT8":
            case "DOUBLE PRECISION":
            case "FLOAT":
                return value -> value == null ? null : Double.parseDouble(value.toString().trim());

            // pgvector-compatible VECTOR type (CockroachDB 24.2+)
            case "VECTOR":
                return value -> {
                    if (value == null) {
                        return null;
                    }
                    Schema schema = fieldDefn.schema();
                    return DoubleVector.fromLogical(schema, value.toString());
                };

            case "TIMESTAMP":
            case "TIMESTAMPTZ":
            case "TIMESTAMP WITHOUT TIME ZONE":
            case "TIMESTAMP WITH TIME ZONE":
                return value -> {
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof Long) {
                        return value;
                    }
                    String ts = value.toString().trim();
                    try {
                        java.time.Instant instant = java.time.Instant.parse(ts);
                        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
                    }
                    catch (java.time.format.DateTimeParseException e) {
                        try {
                            java.time.ZonedDateTime zdt = java.time.ZonedDateTime.parse(ts, java.time.format.DateTimeFormatter.ISO_DATE_TIME);
                            java.time.Instant inst = zdt.toInstant();
                            return inst.getEpochSecond() * 1_000_000L + inst.getNano() / 1_000L;
                        }
                        catch (java.time.format.DateTimeParseException e2) {
                            return null;
                        }
                    }
                };

            case "DATE":
                return value -> {
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof Integer) {
                        return value;
                    }
                    try {
                        return (int) java.time.LocalDate.parse(value.toString().trim()).toEpochDay();
                    }
                    catch (java.time.format.DateTimeParseException e) {
                        return null;
                    }
                };

            // JSON types -- pass through as string
            case "JSON":
            case "JSONB":
                return value -> value != null ? value.toString() : null;

            default:
                return value -> value != null ? value.toString() : null;
        }
    }
}
