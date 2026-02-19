/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Emits change records for CockroachDB changefeed events by extracting
 * column values from the enriched envelope JSON and aligning them to the
 * table schema discovered from {@code information_schema}.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeRecordEmitter extends RelationalChangeRecordEmitter<CockroachDBPartition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBChangeRecordEmitter.class);

    private final Table table;
    private final JsonNode afterNode;
    private final JsonNode beforeNode;
    private final Envelope.Operation operation;

    public CockroachDBChangeRecordEmitter(CockroachDBPartition partition,
                                          CockroachDBOffsetContext offsetContext,
                                          Clock clock,
                                          RelationalDatabaseConnectorConfig connectorConfig,
                                          Table table,
                                          Envelope.Operation operation,
                                          JsonNode afterNode,
                                          JsonNode beforeNode) {
        super(partition, offsetContext, clock, connectorConfig);
        this.table = table;
        this.operation = operation;
        this.afterNode = afterNode;
        this.beforeNode = beforeNode;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        if (beforeNode == null || beforeNode.isNull() || beforeNode.isMissingNode()) {
            return null;
        }
        return extractColumnValues(beforeNode);
    }

    @Override
    protected Object[] getNewColumnValues() {
        if (afterNode == null || afterNode.isNull() || afterNode.isMissingNode()) {
            return null;
        }
        return extractColumnValues(afterNode);
    }

    /**
     * Extracts values from a JSON node in the same order as the table's columns.
     * Each value is converted to a Java type appropriate for the column's JDBC type.
     */
    private Object[] extractColumnValues(JsonNode dataNode) {
        List<Column> columns = table.columns();
        Object[] values = new Object[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            JsonNode fieldNode = dataNode.get(column.name());

            if (fieldNode == null || fieldNode.isNull()) {
                values[i] = null;
            }
            else {
                values[i] = convertJsonValue(fieldNode, column);
            }
        }
        return values;
    }

    /**
     * Converts a JSON value to the appropriate Java type based on the column's data type.
     * Changefeed JSON uses standard JSON types; this maps them to Java types that
     * the {@code CockroachDBValueConverterProvider} and Kafka Connect expect.
     */
    private static Object convertJsonValue(JsonNode node, Column column) {
        if (node.isNull()) {
            return null;
        }

        String typeName = column.typeName();
        if (typeName == null) {
            return node.asText();
        }

        switch (typeName.toUpperCase()) {
            case "BOOL":
            case "BOOLEAN":
                return node.asBoolean();
            case "INT2":
            case "SMALLINT":
                return (short) node.asInt();
            case "INT4":
            case "INT":
            case "INTEGER":
                return node.asInt();
            case "INT8":
            case "BIGINT":
            case "SERIAL":
                return node.asLong();
            case "FLOAT4":
            case "REAL":
                return (float) node.asDouble();
            case "FLOAT8":
            case "DOUBLE PRECISION":
                return node.asDouble();
            case "NUMERIC":
            case "DECIMAL":
                return node.asText();
            case "BYTEA":
            case "BYTES":
                return node.asText();
            case "TIMESTAMP":
            case "TIMESTAMP WITHOUT TIME ZONE":
            case "TIMESTAMPTZ":
            case "TIMESTAMP WITH TIME ZONE":
                return parseTimestampMicros(node.asText());
            case "DATE":
                return parseDateDays(node.asText());
            default:
                // STRING, TEXT, VARCHAR, JSON, JSONB, UUID, INET, INTERVAL,
                // ENUM, arrays, GEOGRAPHY, GEOMETRY, VECTOR
                if (node.isTextual()) {
                    return node.asText();
                }
                else if (node.isNumber()) {
                    return node.numberValue();
                }
                else if (node.isBoolean()) {
                    return node.asBoolean();
                }
                else {
                    // Objects, arrays -> JSON string representation
                    return node.toString();
                }
        }
    }

    /**
     * Parses a CockroachDB timestamp string into microseconds since epoch.
     * CockroachDB changefeed emits ISO-8601 strings like {@code "2026-02-19T19:06:58.916109Z"}.
     * Debezium's {@code MicroTimestamp} schema expects {@code long} (microseconds since epoch).
     */
    private static Long parseTimestampMicros(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            Instant instant = Instant.parse(value);
            return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
        }
        catch (DateTimeParseException e) {
            try {
                ZonedDateTime zdt = ZonedDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME);
                Instant instant = zdt.toInstant();
                return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
            }
            catch (DateTimeParseException e2) {
                LOGGER.warn("Cannot parse timestamp '{}', returning null", value);
                return null;
            }
        }
    }

    /**
     * Parses a CockroachDB date string into days since epoch.
     * Debezium's {@code Date} schema expects {@code int} (days since 1970-01-01).
     */
    private static Integer parseDateDays(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            LocalDate date = LocalDate.parse(value);
            return (int) date.toEpochDay();
        }
        catch (DateTimeParseException e) {
            LOGGER.warn("Cannot parse date '{}', returning null", value);
            return null;
        }
    }
}
