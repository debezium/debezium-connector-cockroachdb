/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;

/**
 * Parses CockroachDB column default value expressions returned by JDBC metadata
 * into typed Java objects suitable for Kafka Connect schema defaults.
 *
 * <p>CockroachDB annotates default expressions with a CRDB-specific {@code :::TYPE}
 * suffix (for example {@code 'PENDING':::STRING}, {@code 0:::INT8},
 * {@code current_timestamp():::TIMESTAMPTZ}). This converter strips the annotation,
 * unwraps single-quoted string literals, and parses scalar values to the Java type
 * that matches the column's logical type. Function-generated defaults
 * ({@code current_timestamp()}, {@code gen_random_uuid()}, {@code now()},
 * {@code unique_rowid()}, etc.) are reported as empty so the database computes them
 * at insert time.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBDefaultValueConverter implements DefaultValueConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBDefaultValueConverter.class);

    private static final Pattern CRDB_TYPE_ANNOTATION = Pattern.compile(":::[A-Za-z0-9_ ]+(\\[\\])?$");
    private static final Pattern PG_TYPE_CAST = Pattern.compile("::[A-Za-z0-9_ ]+(\\[\\])?$");
    private static final Pattern FUNCTION_CALL = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*\\s*\\(");

    private static final Set<String> KEYWORD_DEFAULTS = new HashSet<>();
    static {
        KEYWORD_DEFAULTS.add("CURRENT_TIMESTAMP");
        KEYWORD_DEFAULTS.add("CURRENT_DATE");
        KEYWORD_DEFAULTS.add("CURRENT_TIME");
        KEYWORD_DEFAULTS.add("LOCALTIMESTAMP");
        KEYWORD_DEFAULTS.add("LOCALTIME");
        KEYWORD_DEFAULTS.add("NOW");
        KEYWORD_DEFAULTS.add("TRANSACTION_TIMESTAMP");
        KEYWORD_DEFAULTS.add("STATEMENT_TIMESTAMP");
    }

    private final Map<String, Mapper> mappers = new HashMap<>();

    public CockroachDBDefaultValueConverter() {
        register("BOOL", (c, v) -> Boolean.parseBoolean(v));
        register("BOOLEAN", (c, v) -> Boolean.parseBoolean(v));

        register("INT2", (c, v) -> Short.parseShort(v));
        register("SMALLINT", (c, v) -> Short.parseShort(v));
        register("INT16", (c, v) -> Short.parseShort(v));

        register("INT4", (c, v) -> Integer.parseInt(v));
        register("INT", (c, v) -> Integer.parseInt(v));
        register("INTEGER", (c, v) -> Integer.parseInt(v));
        register("INT32", (c, v) -> Integer.parseInt(v));

        register("INT8", (c, v) -> Long.parseLong(v));
        register("BIGINT", (c, v) -> Long.parseLong(v));
        register("INT64", (c, v) -> Long.parseLong(v));
        register("SERIAL", (c, v) -> Long.parseLong(v));

        register("FLOAT4", (c, v) -> Float.parseFloat(v));
        register("REAL", (c, v) -> Float.parseFloat(v));

        register("FLOAT8", (c, v) -> Double.parseDouble(v));
        register("DOUBLE PRECISION", (c, v) -> Double.parseDouble(v));
        register("FLOAT", (c, v) -> Double.parseDouble(v));

        // DECIMAL/NUMERIC schemas are string-backed in CockroachDBValueConverterProvider
        register("NUMERIC", (c, v) -> new BigDecimal(v).toPlainString());
        register("DECIMAL", (c, v) -> new BigDecimal(v).toPlainString());
        register("DEC", (c, v) -> new BigDecimal(v).toPlainString());

        Mapper passthroughString = (c, v) -> v;
        register("VARCHAR", passthroughString);
        register("CHAR", passthroughString);
        register("CHARACTER VARYING", passthroughString);
        register("TEXT", passthroughString);
        register("STRING", passthroughString);
        register("NAME", passthroughString);
        register("UUID", passthroughString);
        register("INET", passthroughString);
        register("JSON", passthroughString);
        register("JSONB", passthroughString);
        register("INTERVAL", passthroughString);
        register("GEOGRAPHY", passthroughString);
        register("GEOMETRY", passthroughString);
        register("ENUM", passthroughString);
        register("BIT", passthroughString);
        register("VARBIT", passthroughString);
        register("BIT VARYING", passthroughString);

        register("DATE", (c, v) -> (int) LocalDate.parse(v).toEpochDay());

        // CockroachDB writes temporal defaults as SQL literals with a space separator (for example
        // '2026-01-01 12:00:00') and an hour-only offset (for example '+00'). Reuse the shared
        // conversions so the default value matches the schema type each temporal column maps to:
        // TIMESTAMP -> MicroTimestamp (long), TIMESTAMPTZ -> ZonedTimestamp (string),
        // TIME -> MicroTime (long), TIMETZ -> ZonedTime (string).
        Mapper microTimestampMapper = (c, v) -> CockroachDBTemporalConversions.parseTimestampMicros(v.replace(' ', 'T'));
        register("TIMESTAMP", microTimestampMapper);
        register("TIMESTAMP WITHOUT TIME ZONE", microTimestampMapper);

        Mapper zonedTimestampMapper = (c, v) -> CockroachDBTemporalConversions.normalizeZonedTimestamp(v.replace(' ', 'T'));
        register("TIMESTAMPTZ", zonedTimestampMapper);
        register("TIMESTAMP WITH TIME ZONE", zonedTimestampMapper);

        Mapper microTimeMapper = (c, v) -> CockroachDBTemporalConversions.parseTimeMicros(v);
        register("TIME", microTimeMapper);
        register("TIME WITHOUT TIME ZONE", microTimeMapper);

        Mapper zonedTimeMapper = (c, v) -> CockroachDBTemporalConversions.normalizeZonedTime(v);
        register("TIMETZ", zonedTimeMapper);
        register("TIME WITH TIME ZONE", zonedTimeMapper);

        register("BYTEA", (c, v) -> v.getBytes());
        register("BYTES", (c, v) -> v.getBytes());
        register("BLOB", (c, v) -> v.getBytes());

        // pgvector-compatible VECTOR type (CockroachDB 24.2+): '[1.0,2.0,3.0]':::VECTOR
        register("VECTOR", (c, v) -> parseVector(v));
    }

    static List<Double> parseVector(String literal) {
        String s = literal.trim();
        if (s.startsWith("[") && s.endsWith("]")) {
            s = s.substring(1, s.length() - 1);
        }
        s = s.trim();
        if (s.isEmpty()) {
            return new ArrayList<>();
        }
        String[] parts = s.split(",");
        List<Double> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            out.add(Double.parseDouble(p.trim()));
        }
        return out;
    }

    private void register(String typeName, Mapper mapper) {
        mappers.put(typeName.toUpperCase(Locale.ROOT), mapper);
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        if (defaultValueExpression == null) {
            return Optional.empty();
        }

        String expr = defaultValueExpression.trim();
        if (expr.isEmpty() || "NULL".equalsIgnoreCase(expr) || expr.toUpperCase(Locale.ROOT).startsWith("NULL:")) {
            return Optional.empty();
        }

        String stripped = stripTypeAnnotation(expr);

        String upper = stripped.toUpperCase(Locale.ROOT);
        if (KEYWORD_DEFAULTS.contains(upper) || isFunctionCall(stripped)) {
            return Optional.empty();
        }

        String literal = unquote(stripped);

        String typeName = column.typeName();
        if (typeName == null) {
            return Optional.empty();
        }

        String key = typeName.toUpperCase(Locale.ROOT);
        Mapper mapper = mappers.get(key);
        if (mapper == null && key.endsWith("[]")) {
            // CRDB array defaults arrive as e.g. ARRAY['a','b']:::STRING[] — pass through as the raw literal
            mapper = (c, v) -> v;
        }
        if (mapper == null) {
            LOGGER.debug("No default-value mapper for CockroachDB type '{}'; skipping default", typeName);
            return Optional.empty();
        }

        try {
            Object parsed = mapper.parse(column, literal);
            return Optional.ofNullable(parsed);
        }
        catch (Exception e) {
            LOGGER.warn("Cannot parse default value '{}' for column '{}' of type '{}'; ignoring",
                    defaultValueExpression, column.name(), typeName);
            LOGGER.debug("Default-value parsing failed", e);
            return Optional.empty();
        }
    }

    static String stripTypeAnnotation(String expr) {
        String s = CRDB_TYPE_ANNOTATION.matcher(expr).replaceFirst("");
        if (s.equals(expr)) {
            s = PG_TYPE_CAST.matcher(expr).replaceFirst("");
        }
        return s.trim();
    }

    static String unquote(String expr) {
        if (expr.length() >= 2 && expr.charAt(0) == '\'' && expr.charAt(expr.length() - 1) == '\'') {
            String inner = expr.substring(1, expr.length() - 1);
            return inner.replace("''", "'");
        }
        return expr;
    }

    static boolean isFunctionCall(String expr) {
        return FUNCTION_CALL.matcher(expr).find();
    }

    @FunctionalInterface
    private interface Mapper {
        Object parse(Column column, String value) throws Exception;
    }
}
