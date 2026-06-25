/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;

/**
 * Unit tests for {@link CockroachDBDefaultValueConverter}.
 *
 * @author Virag Tripathi
 */
public class CockroachDBDefaultValueConverterTest {

    private final CockroachDBDefaultValueConverter converter = new CockroachDBDefaultValueConverter();

    @Test
    public void shouldStripCrdbTypeAnnotation() {
        assertThat(CockroachDBDefaultValueConverter.stripTypeAnnotation("0:::INT8")).isEqualTo("0");
        assertThat(CockroachDBDefaultValueConverter.stripTypeAnnotation("'PENDING':::STRING")).isEqualTo("'PENDING'");
        assertThat(CockroachDBDefaultValueConverter.stripTypeAnnotation("current_timestamp():::TIMESTAMPTZ"))
                .isEqualTo("current_timestamp()");
    }

    @Test
    public void shouldUnquoteSingleQuotedLiteral() {
        assertThat(CockroachDBDefaultValueConverter.unquote("'PENDING'")).isEqualTo("PENDING");
        assertThat(CockroachDBDefaultValueConverter.unquote("'it''s'")).isEqualTo("it's");
        assertThat(CockroachDBDefaultValueConverter.unquote("0")).isEqualTo("0");
    }

    @Test
    public void shouldDetectFunctionCall() {
        assertThat(CockroachDBDefaultValueConverter.isFunctionCall("current_timestamp()")).isTrue();
        assertThat(CockroachDBDefaultValueConverter.isFunctionCall("gen_random_uuid()")).isTrue();
        assertThat(CockroachDBDefaultValueConverter.isFunctionCall("0")).isFalse();
    }

    @Test
    public void shouldParseIntegerDefault() {
        Column col = column("INT8");
        Optional<Object> parsed = converter.parseDefaultValue(col, "0:::INT8");
        assertThat(parsed).isPresent().get().isInstanceOf(Long.class).isEqualTo(0L);
    }

    @Test
    public void shouldParseBooleanDefault() {
        Column col = column("BOOL");
        assertThat(converter.parseDefaultValue(col, "false")).contains(Boolean.FALSE);
        assertThat(converter.parseDefaultValue(col, "true")).contains(Boolean.TRUE);
    }

    @Test
    public void shouldParseStringDefault() {
        Column col = column("STRING");
        assertThat(converter.parseDefaultValue(col, "'PENDING':::STRING")).contains("PENDING");
    }

    @Test
    public void shouldParseDecimalDefaultAsString() {
        Column col = column("DECIMAL");
        assertThat(converter.parseDefaultValue(col, "1.50:::DECIMAL")).contains(new BigDecimal("1.50").toPlainString());
    }

    @Test
    public void shouldSkipFunctionDefault() {
        Column col = column("TIMESTAMPTZ");
        assertThat(converter.parseDefaultValue(col, "current_timestamp():::TIMESTAMPTZ")).isEmpty();
    }

    @Test
    public void shouldSkipNullDefault() {
        Column col = column("INT8");
        assertThat(converter.parseDefaultValue(col, null)).isEmpty();
        assertThat(converter.parseDefaultValue(col, "NULL")).isEmpty();
        assertThat(converter.parseDefaultValue(col, "NULL:::INT8")).isEmpty();
    }

    @Test
    public void shouldParseDateAsEpochDays() {
        Column col = column("DATE");
        Optional<Object> parsed = converter.parseDefaultValue(col, "'2024-01-01':::DATE");
        assertThat(parsed).isPresent().get().isInstanceOf(Integer.class);
    }

    @Test
    public void shouldParseTimestampDefaultAsMicros() {
        // CockroachDB writes the literal with a space separator; it maps to MicroTimestamp (long).
        Column col = column("TIMESTAMP");
        Optional<Object> parsed = converter.parseDefaultValue(col, "'2026-01-01 12:00:00':::TIMESTAMP");
        assertThat(parsed).isPresent().get().isInstanceOf(Long.class);
    }

    @Test
    public void shouldParseTimestamptzDefaultAsOffsetQualifiedString() {
        // The default must match the ZonedTimestamp (string) schema, with the hour-only offset widened.
        Column col = column("TIMESTAMPTZ");
        Optional<Object> parsed = converter.parseDefaultValue(col, "'2026-01-01 10:00:00+00':::TIMESTAMPTZ");
        assertThat(parsed).isPresent().get().isInstanceOf(String.class).isEqualTo("2026-01-01T10:00:00Z");
    }

    @Test
    public void shouldParseTimeDefaultAsMicrosSinceMidnight() {
        // TIME maps to MicroTime (long), not a string. 12:34:56 = 45296s.
        Column col = column("TIME");
        Optional<Object> parsed = converter.parseDefaultValue(col, "'12:34:56':::TIME");
        assertThat(parsed).isPresent().get().isInstanceOf(Long.class).isEqualTo(45_296_000_000L);
    }

    @Test
    public void shouldParseTimetzDefaultAsOffsetQualifiedString() {
        // TIMETZ maps to ZonedTime (string); the hour-only "+02" offset is widened to "+02:00".
        Column col = column("TIMETZ");
        Optional<Object> parsed = converter.parseDefaultValue(col, "'12:34:56+02':::TIMETZ");
        assertThat(parsed).isPresent().get().isInstanceOf(String.class).isEqualTo("12:34:56+02:00");
    }

    @Test
    public void shouldReturnEmptyForUnknownType() {
        Column col = column("WIDGET");
        assertThat(converter.parseDefaultValue(col, "'foo'")).isEmpty();
    }

    @Test
    public void shouldParseVectorLiteral() {
        List<Double> parsed = CockroachDBDefaultValueConverter.parseVector("[1.0, 2.0, 3.5]");
        assertThat(parsed).containsExactly(1.0, 2.0, 3.5);
    }

    @Test
    public void shouldParseVectorDefault() {
        Column col = column("VECTOR");
        Optional<Object> parsed = converter.parseDefaultValue(col, "'[1.0,2.0,3.0]':::VECTOR");
        assertThat(parsed).isPresent().get().isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Double> values = (List<Double>) parsed.get();
        assertThat(values).containsExactly(1.0, 2.0, 3.0);
    }

    @Test
    public void shouldParseArrayDefaultAsRawLiteral() {
        Column col = column("STRING[]");
        Optional<Object> parsed = converter.parseDefaultValue(col, "ARRAY['a','b']:::STRING[]");
        assertThat(parsed).isPresent();
    }

    private static Column column(String typeName) {
        ColumnEditor editor = Column.editor().name("c").type(typeName);
        return editor.create();
    }
}
