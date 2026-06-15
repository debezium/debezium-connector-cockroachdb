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

import io.debezium.relational.Column;

/**
 * Unit tests for {@link CockroachDBChangeRecordEmitter#parseTimestampMicros(String)}.
 *
 * <p>Covers both forms CockroachDB emits: {@code TIMESTAMPTZ} (with a trailing {@code Z} or an
 * explicit offset) and {@code TIMESTAMP} without a time zone (no offset), which previously failed to
 * parse and produced a null value, including for primary-key columns.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeRecordEmitterTest {

    @Test
    public void shouldParseTimestampWithoutTimeZoneAsUtc() {
        // TIMESTAMP (no zone): the format that regressed to null. Interpreted as UTC.
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("1970-01-01T00:00:01.000"))
                .isEqualTo(1_000_000L);
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("1970-01-01T00:00:00.000001"))
                .isEqualTo(1L);
        // Microsecond precision is preserved (6 fractional digits).
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("1970-01-01T00:00:00.123456"))
                .isEqualTo(123_456L);
        // No fractional seconds.
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("1970-01-01T00:00:02"))
                .isEqualTo(2_000_000L);
    }

    @Test
    public void shouldParseRealWorldTimestampWithoutTimeZone() {
        // The exact format from the field report (TIMESTAMP primary key) must not be null.
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("2026-06-08T11:01:45.883"))
                .isNotNull();
    }

    @Test
    public void shouldParseTimestampTzWithTrailingZ() {
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("1970-01-01T00:00:01Z"))
                .isEqualTo(1_000_000L);
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("1970-01-01T00:00:00.916109Z"))
                .isEqualTo(916_109L);
    }

    @Test
    public void shouldParseTimestampTzWithExplicitOffset() {
        // 00:00:01 at +00:00 is epoch+1s; at +01:00 it is one hour earlier in UTC.
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("1970-01-01T00:00:01+00:00"))
                .isEqualTo(1_000_000L);
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("1970-01-01T01:00:01+01:00"))
                .isEqualTo(1_000_000L);
    }

    @Test
    public void shouldReturnNullForNullEmptyOrUnparseable() {
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros(null)).isNull();
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("")).isNull();
        assertThat(CockroachDBChangeRecordEmitter.parseTimestampMicros("not-a-timestamp")).isNull();
    }

    @Test
    public void shouldExtractTemporalColumnValuesWithCorrectJavaTypes() throws Exception {
        // Real CockroachDB enriched changefeed output for each temporal type (captured from v25.4.10).
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
        // TIMESTAMPTZ -> ZonedTimestamp (String, passed through).
        assertThat(values[1]).isEqualTo("2026-06-08T09:01:45.883Z");
        // TIME -> MicroTime (Long micros since midnight).
        assertThat(values[2]).isEqualTo(39_705_883_000L);
        // TIMETZ -> ZonedTime (String, short "+02" offset carried through).
        assertThat(values[3]).isEqualTo("11:01:45.883+02");
        // DATE -> Date (Integer days since epoch).
        assertThat(values[4]).isInstanceOf(Integer.class);
    }

    @Test
    public void shouldParseTimeToMicrosSinceMidnight() {
        // CockroachDB emits TIME as "HH:mm:ss[.ffffff]"; MicroTime is micros since midnight.
        assertThat(CockroachDBChangeRecordEmitter.parseTimeMicros("00:00:01")).isEqualTo(1_000_000L);
        assertThat(CockroachDBChangeRecordEmitter.parseTimeMicros("00:00:00.000001")).isEqualTo(1L);
        // 11:01:45.883 = (11*3600 + 1*60 + 45)s + 0.883s = 39705.883s.
        assertThat(CockroachDBChangeRecordEmitter.parseTimeMicros("11:01:45.883")).isEqualTo(39_705_883_000L);
        assertThat(CockroachDBChangeRecordEmitter.parseTimeMicros(null)).isNull();
        assertThat(CockroachDBChangeRecordEmitter.parseTimeMicros("")).isNull();
        assertThat(CockroachDBChangeRecordEmitter.parseTimeMicros("nope")).isNull();
    }
}
