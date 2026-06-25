/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CockroachDBTemporalConversions}, covering the temporal strings CockroachDB
 * emits (in both enriched changefeed JSON and SQL column defaults) against the Debezium time logical
 * type contracts: {@code MicroTimestamp}/{@code MicroTime} (long) and {@code ZonedTimestamp}/
 * {@code ZonedTime} (offset-qualified ISO-8601 string).
 *
 * @author Virag Tripathi
 */
public class CockroachDBTemporalConversionsTest {

    @Test
    public void parseTimestampMicrosHandlesZonelessAsUtc() {
        // TIMESTAMP without a zone: the format that previously regressed to null. Interpreted as UTC.
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("1970-01-01T00:00:01.000")).isEqualTo(1_000_000L);
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("1970-01-01T00:00:00.000001")).isEqualTo(1L);
        // Microsecond precision preserved (6 fractional digits).
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("1970-01-01T00:00:00.123456")).isEqualTo(123_456L);
        // No fractional seconds.
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("1970-01-01T00:00:02")).isEqualTo(2_000_000L);
        // The exact field-reported value must not be null.
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("2026-06-08T11:01:45.883")).isNotNull();
    }

    @Test
    public void parseTimestampMicrosHandlesZonedForms() {
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("1970-01-01T00:00:01Z")).isEqualTo(1_000_000L);
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("1970-01-01T00:00:00.916109Z")).isEqualTo(916_109L);
        // Explicit offset: 01:00:01 at +01:00 is epoch+1s in UTC.
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("1970-01-01T00:00:01+00:00")).isEqualTo(1_000_000L);
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("1970-01-01T01:00:01+01:00")).isEqualTo(1_000_000L);
    }

    @Test
    public void parseTimestampMicrosReturnsNullForBadInput() {
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros(null)).isNull();
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("")).isNull();
        assertThat(CockroachDBTemporalConversions.parseTimestampMicros("not-a-timestamp")).isNull();
    }

    @Test
    public void parseTimeMicrosReturnsMicrosSinceMidnight() {
        assertThat(CockroachDBTemporalConversions.parseTimeMicros("00:00:01")).isEqualTo(1_000_000L);
        assertThat(CockroachDBTemporalConversions.parseTimeMicros("00:00:00.000001")).isEqualTo(1L);
        // 11:01:45.883 = (11*3600 + 1*60 + 45)s + 0.883s = 39705.883s.
        assertThat(CockroachDBTemporalConversions.parseTimeMicros("11:01:45.883")).isEqualTo(39_705_883_000L);
        assertThat(CockroachDBTemporalConversions.parseTimeMicros(null)).isNull();
        assertThat(CockroachDBTemporalConversions.parseTimeMicros("")).isNull();
        assertThat(CockroachDBTemporalConversions.parseTimeMicros("nope")).isNull();
    }

    @Test
    public void normalizeZonedTimestampProducesOffsetQualifiedString() {
        // A zoned value (trailing Z) already satisfies ISO_OFFSET_DATE_TIME and is kept.
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("2026-06-15T14:36:34.873Z"))
                .isEqualTo("2026-06-15T14:36:34.873Z");
        // A zoneless value (for example an old row replayed under a TZ schema) is interpreted as UTC.
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("2026-06-15T14:36:34.873"))
                .isEqualTo("2026-06-15T14:36:34.873Z");
        // An hour-only "+HH" offset is widened to "+HH:MM".
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("2026-06-15T14:36:34.873+05"))
                .isEqualTo("2026-06-15T14:36:34.873+05:00");
        // A zero hour-only offset normalizes to Z.
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("2026-06-15T14:36:34.873+00"))
                .isEqualTo("2026-06-15T14:36:34.873Z");
        // A half-hour offset is preserved.
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("2026-06-15T14:36:34.873+05:30"))
                .isEqualTo("2026-06-15T14:36:34.873+05:30");
        // Field-reported zoneless values with 2 and 3 fractional digits (the exact failing inputs).
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("2026-06-07T00:16:01.07"))
                .isEqualTo("2026-06-07T00:16:01.07Z");
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("2026-06-15T14:36:34.873"))
                .isEqualTo("2026-06-15T14:36:34.873Z");
    }

    @Test
    public void normalizeZonedTimestampOutputAlwaysParsesWithLogicalTypeFormatter() {
        for (String in : new String[]{ "2026-06-15T14:36:34.873", "2026-06-15T14:36:34.873Z",
                "2026-06-15T14:36:34.873+05", "2026-06-15T14:36:34.873+05:30", "2026-06-15T14:36:34-08" }) {
            String out = CockroachDBTemporalConversions.normalizeZonedTimestamp(in);
            assertThatNoException()
                    .as("normalized '%s' -> '%s' must parse with ISO_OFFSET_DATE_TIME", in, out)
                    .isThrownBy(() -> DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(out));
        }
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp(null)).isNull();
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("")).isNull();
        assertThat(CockroachDBTemporalConversions.normalizeZonedTimestamp("not-a-timestamp")).isNull();
    }

    @Test
    public void normalizeZonedTimeWidensHourOnlyOffset() {
        // CockroachDB emits a TIMETZ offset as "+HH" (for example +02); normalize to "+HH:MM".
        assertThat(CockroachDBTemporalConversions.normalizeZonedTime("11:01:45.883+02"))
                .isEqualTo("11:01:45.883+02:00");
        // A zero hour-only offset normalizes to Z.
        assertThat(CockroachDBTemporalConversions.normalizeZonedTime("14:36:34.873+00"))
                .isEqualTo("14:36:34.873Z");
        // A zoneless value is interpreted as UTC.
        assertThat(CockroachDBTemporalConversions.normalizeZonedTime("14:36:34.873"))
                .isEqualTo("14:36:34.873Z");
        // A half-hour offset is preserved.
        assertThat(CockroachDBTemporalConversions.normalizeZonedTime("11:01:45.883+05:30"))
                .isEqualTo("11:01:45.883+05:30");
    }

    @Test
    public void normalizeZonedTimeOutputAlwaysParsesWithLogicalTypeFormatter() {
        for (String in : new String[]{ "11:01:45.883", "14:36:34.873+00", "11:01:45.883+02",
                "11:01:45.883+05:30", "11:01:45-08" }) {
            String out = CockroachDBTemporalConversions.normalizeZonedTime(in);
            assertThatNoException()
                    .as("normalized '%s' -> '%s' must parse with ISO_OFFSET_TIME", in, out)
                    .isThrownBy(() -> DateTimeFormatter.ISO_OFFSET_TIME.parse(out));
        }
        assertThat(CockroachDBTemporalConversions.normalizeZonedTime(null)).isNull();
        assertThat(CockroachDBTemporalConversions.normalizeZonedTime("")).isNull();
        assertThat(CockroachDBTemporalConversions.normalizeZonedTime("not-a-time")).isNull();
    }
}
