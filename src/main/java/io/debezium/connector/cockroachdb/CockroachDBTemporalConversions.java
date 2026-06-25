/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Conversions from the temporal strings CockroachDB emits into the wire shapes the Debezium time
 * logical types require. Shared by the change-record emitter (streaming row values), the value
 * converter (schema-typed values), and the default-value converter (column defaults), so all three
 * agree on a single representation per type.
 *
 * <p>CockroachDB emits temporal values as JSON strings in an enriched changefeed, and as quoted SQL
 * literals in column defaults. The relevant shapes are:</p>
 *
 * <ul>
 *   <li>{@code TIMESTAMP} (no zone): {@code 2026-06-15T14:36:34.873} -> {@link io.debezium.time.MicroTimestamp}
 *       (microseconds since epoch, interpreted as UTC).</li>
 *   <li>{@code TIMESTAMPTZ}: {@code 2026-06-15T14:36:34.873Z} -> {@link io.debezium.time.ZonedTimestamp}
 *       (ISO-8601 string with an offset).</li>
 *   <li>{@code TIME} (no zone): {@code 14:36:34.873} -> {@link io.debezium.time.MicroTime}
 *       (microseconds since midnight).</li>
 *   <li>{@code TIMETZ}: {@code 14:36:34.873+00} -> {@link io.debezium.time.ZonedTime}
 *       (ISO-8601 string with an offset).</li>
 * </ul>
 *
 * <p>The zoned types are the subtle ones. {@code ZonedTimestamp} parses with
 * {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME} and {@code ZonedTime} with
 * {@link DateTimeFormatter#ISO_OFFSET_TIME}; both require an offset. CockroachDB writes the offset in
 * the hour-only {@code +HH} form (for example {@code 14:36:34.873+00}), which those formatters reject,
 * and a zoneless value can also reach the zoned path (for example an old row replayed after a column
 * was altered between {@code TIMESTAMP} and {@code TIMESTAMPTZ}). Both cases are normalized here so the
 * emitted string always satisfies the logical type, rather than passing the raw value straight through.</p>
 *
 * @author Virag Tripathi
 */
final class CockroachDBTemporalConversions {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBTemporalConversions.class);

    /**
     * Parses an ISO-8601 timestamp whose offset is the hour-only {@code +HH} form (or {@code Z}) that
     * CockroachDB can emit. {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME} requires at least
     * {@code +HH:MM}, so this covers the hour-only case before the UTC fallback.
     */
    private static final DateTimeFormatter HOUR_OFFSET_DATE_TIME = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .appendOffset("+HH", "Z")
            .toFormatter();

    /**
     * Parses an ISO-8601 time whose offset is the hour-only {@code +HH} form (or {@code Z}). This is
     * the common CockroachDB shape for {@code TIMETZ} (for example {@code 14:36:34.873+00}).
     */
    private static final DateTimeFormatter HOUR_OFFSET_TIME = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendOffset("+HH", "Z")
            .toFormatter();

    private CockroachDBTemporalConversions() {
    }

    /**
     * Parses a CockroachDB timestamp string into microseconds since epoch for
     * {@link io.debezium.time.MicroTimestamp}.
     *
     * <p>Handles {@code TIMESTAMPTZ} with a trailing {@code Z} or an explicit {@code +HH:MM} offset,
     * and {@code TIMESTAMP} without a zone (interpreted as UTC, matching Debezium's convention for
     * timestamps without a time zone). Returns {@code null} for null, empty, or unparseable input.</p>
     */
    static Long parseTimestampMicros(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        String v = value.trim();
        // TIMESTAMPTZ with a trailing 'Z'.
        try {
            return toMicros(Instant.parse(v));
        }
        catch (DateTimeParseException ignored) {
        }
        // TIMESTAMPTZ with an explicit offset (for example +00:00).
        try {
            return toMicros(ZonedDateTime.parse(v, DateTimeFormatter.ISO_DATE_TIME).toInstant());
        }
        catch (DateTimeParseException ignored) {
        }
        // TIMESTAMP without time zone (no offset): interpret as UTC.
        try {
            return toMicros(LocalDateTime.parse(v, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC));
        }
        catch (DateTimeParseException e) {
            LOGGER.warn("Cannot parse timestamp '{}', returning null", value);
            return null;
        }
    }

    /**
     * Parses a CockroachDB {@code TIME} string ({@code HH:mm:ss[.ffffff]}) into microseconds since
     * midnight for {@link io.debezium.time.MicroTime}. Returns {@code null} for null, empty, or
     * unparseable input.
     */
    static Long parseTimeMicros(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return LocalTime.parse(value.trim(), DateTimeFormatter.ISO_LOCAL_TIME).toNanoOfDay() / 1_000L;
        }
        catch (DateTimeParseException e) {
            LOGGER.warn("Cannot parse time '{}', returning null", value);
            return null;
        }
    }

    /**
     * Normalizes a CockroachDB {@code TIMESTAMPTZ} value into an offset-qualified ISO-8601 string that
     * satisfies {@link io.debezium.time.ZonedTimestamp} ({@link DateTimeFormatter#ISO_OFFSET_DATE_TIME}).
     * An hour-only {@code +HH} offset is widened to {@code +HH:MM}, and a zoneless value is interpreted
     * as UTC. Returns {@code null} for null, empty, or unparseable input.
     */
    static String normalizeZonedTimestamp(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        String v = value.trim();
        // Already offset-qualified (Z, +HH:MM, ...): canonicalize via the target formatter.
        try {
            return OffsetDateTime.parse(v, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        catch (DateTimeParseException ignored) {
        }
        // Hour-only offset (for example +00): widen to +HH:MM.
        try {
            return OffsetDateTime.parse(v, HOUR_OFFSET_DATE_TIME)
                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        catch (DateTimeParseException ignored) {
        }
        // Zoneless: interpret as UTC.
        try {
            return LocalDateTime.parse(v, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        catch (DateTimeParseException e) {
            LOGGER.warn("Cannot normalize timestamptz '{}', returning null", value);
            return null;
        }
    }

    /**
     * Normalizes a CockroachDB {@code TIMETZ} value into an offset-qualified ISO-8601 string that
     * satisfies {@link io.debezium.time.ZonedTime} ({@link DateTimeFormatter#ISO_OFFSET_TIME}). An
     * hour-only {@code +HH} offset (the shape CockroachDB emits, for example {@code +00}) is widened to
     * {@code +HH:MM}, and a zoneless value is interpreted as UTC. Returns {@code null} for null, empty,
     * or unparseable input.
     */
    static String normalizeZonedTime(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        String v = value.trim();
        // Already offset-qualified (Z, +HH:MM, ...): canonicalize via the target formatter.
        try {
            return OffsetTime.parse(v, DateTimeFormatter.ISO_OFFSET_TIME)
                    .format(DateTimeFormatter.ISO_OFFSET_TIME);
        }
        catch (DateTimeParseException ignored) {
        }
        // Hour-only offset (for example +00): widen to +HH:MM.
        try {
            return OffsetTime.parse(v, HOUR_OFFSET_TIME)
                    .format(DateTimeFormatter.ISO_OFFSET_TIME);
        }
        catch (DateTimeParseException ignored) {
        }
        // Zoneless: interpret as UTC.
        try {
            return LocalTime.parse(v, DateTimeFormatter.ISO_LOCAL_TIME)
                    .atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ISO_OFFSET_TIME);
        }
        catch (DateTimeParseException e) {
            LOGGER.warn("Cannot normalize timetz '{}', returning null", value);
            return null;
        }
    }

    private static long toMicros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }
}
