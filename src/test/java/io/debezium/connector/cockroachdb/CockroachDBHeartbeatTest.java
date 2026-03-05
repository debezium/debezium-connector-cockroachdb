/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for heartbeat support via CockroachDB resolved timestamps.
 * CockroachDB uses HLC (Hybrid Logical Clock) timestamps: {@code <wall_time_nanos>.<logical_counter>}.
 *
 * @author Virag Tripathi
 */
public class CockroachDBHeartbeatTest {

    @Test
    public void shouldParseHlcTimestamp() {
        // 1772695406971781718 nanos = 1772695406 seconds + 971781718 nanos
        Instant result = CockroachDBStreamingChangeEventSource.parseResolvedTimestamp("1772695406971781718.0000000000");
        assertThat(result.getEpochSecond()).isEqualTo(1772695406L);
        assertThat(result.getNano()).isEqualTo(971781718);
    }

    @Test
    public void shouldParseHlcTimestampWithLogicalCounter() {
        // The logical counter after the dot is ignored for time purposes
        Instant result = CockroachDBStreamingChangeEventSource.parseResolvedTimestamp("1772695406971781718.0000000001");
        assertThat(result.getEpochSecond()).isEqualTo(1772695406L);
        assertThat(result.getNano()).isEqualTo(971781718);
    }

    @Test
    public void shouldParseHlcTimestampWithZeroNanos() {
        // Exact second boundary: 1709000000 seconds = 1709000000000000000 nanos
        Instant result = CockroachDBStreamingChangeEventSource.parseResolvedTimestamp("1709000000000000000.0000000000");
        assertThat(result.getEpochSecond()).isEqualTo(1709000000L);
        assertThat(result.getNano()).isEqualTo(0);
    }

    @Test
    public void shouldParseTimestampWithoutLogicalCounter() {
        // Just wall-time nanos, no dot
        Instant result = CockroachDBStreamingChangeEventSource.parseResolvedTimestamp("1709000000500000000");
        assertThat(result.getEpochSecond()).isEqualTo(1709000000L);
        assertThat(result.getNano()).isEqualTo(500000000);
    }

    @Test
    public void shouldReturnEpochForNullTimestamp() {
        Instant result = CockroachDBStreamingChangeEventSource.parseResolvedTimestamp(null);
        assertThat(result).isEqualTo(Instant.EPOCH);
    }

    @Test
    public void shouldReturnEpochForEmptyTimestamp() {
        Instant result = CockroachDBStreamingChangeEventSource.parseResolvedTimestamp("");
        assertThat(result).isEqualTo(Instant.EPOCH);
    }

    @Test
    public void shouldReturnEpochForMalformedTimestamp() {
        Instant result = CockroachDBStreamingChangeEventSource.parseResolvedTimestamp("not-a-timestamp");
        assertThat(result).isEqualTo(Instant.EPOCH);
    }

    @Test
    public void shouldParseRecentTimestampToReasonableDate() {
        // March 2026 timestamp: wall time in nanoseconds
        Instant result = CockroachDBStreamingChangeEventSource.parseResolvedTimestamp("1772695406971781718.0000000000");
        assertThat(result).isAfter(Instant.parse("2026-01-01T00:00:00Z"));
        assertThat(result).isBefore(Instant.parse("2027-01-01T00:00:00Z"));
    }

    @Test
    public void shouldAdvanceOffsetOnResolvedTimestamp() {
        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(
                io.debezium.config.Configuration.create()
                        .with("database.hostname", "localhost")
                        .with("database.port", "26257")
                        .with("database.user", "root")
                        .with("database.dbname", "testdb")
                        .with("topic.prefix", "test")
                        .build());

        CockroachDBOffsetContext offsetContext = new CockroachDBOffsetContext(config);
        String initialCursor = offsetContext.getCursor();

        String resolvedTs = "1772695406971781718.0000000000";
        offsetContext.setCursor(resolvedTs);
        offsetContext.setTimestamp(CockroachDBStreamingChangeEventSource.parseResolvedTimestamp(resolvedTs));

        assertThat(offsetContext.getCursor()).isEqualTo(resolvedTs);
        assertThat(offsetContext.getCursor()).isNotEqualTo(initialCursor);
        assertThat(offsetContext.getTimestamp().getEpochSecond()).isEqualTo(1772695406L);
    }

    @Test
    public void shouldAdvanceOffsetMultipleTimesMonotonically() {
        CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(
                io.debezium.config.Configuration.create()
                        .with("database.hostname", "localhost")
                        .with("database.port", "26257")
                        .with("database.user", "root")
                        .with("database.dbname", "testdb")
                        .with("topic.prefix", "test")
                        .build());

        CockroachDBOffsetContext offsetContext = new CockroachDBOffsetContext(config);

        String ts1 = "1772695406971781718.0000000000";
        String ts2 = "1772695416971781718.0000000000"; // 10 seconds later

        offsetContext.setCursor(ts1);
        offsetContext.setTimestamp(CockroachDBStreamingChangeEventSource.parseResolvedTimestamp(ts1));
        Instant first = offsetContext.getTimestamp();

        offsetContext.setCursor(ts2);
        offsetContext.setTimestamp(CockroachDBStreamingChangeEventSource.parseResolvedTimestamp(ts2));
        Instant second = offsetContext.getTimestamp();

        assertThat(second).isAfter(first);
        assertThat(offsetContext.getCursor()).isEqualTo(ts2);
    }
}
