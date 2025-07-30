/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;

/**
 * Tests for CockroachDB source information handling.
 *
 * @author Virag Tripathi
 */
public class SourceInfoTest {

    private SourceInfo sourceInfo;
    private CockroachDBConnectorConfig config;

    @BeforeEach
    public void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        config = new CockroachDBConnectorConfig(Configuration.from(props));
        sourceInfo = new SourceInfo(config);
    }

    @Test
    public void shouldCreateSourceInfo() {
        assertThat(sourceInfo).isNotNull();
        assertThat(sourceInfo.database()).isEqualTo("testdb");
        assertThat(sourceInfo.cluster()).isEqualTo("test");
    }

    @Test
    public void shouldSetSourceTime() {
        Instant now = Instant.now();
        sourceInfo.setSourceTime(now);

        assertThat(sourceInfo.timestamp()).isEqualTo(now);
    }

    @Test
    public void shouldHandleNullSourceTime() {
        sourceInfo.setSourceTime(null);

        assertThat(sourceInfo.timestamp()).isEqualTo(Instant.EPOCH);
    }

    @Test
    public void shouldSetResolvedTimestamp() {
        String resolvedTs = "2023-01-01T00:00:00Z";
        sourceInfo.setResolvedTimestamp(resolvedTs);

        assertThat(sourceInfo.resolvedTimestamp()).isEqualTo(resolvedTs);
    }

    @Test
    public void shouldSetHlc() {
        String hlc = "1234567890.123456789";
        sourceInfo.setHlc(hlc);

        assertThat(sourceInfo.hlc()).isEqualTo(hlc);
    }

    @Test
    public void shouldSetTsNanos() {
        Long tsNanos = 1234567890123456789L;
        sourceInfo.setTsNanos(tsNanos);

        assertThat(sourceInfo.tsNanos()).isEqualTo(tsNanos);
    }

    @Test
    public void shouldCreateStruct() {
        Instant now = Instant.now();
        sourceInfo.setSourceTime(now);
        sourceInfo.setResolvedTimestamp("2023-01-01T00:00:00Z");
        sourceInfo.setHlc("1234567890.123456789");
        sourceInfo.setTsNanos(1234567890123456789L);

        Struct struct = sourceInfo.struct();
        assertThat(struct).isNotNull();
        assertThat(struct.getString("db")).isEqualTo("testdb");
        assertThat(struct.getString("cluster")).isEqualTo("test");
        assertThat(struct.getInt64("ts_ms")).isEqualTo(now.toEpochMilli());
    }

    @Test
    public void shouldHaveCorrectSchema() {
        Schema schema = sourceInfo.schema();
        assertThat(schema).isNotNull();
        assertThat(schema.field("db")).isNotNull();
        assertThat(schema.field("cluster")).isNotNull();
        assertThat(schema.field("ts_ms")).isNotNull();
        assertThat(schema.field("resolved_ts")).isNotNull();
    }

    @Test
    public void shouldHandleDefaultValues() {
        sourceInfo.setResolvedTimestamp("2023-01-01T00:00:00Z");

        Struct struct = sourceInfo.struct();
        assertThat(struct.getString("db")).isEqualTo("testdb");
        assertThat(struct.getString("cluster")).isEqualTo("test");
        assertThat(struct.getInt64("ts_ms")).isEqualTo(Instant.EPOCH.toEpochMilli());
    }

    @Test
    public void shouldUpdateTimestamp() {
        Instant timestamp = Instant.now();
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.setResolvedTimestamp("2023-01-01T00:00:00Z");

        Struct struct = sourceInfo.struct();
        assertThat(struct.getInt64("ts_ms")).isEqualTo(timestamp.toEpochMilli());
    }

    @Test
    public void shouldMaintainDatabaseName() {
        sourceInfo.setResolvedTimestamp("2023-01-01T00:00:00Z");

        Struct struct = sourceInfo.struct();
        assertThat(struct.getString("db")).isEqualTo("testdb");
    }

    @Test
    public void shouldMaintainServerName() {
        sourceInfo.setResolvedTimestamp("2023-01-01T00:00:00Z");

        Struct struct = sourceInfo.struct();
        assertThat(struct.getString("cluster")).isEqualTo("test");
    }

    @Test
    public void shouldHandleMultipleUpdates() {
        Instant time1 = Instant.now();
        sourceInfo.setSourceTime(time1);
        assertThat(sourceInfo.timestamp()).isEqualTo(time1);

        Instant time2 = Instant.now().plusSeconds(1);
        sourceInfo.setSourceTime(time2);
        assertThat(sourceInfo.timestamp()).isEqualTo(time2);
    }

    @Test
    public void shouldHandleConfiguration() {
        assertThat(config).isNotNull();
        assertThat(config.getHostname()).isEqualTo("localhost");
        assertThat(config.getDatabaseName()).isEqualTo("testdb");
    }

    @Test
    public void shouldHandleSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        assertThat(sourceInfo).isNotNull();
    }

    @Test
    public void shouldHandleToString() {
        String stringRepresentation = sourceInfo.toString();
        assertThat(stringRepresentation).isNotNull();
        assertThat(stringRepresentation).isNotEmpty();
    }
}
