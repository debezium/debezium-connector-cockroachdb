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
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;

/**
 * Unit tests for SourceInfo.
 *
 * @author Virag Tripathi
 */
public class SourceInfoTest {

    private SourceInfo sourceInfo;
    private CockroachDBConnectorConfig config;

    @Before
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
    }

    @Test
    public void shouldHandleConfiguration() {
        assertThat(config).isNotNull();
        assertThat(config.getHostname()).isEqualTo("localhost");
        assertThat(config.getDatabaseName()).isEqualTo("testdb");
    }

    @Test
    public void shouldHandleTimestampOperations() {
        Instant testTimestamp = Instant.now();
        sourceInfo.setSourceTime(testTimestamp);
        assertThat(sourceInfo).isNotNull();
    }

    @Test
    public void shouldHandleSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        assertThat(sourceInfo).isNotNull();
    }

    @Test
    public void shouldHandleSchema() {
        Schema schema = sourceInfo.schema();
        assertThat(schema).isNotNull();
    }

    @Test
    public void shouldHandleDatabaseName() {
        String dbName = sourceInfo.database();
        assertThat(dbName).isEqualTo("testdb");
    }

    @Test
    public void shouldHandleClusterName() {
        String clusterName = sourceInfo.cluster();
        assertThat(clusterName).isEqualTo("test");
    }

    @Test
    public void shouldHandleResolvedTimestamp() {
        String resolvedTimestamp = "2023-01-01T00:00:00Z";
        sourceInfo.setResolvedTimestamp(resolvedTimestamp);
        assertThat(sourceInfo.resolvedTimestamp()).isEqualTo(resolvedTimestamp);
    }

    @Test
    public void shouldHandleHlc() {
        String hlc = "1234567890.123456789";
        sourceInfo.setHlc(hlc);
        assertThat(sourceInfo.hlc()).isEqualTo(hlc);
    }

    @Test
    public void shouldHandleTsNanos() {
        Long tsNanos = 1234567890123456789L;
        sourceInfo.setTsNanos(tsNanos);
        assertThat(sourceInfo.tsNanos()).isEqualTo(tsNanos);
    }

    @Test
    public void shouldHandleToString() {
        String stringRepresentation = sourceInfo.toString();
        assertThat(stringRepresentation).isNotNull();
        assertThat(stringRepresentation).isNotEmpty();
    }
}