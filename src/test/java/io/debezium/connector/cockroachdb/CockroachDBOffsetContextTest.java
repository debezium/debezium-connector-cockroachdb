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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;

/**
 * Tests for CockroachDB offset context.
 *
 * @author Virag Tripathi
 */
public class CockroachDBOffsetContextTest {

    private CockroachDBOffsetContext offsetContext;
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
        offsetContext = new CockroachDBOffsetContext(config);
    }

    @Test
    public void shouldCreateOffsetContext() {
        assertThat(offsetContext).isNotNull();
        assertThat(offsetContext.getOffset()).isNotNull();
    }

    @Test
    public void shouldHandleCursorOperations() {
        String testCursor = "test-cursor-123";
        offsetContext.setCursor(testCursor);

        assertThat(offsetContext.getCursor()).isEqualTo(testCursor);

        Map<String, ?> offset = offsetContext.getOffset();
        assertThat(offset).containsKey("offset.cursor");
        assertThat(offset.get("offset.cursor")).isEqualTo(testCursor);
    }

    @Test
    public void shouldHandleTimestampOperations() {
        Instant testTimestamp = Instant.now();
        offsetContext.setTimestamp(testTimestamp);

        assertThat(offsetContext.getTimestamp()).isEqualTo(testTimestamp);

        Map<String, ?> offset = offsetContext.getOffset();
        assertThat(offset).containsKey("offset.timestamp");
        assertThat(offset.get("offset.timestamp")).isEqualTo(testTimestamp.toEpochMilli());
    }

    @Test
    public void shouldHandleKafkaOffsetOperations() {
        Long testOffset = 12345L;
        offsetContext.setKafkaOffset(testOffset);

        assertThat(offsetContext.getKafkaOffset()).isEqualTo(testOffset);
    }

    @Test
    public void shouldHandleSnapshotRecord() {
        offsetContext.markSnapshotRecord(SnapshotRecord.TRUE);

        Map<String, ?> offset = offsetContext.getOffset();
        assertThat(offset).containsKey("snapshot_completed");
        assertThat(offset.get("snapshot_completed")).isEqualTo("false");
    }

    @Test
    public void shouldHandleSnapshotFalse() {
        offsetContext.markSnapshotRecord(SnapshotRecord.FALSE);

        Map<String, ?> offset = offsetContext.getOffset();
        assertThat(offset).containsKey("snapshot_completed");
        assertThat(offset.get("snapshot_completed")).isEqualTo("false");
    }

    @Test
    public void shouldHandleConstructorWithCursorAndTimestamp() {
        String testCursor = "test-cursor";
        Instant testTimestamp = Instant.now();
        Long testKafkaOffset = 67890L;

        CockroachDBOffsetContext context = new CockroachDBOffsetContext(config, testCursor, testTimestamp, testKafkaOffset);
        assertThat(context).isNotNull();
        assertThat(context.getCursor()).isEqualTo(testCursor);
        assertThat(context.getTimestamp()).isEqualTo(testTimestamp);
        assertThat(context.getKafkaOffset()).isEqualTo(testKafkaOffset);
    }

    @Test
    public void shouldHandleDefaultValues() {
        Map<String, ?> offset = offsetContext.getOffset();
        assertThat(offset).containsKey("offset.cursor");
        assertThat(offset).containsKey("offset.timestamp");
        assertThat(offset).containsKey("snapshot_completed");

        // Default cursor should be "now" if not set
        assertThat(offset.get("offset.cursor")).isEqualTo("now");
        assertThat(offset.get("snapshot_completed")).isEqualTo("false");
    }
}