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
    public void shouldHandleConsumerOffsetOperations() {
        String key = "offsetcommit-test.db.public.orders:0";
        offsetContext.setConsumerOffset(key, 12345L);

        assertThat(offsetContext.getConsumerOffset(key)).isEqualTo(12345L);
        // The consumer offset is persisted in the source offset so a restart can resume from it.
        assertThat(offsetContext.getOffset().get(CockroachDBOffsetContext.CONSUMER_OFFSET_PREFIX + key))
                .isEqualTo(12345L);
        // Unknown keys return null (first start for that partition).
        assertThat(offsetContext.getConsumerOffset("unknown:9")).isNull();
    }

    @Test
    public void shouldRoundTripConsumerOffsetThroughLoader() {
        offsetContext.setConsumerOffset("topic-a:0", 100L);
        offsetContext.setConsumerOffset("topic-a:1", 200L);

        Map<String, ?> persisted = offsetContext.getOffset();
        CockroachDBOffsetContext restored = new CockroachDBOffsetContext.Loader(config).load(persisted);

        assertThat(restored.getConsumerOffset("topic-a:0")).isEqualTo(100L);
        assertThat(restored.getConsumerOffset("topic-a:1")).isEqualTo(200L);
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
    public void shouldMarkSnapshotCompletedInOffsetAndRoundTrip() {
        // Before completion the flag is false (matches other connectors before the snapshot finishes).
        assertThat(offsetContext.getOffset().get("snapshot_completed")).isEqualTo("false");

        // Completing the snapshot sets the persisted flag to true.
        offsetContext.preSnapshotCompletion();
        assertThat(offsetContext.getOffset().get("snapshot_completed")).isEqualTo("true");

        // The completed state survives a reload from the stored offset.
        CockroachDBOffsetContext restored = new CockroachDBOffsetContext.Loader(config)
                .load(offsetContext.getOffset());
        assertThat(restored.getOffset().get("snapshot_completed")).isEqualTo("true");
    }

    @Test
    public void shouldHandleConstructorWithCursorAndTimestamp() {
        String testCursor = "test-cursor";
        Instant testTimestamp = Instant.now();

        CockroachDBOffsetContext context = new CockroachDBOffsetContext(config, testCursor, testTimestamp,
                new io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext<>(false));
        assertThat(context).isNotNull();
        assertThat(context.getCursor()).isEqualTo(testCursor);
        assertThat(context.getTimestamp()).isEqualTo(testTimestamp);
    }

    @Test
    public void shouldHandleDefaultValues() {
        Map<String, ?> offset = offsetContext.getOffset();
        assertThat(offset).containsKey("offset.cursor");
        assertThat(offset).containsKey("offset.timestamp");
        assertThat(offset).containsKey("snapshot_completed");

        // Default cursor should match the CURSOR_NOW constant
        assertThat(offset.get("offset.cursor")).isEqualTo(CockroachDBOffsetContext.CURSOR_NOW);
        assertThat(offset.get("snapshot_completed")).isEqualTo("false");
    }

    @Test
    public void shouldDefineCursorSentinelConstants() {
        assertThat(CockroachDBOffsetContext.CURSOR_INITIAL).isEqualTo("initial");
        assertThat(CockroachDBOffsetContext.CURSOR_NOW).isEqualTo("now");
    }

    @Test
    public void shouldUseCursorInitialAsFallbackWhenCursorNull() {
        CockroachDBOffsetContext ctx = new CockroachDBOffsetContext(config);
        ctx.setCursor(null);

        Map<String, ?> offset = ctx.getOffset();
        assertThat(offset.get(CockroachDBOffsetContext.CURSOR))
                .isEqualTo(CockroachDBOffsetContext.CURSOR_INITIAL);
    }
}