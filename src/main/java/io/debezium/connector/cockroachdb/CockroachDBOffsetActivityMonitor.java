/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The changefeed cursor and the intermediate consumer positions are compared against the
 * values captured when the monitor was last consulted, and when neither has moved, a stale
 * result is reported. The cursor is the resolved timestamp that governs where the changefeed resumes
 * on restart; because CockroachDB emits resolved timestamps at the configured resolved
 * interval even when the database is idle, an unmoved cursor typically indicates the
 * changefeed job is paused or failed, or that events are no longer being received from the
 * changefeed sink. The intermediate consumer positions are additionally compared so that
 * progress through the intermediate buffer between resolved timestamps is not reported as
 * stale.
 * <p>
 * No check is performed until the changefeed has delivered its first resolved timestamp or
 * the intermediate consumer has recorded a position, so a long-running initial scan is not
 * reported as stale.
 *
 * @author Chris Cranford
 */
public class CockroachDBOffsetActivityMonitor implements OffsetActivityMonitor<CockroachDBPartition, CockroachDBOffsetContext> {

    private final Duration checkInterval;

    private String previousCursor;
    private Map<String, Long> previousConsumerOffsets;

    public CockroachDBOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(CockroachDBPartition partition, CockroachDBOffsetContext offsetContext) {
        final String cursor = offsetContext.getCursor();
        final Map<String, Long> consumerOffsets = offsetContext.getConsumerOffsets();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if ((isResolvedCursor(cursor) || !consumerOffsets.isEmpty())
                && Objects.equals(previousCursor, cursor)
                && Objects.equals(previousConsumerOffsets, consumerOffsets)) {
            result = StaleOffsetsResult.stale(
                    ("Offset cursor %s and intermediate consumer positions have not changed in %d milliseconds. " +
                            "CockroachDB emits resolved timestamps at the configured resolved interval even when the " +
                            "database is idle, so this may indicate the changefeed job is paused or failed, events are " +
                            "no longer being received from the changefeed sink, or the resolved interval is longer than " +
                            "the check interval.")
                            .formatted(cursor, checkInterval.toMillis()));
        }

        // Update tracked stats
        previousCursor = cursor;
        previousConsumerOffsets = consumerOffsets;

        return result;
    }

    /**
     * Returns {@code true} when the cursor holds a resolved timestamp rather than one of the
     * pre-streaming sentinel values.
     */
    private static boolean isResolvedCursor(String cursor) {
        return cursor != null
                && !CockroachDBOffsetContext.CURSOR_INITIAL.equals(cursor)
                && !CockroachDBOffsetContext.CURSOR_NOW.equals(cursor);
    }
}