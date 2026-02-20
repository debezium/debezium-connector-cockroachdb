/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;

/**
 * Snapshot change event source for CockroachDB.
 *
 * <p>CockroachDB CDC uses native changefeed {@code initial_scan} to backfill existing
 * rows. The actual snapshot data flows through the same Kafka consumer path as
 * streaming events (in {@link CockroachDBStreamingChangeEventSource}). This source
 * acts as a coordinator that determines whether a snapshot should occur based on the
 * configured {@code snapshot.mode} and creates a default offset if none exists.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBSnapshotChangeEventSource
        implements SnapshotChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSnapshotChangeEventSource.class);

    private final CockroachDBConnectorConfig connectorConfig;

    public CockroachDBSnapshotChangeEventSource(CockroachDBConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public SnapshotResult<CockroachDBOffsetContext> execute(
                                                            ChangeEventSourceContext context,
                                                            CockroachDBPartition partition,
                                                            CockroachDBOffsetContext previousOffset,
                                                            SnapshottingTask snapshottingTask)
            throws InterruptedException {

        CockroachDBConnectorConfig.SnapshotMode snapshotMode = connectorConfig.getSnapshotMode();
        LOGGER.info("Snapshot phase: mode={}, previousOffset={}", snapshotMode.getValue(),
                previousOffset != null ? previousOffset.getCursor() : "none");

        CockroachDBOffsetContext offset = previousOffset != null
                ? previousOffset
                : new CockroachDBOffsetContext(connectorConfig);

        // CockroachDB handles snapshot via changefeed initial_scan in the streaming phase.
        // This source skips so the coordinator proceeds to streaming, which will create the
        // changefeed with the appropriate initial_scan option.
        LOGGER.info("Snapshot delegated to CockroachDB changefeed initial_scan (snapshot.mode={})",
                snapshotMode.getValue());
        return SnapshotResult.skipped(offset);
    }

    @Override
    public SnapshottingTask getSnapshottingTask(CockroachDBPartition partition,
                                                CockroachDBOffsetContext previousOffset) {
        return new SnapshottingTask(false, false, Collections.emptyList(), Collections.emptyMap(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(CockroachDBPartition partition,
                                                        CockroachDBOffsetContext previousOffset,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return new SnapshottingTask(false, false, Collections.emptyList(), Collections.emptyMap(), false);
    }
}
