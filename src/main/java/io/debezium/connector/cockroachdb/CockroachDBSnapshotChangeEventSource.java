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
 * No-op snapshot change event source for CockroachDB.
 *
 * <p>CockroachDB CDC uses sink changefeeds (streaming-only). This source always
 * skips the snapshot phase so that the coordinator proceeds directly to streaming.
 * When no previous offset exists, it creates a default offset context so the
 * coordinator can pass a non-null offset to the streaming source.</p>
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
        LOGGER.info("Snapshot skipped -- CockroachDB connector uses streaming-only CDC via sink changefeeds");
        CockroachDBOffsetContext offset = previousOffset != null
                ? previousOffset
                : new CockroachDBOffsetContext(connectorConfig);
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
