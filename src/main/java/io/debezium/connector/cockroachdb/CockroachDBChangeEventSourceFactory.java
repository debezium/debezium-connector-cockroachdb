/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Factory for CockroachDB change event sources (snapshot, streaming, incremental).
 * Provides ChangeEventSource implementations to the coordinator.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeEventSourceFactory implements ChangeEventSourceFactory<CockroachDBPartition, CockroachDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBChangeEventSourceFactory.class);

    private final CockroachDBConnectorConfig config;
    private final EventDispatcher<CockroachDBPartition, TableId> dispatcher;
    private final CockroachDBSchema schema;
    private final Clock clock;

    public CockroachDBChangeEventSourceFactory(
                                               CockroachDBConnectorConfig config,
                                               EventDispatcher<CockroachDBPartition, TableId> dispatcher,
                                               CockroachDBSchema schema,
                                               Clock clock) {
        this.config = config;
        this.dispatcher = dispatcher;
        this.schema = schema;
        this.clock = clock;
    }

    @Override
    public SnapshotChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> getSnapshotChangeEventSource(
                                                                                                                  SnapshotProgressListener<CockroachDBPartition> snapshotProgressListener,
                                                                                                                  NotificationService<CockroachDBPartition, CockroachDBOffsetContext> notificationService) {
        return new CockroachDBSnapshotChangeEventSource(config);
    }

    @Override
    public StreamingChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> getStreamingChangeEventSource() {
        LOGGER.debug("Creating CockroachDBStreamingChangeEventSource");
        return new CockroachDBStreamingChangeEventSource(config, dispatcher, schema, clock);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<CockroachDBPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                    CockroachDBOffsetContext offsetContext,
                                                                                                                                                    SnapshotProgressListener<CockroachDBPartition> snapshotProgressListener,
                                                                                                                                                    DataChangeEventListener<CockroachDBPartition> dataChangeEventListener,
                                                                                                                                                    NotificationService<CockroachDBPartition, CockroachDBOffsetContext> notificationService) {
        if (config.getSignalingDataCollectionIds().isEmpty()) {
            LOGGER.debug("No signal data collection configured, incremental snapshots disabled");
            return Optional.empty();
        }

        final CockroachDBConnection connection = new CockroachDBConnection(config);
        return Optional.of(new CockroachDBSignalBasedIncrementalSnapshotChangeEventSource(
                config,
                connection,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }

}
