/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

/**
 * CockroachDB implementation of {@link SignalBasedIncrementalSnapshotChangeEventSource}.
 * Refreshes the table schema from {@code information_schema} before each incremental
 * snapshot to ensure the schema is current (leveraging the schema evolution support
 * added in debezium/dbz#1629).
 *
 * @author Virag Tripathi
 */
public class CockroachDBSignalBasedIncrementalSnapshotChangeEventSource
        extends SignalBasedIncrementalSnapshotChangeEventSource<CockroachDBPartition, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSignalBasedIncrementalSnapshotChangeEventSource.class);

    private final CockroachDBConnection cockroachDBConnection;
    private final CockroachDBSchema cockroachDBSchema;

    public CockroachDBSignalBasedIncrementalSnapshotChangeEventSource(
                                                                      RelationalDatabaseConnectorConfig config,
                                                                      JdbcConnection jdbcConnection,
                                                                      EventDispatcher<CockroachDBPartition, TableId> dispatcher,
                                                                      DatabaseSchema<?> databaseSchema,
                                                                      Clock clock,
                                                                      SnapshotProgressListener<CockroachDBPartition> progressListener,
                                                                      DataChangeEventListener<CockroachDBPartition> dataChangeEventListener,
                                                                      NotificationService<CockroachDBPartition, ? extends OffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        if (!(jdbcConnection instanceof CockroachDBConnection)) {
            throw new IllegalArgumentException("Expected CockroachDBConnection but got " + jdbcConnection.getClass().getName());
        }
        if (!(databaseSchema instanceof CockroachDBSchema)) {
            throw new IllegalArgumentException("Expected CockroachDBSchema but got " + databaseSchema.getClass().getName());
        }
        this.cockroachDBConnection = (CockroachDBConnection) jdbcConnection;
        this.cockroachDBSchema = (CockroachDBSchema) databaseSchema;
    }

    @Override
    protected Table refreshTableSchema(Table table) throws SQLException {
        LOGGER.debug("Refreshing table '{}' schema for incremental snapshot.", table.id());
        try {
            cockroachDBSchema.refreshTable(cockroachDBConnection, table.id());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while refreshing schema for table " + table.id(), e);
        }
        catch (Exception e) {
            throw new SQLException("Failed to refresh schema for table " + table.id(), e);
        }
        return cockroachDBSchema.tableFor(table.id());
    }
}
