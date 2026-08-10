/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
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
    private final Set<TableId> capturedTables;

    public CockroachDBSignalBasedIncrementalSnapshotChangeEventSource(
                                                                      RelationalDatabaseConnectorConfig config,
                                                                      JdbcConnection jdbcConnection,
                                                                      EventDispatcher<CockroachDBPartition, TableId> dispatcher,
                                                                      DatabaseSchema<?> databaseSchema,
                                                                      Clock clock,
                                                                      SnapshotProgressListener<CockroachDBPartition> progressListener,
                                                                      DataChangeEventListener<CockroachDBPartition> dataChangeEventListener,
                                                                      NotificationService<CockroachDBPartition, ? extends OffsetContext> notificationService,
                                                                      Collection<TableId> capturedTables) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        if (!(jdbcConnection instanceof CockroachDBConnection)) {
            throw new IllegalArgumentException("Expected CockroachDBConnection but got " + jdbcConnection.getClass().getName());
        }
        if (!(databaseSchema instanceof CockroachDBSchema)) {
            throw new IllegalArgumentException("Expected CockroachDBSchema but got " + databaseSchema.getClass().getName());
        }
        this.cockroachDBConnection = (CockroachDBConnection) jdbcConnection;
        this.cockroachDBSchema = (CockroachDBSchema) databaseSchema;
        this.capturedTables = Set.copyOf(capturedTables);
    }

    @Override
    public void addDataCollectionNamesToSnapshot(
                                                 SignalPayload<CockroachDBPartition> signalPayload,
                                                 SnapshotConfiguration snapshotConfiguration)
            throws InterruptedException {
        List<String> uncaptured = findUncapturedDataCollections(
                capturedTables, snapshotConfiguration.getDataCollections());
        if (!uncaptured.isEmpty()) {
            LOGGER.warn("Incremental snapshot signal '{}' requests data collection pattern(s) {} that match no table "
                    + "in the running CockroachDB changefeed capture set. table.include.list patterns are resolved "
                    + "only when the connector starts; a table created later is not streamed even if it matches the "
                    + "configured pattern. Restart the connector to rediscover the table and then reissue the "
                    + "snapshot signal; without that restart, subsequent changes will not be captured.", signalPayload.id, uncaptured);
        }
        super.addDataCollectionNamesToSnapshot(signalPayload, snapshotConfiguration);
    }

    static List<String> findUncapturedDataCollections(Collection<TableId> capturedTables, List<String> requestedPatterns) {
        return requestedPatterns.stream()
                .filter(requested -> {
                    Pattern pattern = Pattern.compile(requested);
                    return capturedTables.stream().noneMatch(table -> matches(pattern, table));
                })
                .collect(Collectors.toList());
    }

    private static boolean matches(Pattern pattern, TableId table) {
        if (pattern.matcher(table.identifier()).matches()) {
            return true;
        }
        String schemaAndTable = table.schema() == null || table.schema().isEmpty()
                ? table.table()
                : table.schema() + "." + table.table();
        return pattern.matcher(schemaAndTable).matches();
    }

    @Override
    protected Table refreshTableSchema(Table table) throws SQLException {
        LOGGER.debug("Refreshing table '{}' schema for incremental snapshot.", table.id());
        cockroachDBSchema.refreshTable(cockroachDBConnection, table.id());
        return cockroachDBSchema.tableFor(table.id());
    }
}
