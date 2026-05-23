/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Schema manager for the CockroachDB connector.
 *
 * <p>Discovers tables via {@link io.debezium.jdbc.JdbcConnection#readSchema} and registers them
 * with the parent so the {@link io.debezium.pipeline.EventDispatcher} can resolve table schemas
 * at dispatch time. Honors {@code schema.include.list} / {@code schema.exclude.list} and
 * {@code table.include.list} / {@code table.exclude.list} via the inherited {@code TableFilter}.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBSchema extends RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSchema.class);

    public CockroachDBSchema(CdcSourceTaskContext<CockroachDBConnectorConfig> cdcSourceTaskContext,
                             TopicNamingStrategy<TableId> topicNamingStrategy) {
        super(
                cdcSourceTaskContext.getConfig(),
                topicNamingStrategy,
                cdcSourceTaskContext.getConfig().getTableFilters().dataCollectionFilter(),
                cdcSourceTaskContext.getConfig().getColumnFilter(),
                new TableSchemaBuilder(
                        new CockroachDBValueConverterProvider(),
                        new CockroachDBDefaultValueConverter(),
                        cdcSourceTaskContext.getConfig().schemaNameAdjuster(),
                        new CustomConverterRegistry(Collections.emptyList()),
                        cdcSourceTaskContext.getConfig().getSourceInfoStructMaker().schema(),
                        cdcSourceTaskContext.getConfig().getFieldNamer(),
                        false,
                        cdcSourceTaskContext.getConfig().getEventConvertingFailureHandlingMode()),
                false,
                cdcSourceTaskContext.getConfig().getKeyMapper(),
                cdcSourceTaskContext);
    }

    public void initialize(CockroachDBConnectorConfig config) {
        LOGGER.info("Initializing CockroachDBSchema");
        try (CockroachDBConnection connection = new CockroachDBConnection(config)) {
            connection.connect();
            connection.readSchema(tables(), null, null, getTableFilter(), null, true);
            refreshSchemas();
            LOGGER.info("Schema initialization completed with {} tables", tableIds().size());
        }
        catch (SQLException e) {
            LOGGER.error("Failed to initialize schema", e);
            throw new RuntimeException("Failed to initialize schema", e);
        }
    }

    public Table refreshTable(CockroachDBConnection connection, TableId tableId) throws SQLException {
        LOGGER.info("Refreshing schema for table {}", tableId);
        Tables temp = new Tables();
        connection.readSchema(temp, null, null, tableId::equals, null, true);
        if (temp.size() == 0) {
            LOGGER.warn("Table {} no longer exists after schema refresh", tableId);
            return null;
        }
        Table updatedTable = temp.forTable(tableId);
        tables().overwriteTable(updatedTable);
        refreshSchema(tableId);
        LOGGER.info("Schema refreshed for table {} ({} columns)", tableId, updatedTable.columns().size());
        return updatedTable;
    }

    private void refreshSchemas() {
        clearSchemas();
        tableIds().forEach(this::refreshSchema);
    }

    public List<TableId> getDiscoveredTables() {
        return new ArrayList<>(tableIds());
    }

    @Override
    public void close() {
        LOGGER.info("Closing CockroachDBSchema");
        super.close();
    }
}
