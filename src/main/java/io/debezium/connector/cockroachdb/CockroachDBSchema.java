/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Schema manager for the CockroachDB connector.
 * Extends Debezium's RelationalDatabaseSchema to manage table schemas.
 *
 * Compatible with Debezium versions that use extended TableSchemaBuilder constructors.
 *
 * @author Virag Tripathi
 */
public class CockroachDBSchema extends RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSchema.class);

    private List<TableId> discoveredTables = new ArrayList<>();

    public CockroachDBSchema(CdcSourceTaskContext<CockroachDBConnectorConfig> cdcSourceTaskContext,
                             TopicNamingStrategy<TableId> topicNamingStrategy) {

        super(
                cdcSourceTaskContext.getConfig(),
                topicNamingStrategy,
                cdcSourceTaskContext.getConfig().getTableFilters().dataCollectionFilter(),
                cdcSourceTaskContext.getConfig().getColumnFilter(),
                new TableSchemaBuilder(
                        new CockroachDBValueConverterProvider(), // custom or stub
                        cdcSourceTaskContext.getConfig().schemaNameAdjuster(),
                        new CustomConverterRegistry(Collections.emptyList()),
                        new CockroachDBSourceInfoStructMaker().schema(), // source struct schema
                        column -> column.name(),
                        false // multiPartitionMode
                ),
                false, // tableIdCaseInsensitive
                null, // KeyMapper: pass null if no custom mapping logic,
                cdcSourceTaskContext);
    }

    public void initialize(CockroachDBConnectorConfig config) {
        LOGGER.info("Initializing CockroachDBSchema");
        try (CockroachDBConnection connection = new CockroachDBConnection(config)) {
            connection.connect();
            loadTables(connection, config);
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize schema", e);
            throw new RuntimeException("Failed to initialize schema", e);
        }
    }

    private void loadTables(CockroachDBConnection connection, CockroachDBConnectorConfig config) throws Exception {
        LOGGER.info("Loading tables from CockroachDB");

        // Query for tables in the configured database
        String sql = "SELECT table_schema, table_name FROM information_schema.tables " +
                "WHERE table_schema = ? AND table_type = 'BASE TABLE'";

        try (var pstmt = connection.connection().prepareStatement(sql)) {
            pstmt.setString(1, config.getSchemaName());
            try (ResultSet rs = pstmt.executeQuery()) {

                while (rs.next()) {
                    String schemaName = rs.getString("table_schema");
                    String tableName = rs.getString("table_name");
                    TableId tableId = new TableId(config.getDatabaseName(), schemaName, tableName);

                    // Add the table to the schema registry
                    LOGGER.info("Found table: {}", tableId);
                    discoveredTables.add(tableId);

                    // Load and register the complete table structure
                    loadTableStructure(connection, tableId);
                }
            }
        }

        LOGGER.info("Schema initialization completed with {} tables", discoveredTables.size());
    }

    private void loadTableStructure(CockroachDBConnection connection, TableId tableId) throws Exception {
        // Query for table columns
        String sql = "SELECT column_name, data_type, is_nullable, column_default " +
                "FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? " +
                "ORDER BY ordinal_position";

        try (var stmt = connection.connection().prepareStatement(sql)) {
            stmt.setString(1, tableId.schema());
            stmt.setString(2, tableId.table());

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    String isNullable = rs.getString("is_nullable");
                    String columnDefault = rs.getString("column_default");

                    // For now, just log the column info - the schema will be built dynamically
                    LOGGER.debug("Column {}: {} (nullable: {}, default: {})",
                            columnName, dataType, isNullable, columnDefault);
                }
            }
        }

        // For now, just log that we found the table
        // The schema will be built dynamically when events are processed
        LOGGER.debug("Found table structure for: {}", tableId);
    }

    @Override
    public void close() {
        LOGGER.info("Closing CockroachDBSchema");
        super.close();
    }

    public List<TableId> getDiscoveredTables() {
        return discoveredTables;
    }
}
