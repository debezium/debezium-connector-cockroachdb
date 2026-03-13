/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Schema manager for the CockroachDB connector.
 *
 * <p>Extends Debezium's {@link RelationalDatabaseSchema} to discover tables from
 * CockroachDB's {@code information_schema} and register them with the parent so that
 * the {@link io.debezium.pipeline.EventDispatcher} can resolve table schemas at dispatch time.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBSchema extends RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSchema.class);

    private final List<TableId> discoveredTables = new ArrayList<>();

    public CockroachDBSchema(CdcSourceTaskContext<CockroachDBConnectorConfig> cdcSourceTaskContext,
                             TopicNamingStrategy<TableId> topicNamingStrategy) {
        super(
                cdcSourceTaskContext.getConfig(),
                topicNamingStrategy,
                cdcSourceTaskContext.getConfig().getTableFilters().dataCollectionFilter(),
                cdcSourceTaskContext.getConfig().getColumnFilter(),
                new TableSchemaBuilder(
                        new CockroachDBValueConverterProvider(),
                        null,
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

    /**
     * Connects to CockroachDB, discovers tables from {@code information_schema},
     * builds {@link Table} objects with column metadata, and registers them with
     * the parent {@link RelationalDatabaseSchema}.
     */
    public void initialize(CockroachDBConnectorConfig config) {
        LOGGER.info("Initializing CockroachDBSchema");
        try (CockroachDBConnection connection = new CockroachDBConnection(config)) {
            connection.connect();
            loadAndRegisterTables(connection, config);
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize schema", e);
            throw new RuntimeException("Failed to initialize schema", e);
        }
    }

    private void loadAndRegisterTables(CockroachDBConnection connection, CockroachDBConnectorConfig config) throws Exception {
        String sql = "SELECT table_schema, table_name FROM information_schema.tables " +
                "WHERE table_schema = ? AND table_type = 'BASE TABLE'";

        try (var pstmt = connection.connection().prepareStatement(sql)) {
            pstmt.setString(1, config.getSchemaName());
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String schemaName = rs.getString("table_schema");
                    String tableName = rs.getString("table_name");
                    TableId tableId = new TableId(config.getDatabaseName(), schemaName, tableName);

                    Table table = buildTable(connection, tableId);
                    if (table != null) {
                        tables().overwriteTable(table);
                        if (tableFor(tableId) != null) {
                            refreshSchema(tableId);
                            discoveredTables.add(tableId);
                            LOGGER.info("Registered table: {} ({} columns)", tableId, table.columns().size());
                        }
                        else {
                            LOGGER.debug("Table {} excluded by table filter", tableId);
                        }
                    }
                }
            }
        }

        LOGGER.info("Schema initialization completed with {} tables", discoveredTables.size());
    }

    /**
     * Refreshes the schema for a single table by re-querying {@code information_schema}.
     * Called when a schema change is detected during changefeed event processing.
     *
     * @return the refreshed {@link Table}, or {@code null} if the table no longer exists
     */
    public Table refreshTable(CockroachDBConnection connection, TableId tableId) throws Exception {
        LOGGER.info("Refreshing schema for table {}", tableId);
        Table table = buildTable(connection, tableId);
        if (table != null) {
            tables().overwriteTable(table);
            refreshSchema(tableId);
            LOGGER.info("Schema refreshed for table {} ({} columns)", tableId, table.columns().size());
        }
        else {
            LOGGER.warn("Table {} no longer exists after schema refresh", tableId);
        }
        return table;
    }

    /**
     * Queries {@code information_schema.columns} and {@code information_schema.key_column_usage}
     * to build a complete {@link Table} with column definitions and primary key metadata.
     */
    private Table buildTable(CockroachDBConnection connection, TableId tableId) throws Exception {
        TableEditor tableEditor = Table.editor().tableId(tableId);
        List<String> pkColumns = new ArrayList<>();

        String columnSql = "SELECT column_name, data_type, is_nullable, column_default, " +
                "character_maximum_length, numeric_precision, numeric_scale, ordinal_position " +
                "FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? " +
                "ORDER BY ordinal_position";

        try (var stmt = connection.connection().prepareStatement(columnSql)) {
            stmt.setString(1, tableId.schema());
            stmt.setString(2, tableId.table());

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    boolean nullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
                    int position = rs.getInt("ordinal_position");

                    int maxLength = rs.getInt("character_maximum_length");
                    int precision = rs.getInt("numeric_precision");
                    int scale = rs.getInt("numeric_scale");

                    ColumnEditor columnEditor = Column.editor()
                            .name(columnName)
                            .type(dataType)
                            .jdbcType(mapCockroachDBTypeToJdbc(dataType))
                            .optional(nullable)
                            .position(position);

                    if (maxLength > 0) {
                        columnEditor.length(maxLength);
                    }
                    if (precision > 0) {
                        columnEditor.length(precision);
                        columnEditor.scale(scale);
                    }

                    tableEditor.addColumn(columnEditor.create());
                    LOGGER.debug("Column {}.{}: {} (jdbc={}, nullable={}, pos={})",
                            tableId, columnName, dataType,
                            mapCockroachDBTypeToJdbc(dataType), nullable, position);
                }
            }
        }

        String pkSql = "SELECT kcu.column_name " +
                "FROM information_schema.key_column_usage kcu " +
                "JOIN information_schema.table_constraints tc " +
                "  ON kcu.constraint_name = tc.constraint_name " +
                "  AND kcu.table_schema = tc.table_schema " +
                "  AND kcu.table_name = tc.table_name " +
                "WHERE kcu.table_schema = ? AND kcu.table_name = ? " +
                "AND tc.constraint_type = 'PRIMARY KEY' " +
                "ORDER BY kcu.ordinal_position";

        try (var stmt = connection.connection().prepareStatement(pkSql)) {
            stmt.setString(1, tableId.schema());
            stmt.setString(2, tableId.table());

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    pkColumns.add(rs.getString("column_name"));
                }
            }
        }

        if (!pkColumns.isEmpty()) {
            tableEditor.setPrimaryKeyNames(pkColumns);
        }

        Table table = tableEditor.create();
        if (table.columns().isEmpty()) {
            LOGGER.warn("Table {} has no columns, skipping registration", tableId);
            return null;
        }
        return table;
    }

    /**
     * Maps CockroachDB data type names to JDBC type codes.
     * CockroachDB uses PostgreSQL-compatible type names.
     */
    private static int mapCockroachDBTypeToJdbc(String dataType) {
        if (dataType == null) {
            return Types.OTHER;
        }
        switch (dataType.toUpperCase()) {
            case "BOOL":
            case "BOOLEAN":
                return Types.BOOLEAN;
            case "INT2":
            case "SMALLINT":
                return Types.SMALLINT;
            case "INT4":
            case "INTEGER":
            case "INT":
                return Types.INTEGER;
            case "INT8":
            case "BIGINT":
                return Types.BIGINT;
            case "FLOAT4":
            case "REAL":
                return Types.REAL;
            case "FLOAT8":
            case "DOUBLE PRECISION":
                return Types.DOUBLE;
            case "NUMERIC":
            case "DECIMAL":
                return Types.NUMERIC;
            case "VARCHAR":
            case "CHARACTER VARYING":
                return Types.VARCHAR;
            case "TEXT":
            case "STRING":
                return Types.VARCHAR;
            case "CHAR":
            case "CHARACTER":
                return Types.CHAR;
            case "BYTEA":
            case "BYTES":
                return Types.BINARY;
            case "DATE":
                return Types.DATE;
            case "TIME":
            case "TIME WITHOUT TIME ZONE":
                return Types.TIME;
            case "TIMETZ":
            case "TIME WITH TIME ZONE":
                return Types.TIME_WITH_TIMEZONE;
            case "TIMESTAMP":
            case "TIMESTAMP WITHOUT TIME ZONE":
                return Types.TIMESTAMP;
            case "TIMESTAMPTZ":
            case "TIMESTAMP WITH TIME ZONE":
                return Types.TIMESTAMP_WITH_TIMEZONE;
            case "INTERVAL":
                return Types.OTHER;
            case "UUID":
                return Types.OTHER;
            case "JSONB":
            case "JSON":
                return Types.OTHER;
            case "INET":
                return Types.OTHER;
            case "BIT":
            case "VARBIT":
            case "BIT VARYING":
                return Types.BIT;
            case "ARRAY":
                return Types.ARRAY;
            default:
                return Types.OTHER;
        }
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
