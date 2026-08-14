/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Coordinates metadata from CockroachDB changefeed events
 * to populate the `source` block in Debezium records.
 *
 * @author Virag Tripathi
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    private final String databaseName;
    private final String clusterName;

    private Instant sourceTime = Instant.EPOCH;
    private String resolvedTimestamp;
    private String hlc;
    private Long tsNanos;
    private String schemaName;
    private String tableName;

    protected SourceInfo(CockroachDBConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.databaseName = connectorConfig.getDatabaseName();
        this.clusterName = connectorConfig.getLogicalName();
    }

    /**
     * Records the collection of the event being emitted so the source block carries its
     * schema and table, matching the PostgreSQL and SQL Server connectors.
     */
    public void tableEvent(TableId tableId) {
        if (tableId != null) {
            this.schemaName = tableId.schema();
            this.tableName = tableId.table();
        }
    }

    public String schemaName() {
        return schemaName;
    }

    public String tableName() {
        return tableName;
    }

    public void setSourceTime(Instant instant) {
        this.sourceTime = instant != null ? instant : Instant.EPOCH;
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        return databaseName;
    }

    public String cluster() {
        return clusterName;
    }

    public void setResolvedTimestamp(String resolvedTimestamp) {
        this.resolvedTimestamp = resolvedTimestamp;
    }

    public String resolvedTimestamp() {
        return resolvedTimestamp;
    }

    public void setHlc(String hlc) {
        this.hlc = hlc;
    }

    public String hlc() {
        return hlc;
    }

    public void setTsNanos(Long tsNanos) {
        this.tsNanos = tsNanos;
    }

    public Long tsNanos() {
        return tsNanos;
    }
}
