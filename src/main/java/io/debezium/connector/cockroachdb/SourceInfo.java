/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;

/**
 * Coordinates metadata from CockroachDB changefeed events
 * to populate the `source` block in Debezium records.
 *
 * @author Virag Tripathi
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String DATABASE_NAME_KEY = "db";

    private final String databaseName;
    private final String clusterName;

    private Instant sourceTime = Instant.EPOCH;
    private String resolvedTimestamp;
    private String hlc;
    private Long tsNanos;

    protected SourceInfo(CockroachDBConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.databaseName = connectorConfig.getDatabaseName();
        this.clusterName = connectorConfig.getLogicalName();
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
