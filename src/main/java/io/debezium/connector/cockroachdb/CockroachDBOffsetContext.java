/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Maintains offset state and metadata for CockroachDB changefeeds.
 * Tracks cursor and timestamp from changefeed progress messages.
 *
 * @author Virag Tripathi
 */
public class CockroachDBOffsetContext extends CommonOffsetContext<SourceInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBOffsetContext.class);

    public static final String CURSOR = "offset.cursor";
    public static final String TIMESTAMP = "offset.timestamp";
    public static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    public static final Field CURSOR_FIELD = Field.create(CURSOR);
    public static final Field TIMESTAMP_FIELD = Field.create(TIMESTAMP);

    private final Map<String, String> partition;
    private final CockroachDBConnectorConfig connectorConfig;
    private final SourceInfo sourceInfo;

    private final String logicalName;
    private String cursor;
    private Instant timestamp;
    private TransactionContext transactionContext;
    private Long kafkaOffset; // Track Kafka offset for hybrid approach

    public CockroachDBOffsetContext(CockroachDBConnectorConfig connectorConfig) {
        super(new SourceInfo(connectorConfig), false);
        this.connectorConfig = connectorConfig;
        this.logicalName = connectorConfig.getLogicalName();
        this.partition = Collections.singletonMap("server", logicalName);
        this.sourceInfo = new SourceInfo(connectorConfig);
        this.cursor = connectorConfig.getChangefeedCursor();
        this.timestamp = Instant.now();
        this.kafkaOffset = null;
    }

    public CockroachDBOffsetContext(CockroachDBConnectorConfig config, String cursor, Instant timestamp, Long kafkaOffset) {
        super(new SourceInfo(config), false);
        this.connectorConfig = config;
        this.logicalName = config.getLogicalName();
        this.partition = Collections.singletonMap("server", logicalName);
        this.sourceInfo = new SourceInfo(config);
        this.cursor = cursor;
        this.timestamp = timestamp;
        this.kafkaOffset = kafkaOffset;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
        sourceInfo.setSourceTime(timestamp);
    }

    public void setTransaction(TransactionContext context) {
        this.transactionContext = context;
    }

    public void endTransaction() {
        this.transactionContext = null;
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(CURSOR, cursor != null ? cursor : "initial");
        offset.put(TIMESTAMP, timestamp.toEpochMilli());
        offset.put(SNAPSHOT_COMPLETED_KEY, Boolean.toString(snapshotCompleted));
        return offset;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public void event(DataCollectionId collectionId, Instant ts) {
        setTimestamp(ts);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public String toString() {
        return "CockroachDBOffsetContext [cursor=" + cursor + ", ts=" + timestamp + "]";
    }

    public Instant getTimestamp() {
        return this.timestamp;
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        sourceInfo.setSnapshot(record);
    }

    public static class Loader implements OffsetContext.Loader<CockroachDBOffsetContext> {

        private final CockroachDBConnectorConfig connectorConfig;

        public Loader(CockroachDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public CockroachDBOffsetContext load(Map<String, ?> offset) {
            CockroachDBOffsetContext context = new CockroachDBOffsetContext(connectorConfig);

            String cursor = (String) offset.get(CURSOR);
            String tsStr = offset.get(TIMESTAMP) != null ? offset.get(TIMESTAMP).toString() : null;
            String snapshot = (String) offset.get(SNAPSHOT_COMPLETED_KEY);

            if (cursor != null) {
                context.setCursor(cursor);
            }
            if (tsStr != null) {
                context.setTimestamp(Instant.ofEpochMilli(Long.parseLong(tsStr)));
            }
            if (snapshot != null) {
                context.snapshotCompleted = Boolean.parseBoolean(snapshot);
            }

            return context;
        }
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public String getCursor() {
        return this.cursor;
    }

    public Long getKafkaOffset() {
        return kafkaOffset;
    }

    public void setKafkaOffset(Long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }
}
