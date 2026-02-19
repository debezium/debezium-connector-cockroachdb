/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final SourceInfo sourceInfo;

    private String cursor;
    private Instant timestamp;
    private TransactionContext transactionContext;
    private Long kafkaOffset; // Track Kafka offset for hybrid approach

    public CockroachDBOffsetContext(CockroachDBConnectorConfig connectorConfig) {
        super(new SourceInfo(connectorConfig), false);
        this.sourceInfo = new SourceInfo(connectorConfig);
        this.cursor = connectorConfig.getChangefeedCursor();
        this.timestamp = Instant.now();
        this.kafkaOffset = null;
    }

    public CockroachDBOffsetContext(CockroachDBConnectorConfig config, String cursor, Instant timestamp, Long kafkaOffset) {
        super(new SourceInfo(config), false);
        this.sourceInfo = new SourceInfo(config);
        this.cursor = cursor;
        this.timestamp = timestamp;
        this.kafkaOffset = kafkaOffset;
    }

    public void setTimestamp(Instant timestamp) {
        Instant previous = this.timestamp;
        this.timestamp = timestamp != null ? timestamp : Instant.EPOCH;
        sourceInfo.setSourceTime(this.timestamp);
        LOGGER.debug("Offset timestamp updated: {} -> {}", previous, this.timestamp);
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
        Instant ts = timestamp != null ? timestamp : Instant.EPOCH;
        offset.put(TIMESTAMP, ts.toEpochMilli());
        offset.put(SNAPSHOT_COMPLETED_KEY, Boolean.toString(snapshotCompleted));
        LOGGER.trace("Returning offset: cursor='{}', timestamp={}, snapshotCompleted={}", cursor, ts, snapshotCompleted);
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
                try {
                    context.setTimestamp(Instant.ofEpochMilli(Long.parseLong(tsStr)));
                }
                catch (NumberFormatException e) {
                    LOGGER.warn("Invalid timestamp in stored offset: '{}', using epoch", tsStr);
                    context.setTimestamp(Instant.EPOCH);
                }
            }
            if (snapshot != null) {
                context.snapshotCompleted = Boolean.parseBoolean(snapshot);
            }

            LOGGER.debug("Loaded offset from storage: cursor='{}', timestamp={}, snapshotCompleted={}",
                    context.getCursor(), context.getTimestamp(), context.snapshotCompleted);
            return context;
        }
    }

    public void setCursor(String cursor) {
        String previous = this.cursor;
        this.cursor = cursor;
        LOGGER.debug("Offset cursor updated: '{}' -> '{}'", previous, cursor);
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
