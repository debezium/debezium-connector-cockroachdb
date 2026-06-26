/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
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

    public static final String CURSOR_INITIAL = "initial";
    public static final String CURSOR_NOW = "now";

    /**
     * Prefix for the intermediate consumer position persisted in the Debezium source offset. This is
     * sink-agnostic: any delivery mode that consumes change events from an intermediate buffer (the
     * kafka mode today, pubsub or cloudstorage in future) stores its resume position here under an
     * opaque, sink-defined key. Persisting it in the source offset means Kafka Connect flushes it
     * atomically with the cursor, only after the corresponding records are produced, so a restart can
     * resume from the durable position instead of replaying the whole buffer. The sinkless mode does
     * not use this; it resumes via the changefeed cursor and has no intermediate consumer.
     */
    public static final String CONSUMER_OFFSET_PREFIX = "consumer.offset.";

    private final SourceInfo sourceInfo;

    private String cursor;
    private Instant timestamp;
    private TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private final Map<String, Long> consumerOffsets = new ConcurrentHashMap<>();

    public CockroachDBOffsetContext(CockroachDBConnectorConfig connectorConfig) {
        super(new SourceInfo(connectorConfig), false);
        this.sourceInfo = new SourceInfo(connectorConfig);
        this.cursor = connectorConfig.getChangefeedCursor();
        this.timestamp = Instant.now();
        this.incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>(false);
    }

    public CockroachDBOffsetContext(CockroachDBConnectorConfig config, String cursor, Instant timestamp,
                                    IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(config), false);
        this.sourceInfo = new SourceInfo(config);
        this.cursor = cursor;
        this.timestamp = timestamp;
        this.incrementalSnapshotContext = incrementalSnapshotContext != null
                ? incrementalSnapshotContext
                : new SignalBasedIncrementalSnapshotContext<>(false);
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
        offset.put(CURSOR, cursor != null ? cursor : CURSOR_INITIAL);
        Instant ts = timestamp != null ? timestamp : Instant.EPOCH;
        offset.put(TIMESTAMP, ts.toEpochMilli());
        offset.put(SNAPSHOT_COMPLETED_KEY, Boolean.toString(snapshotCompleted));
        // Persist the intermediate consumer position so a restart resumes instead of replaying the
        // whole intermediate buffer from the beginning.
        for (Map.Entry<String, Long> entry : consumerOffsets.entrySet()) {
            offset.put(CONSUMER_OFFSET_PREFIX + entry.getKey(), entry.getValue());
        }
        LOGGER.trace("Returning offset: cursor='{}', timestamp={}, snapshotCompleted={}, consumerOffsets={}",
                cursor, ts, snapshotCompleted, consumerOffsets);
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

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public static class Loader implements OffsetContext.Loader<CockroachDBOffsetContext> {

        private final CockroachDBConnectorConfig connectorConfig;

        public Loader(CockroachDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public CockroachDBOffsetContext load(Map<String, ?> offset) {
            String cursor = (String) offset.get(CURSOR);
            if (cursor == null) {
                cursor = connectorConfig.getChangefeedCursor();
            }
            String tsStr = offset.get(TIMESTAMP) != null ? offset.get(TIMESTAMP).toString() : null;
            String snapshot = (String) offset.get(SNAPSHOT_COMPLETED_KEY);

            Instant ts = Instant.EPOCH;
            if (tsStr != null) {
                try {
                    ts = Instant.ofEpochMilli(Long.parseLong(tsStr));
                }
                catch (NumberFormatException e) {
                    LOGGER.warn("Invalid timestamp in stored offset: '{}', using epoch", tsStr);
                }
            }

            CockroachDBOffsetContext context = new CockroachDBOffsetContext(
                    connectorConfig, cursor, ts,
                    SignalBasedIncrementalSnapshotContext.load(offset, false));

            if (snapshot != null) {
                context.snapshotCompleted = Boolean.parseBoolean(snapshot);
            }

            // Restore the intermediate consumer positions so the consume path can resume from them
            // on restart rather than replaying the buffer from the beginning.
            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                if (entry.getKey().startsWith(CONSUMER_OFFSET_PREFIX) && entry.getValue() != null) {
                    String key = entry.getKey().substring(CONSUMER_OFFSET_PREFIX.length());
                    try {
                        context.consumerOffsets.put(key, Long.parseLong(entry.getValue().toString()));
                    }
                    catch (NumberFormatException e) {
                        LOGGER.warn("Invalid stored consumer offset '{}' for '{}', ignoring", entry.getValue(), key);
                    }
                }
            }

            LOGGER.debug("Loaded offset from storage: cursor='{}', timestamp={}, snapshotCompleted={}, consumerOffsets={}",
                    context.getCursor(), context.getTimestamp(), context.snapshotCompleted, context.consumerOffsets);
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

    /**
     * Returns the last persisted intermediate consumer position for the given sink-defined key, or
     * {@code null} if none is stored (for example on first start). For the kafka mode the key is
     * {@code <topic>:<partition>}.
     */
    public Long getConsumerOffset(String key) {
        return consumerOffsets.get(key);
    }

    /**
     * Records the intermediate consumer position for the given sink-defined key. This is included in
     * {@link #getOffset()} and is flushed by Kafka Connect after the corresponding records are
     * produced, so it marks a durable resume point that survives a restart.
     */
    public void setConsumerOffset(String key, long offset) {
        consumerOffsets.put(key, offset);
    }
}
