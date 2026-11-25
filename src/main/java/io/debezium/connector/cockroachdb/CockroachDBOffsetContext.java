/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     * Prefix for the point where a new changefeed starts for a table whose changefeed is ahead of the
     * cursor of these offsets. That is the start of a changefeed that started later than the cursor:
     * the changefeed of a table that was created after the cursor, or on which the user got the
     * privilege after it, or one that began with a new initial scan because CockroachDB no longer had
     * the changes since the cursor. And it is how far a table had come that was ahead of the cursor
     * when the cursor moved back, or when the connector replaced a failed changefeed of other tables.
     * The key is the one of the consumer positions without the partition, so for the kafka mode the
     * intermediate topic of the table. The entry stays until the cursor has reached the point. Until
     * then a new changefeed for the table starts there, because CockroachDB can reject the cursor for
     * the table, and the cursor can be older than what the table needs.
     */
    public static final String CHANGEFEED_START_PREFIX = "changefeed.start.";

    /** Marks the stored start of a changefeed that began with an initial scan. */
    private static final String SCAN_SUFFIX = ";scan";

    /**
     * Where a changefeed for a table starts: the cursor, and whether the changefeed begins with an
     * initial scan as of that cursor.
     */
    public record ChangefeedStart(String cursor, boolean scans) {
    }

    private String cursor;
    private Instant timestamp;
    private TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private final Map<String, Long> consumerOffsets = new ConcurrentHashMap<>();
    private final Map<String, ChangefeedStart> changefeedStarts = new ConcurrentHashMap<>();

    public CockroachDBOffsetContext(CockroachDBConnectorConfig connectorConfig) {
        super(new SourceInfo(connectorConfig), false);
        this.cursor = connectorConfig.getChangefeedCursor();
        this.timestamp = Instant.now();
        this.incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>(false);
    }

    public CockroachDBOffsetContext(CockroachDBConnectorConfig config, String cursor, Instant timestamp,
                                    IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(config), false);
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
        for (Map.Entry<String, ChangefeedStart> entry : changefeedStarts.entrySet()) {
            final ChangefeedStart start = entry.getValue();
            offset.put(CHANGEFEED_START_PREFIX + entry.getKey(), start.scans() ? start.cursor() + SCAN_SUFFIX : start.cursor());
        }
        LOGGER.trace("Returning offset: cursor='{}', timestamp={}, snapshotCompleted={}, consumerOffsets={}, changefeedStarts={}",
                cursor, ts, snapshotCompleted, consumerOffsets, changefeedStarts);
        return offset;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public void event(DataCollectionId collectionId, Instant ts) {
        setTimestamp(ts);
        if (collectionId instanceof TableId) {
            sourceInfo.tableEvent((TableId) collectionId);
        }
    }

    /**
     * Sets the hybrid logical clock commit timestamp of the event being emitted, emitted as
     * {@code source.ts_hlc}. Pass {@code null} when the event carries none.
     */
    public void setHlc(String hlc) {
        sourceInfo.setHlc(hlc);
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

    /**
     * Returns {@code true} if the offset marks the initial snapshot as completed.
     */
    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
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

            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                if (entry.getKey().startsWith(CHANGEFEED_START_PREFIX) && entry.getValue() != null) {
                    final String key = entry.getKey().substring(CHANGEFEED_START_PREFIX.length());
                    final String value = entry.getValue().toString();
                    final boolean scans = value.endsWith(SCAN_SUFFIX);
                    final String start = scans ? value.substring(0, value.length() - SCAN_SUFFIX.length()) : value;
                    if (decimalTimestamp(start) == null) {
                        LOGGER.warn("Invalid stored changefeed start '{}' for '{}', ignoring", value, key);
                        continue;
                    }
                    context.changefeedStarts.put(key, new ChangefeedStart(start, scans));
                }
            }

            LOGGER.debug("Loaded offset from storage: cursor='{}', timestamp={}, snapshotCompleted={}, consumerOffsets={}, changefeedStarts={}",
                    context.getCursor(), context.getTimestamp(), context.snapshotCompleted, context.consumerOffsets, context.changefeedStarts);
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

    /**
     * Returns an immutable snapshot of the intermediate consumer positions keyed by their
     * sink-defined keys; empty when no position has been recorded (for example on first start,
     * or always in the sinkless mode which has no intermediate consumer).
     */
    public Map<String, Long> getConsumerOffsets() {
        return Map.copyOf(consumerOffsets);
    }

    /**
     * Returns where a new changefeed for the given key starts, or {@code null} if it can start at the
     * cursor (see {@link #CHANGEFEED_START_PREFIX}).
     */
    public ChangefeedStart getChangefeedStart(String key) {
        return changefeedStarts.get(key);
    }

    /**
     * Records that a new changefeed for the given key starts at the given cursor, which is later than
     * the cursor of these offsets. It is included in {@link #getOffset()}.
     */
    public void setChangefeedStart(String key, String cursor, boolean scans) {
        changefeedStarts.put(key, new ChangefeedStart(cursor, scans));
    }

    /** Returns an immutable snapshot of the recorded changefeed starts by their keys. */
    public Map<String, ChangefeedStart> getChangefeedStarts() {
        return Map.copyOf(changefeedStarts);
    }

    /**
     * Forgets the changefeed starts that the given cursor has reached. From such a cursor on, a
     * changefeed for the table can start at the cursor like the changefeeds of the other tables.
     */
    public void removeChangefeedStartsUpTo(String cursor) {
        if (changefeedStarts.isEmpty()) {
            return;
        }
        final BigDecimal reached = decimalTimestamp(cursor);
        if (reached == null) {
            return;
        }
        changefeedStarts.values().removeIf(start -> decimalTimestamp(start.cursor()).compareTo(reached) <= 0);
    }

    /** Parses a CockroachDB decimal timestamp. Returns {@code null} for anything else. */
    private static BigDecimal decimalTimestamp(String timestamp) {
        if (timestamp == null) {
            return null;
        }
        try {
            return new BigDecimal(timestamp.trim());
        }
        catch (NumberFormatException e) {
            return null;
        }
    }
}
