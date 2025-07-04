/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionInfo;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Supplies event-level metadata for CockroachDB changefeed events.
 * Used for setting ts_ms, transaction_id, position, etc.
 *
 * @author Virag Tripathi
 */
public class CockroachDBEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (offset instanceof CockroachDBOffsetContext ctx) {
            return ctx.getTimestamp();
        }
        return null;
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (offset instanceof CockroachDBOffsetContext ctx) {
            return Map.of("cursor", ctx.getCursor());
        }
        return Map.of();
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // CockroachDB changefeeds don't expose per-tx metadata; stub empty
        return "";
    }

    @Override
    public TransactionInfo getTransactionInfo(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // Optional; CRDB does not provide tx boundaries per changefeed entry
        return null;
    }

    @Override
    public String toSummaryString(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        return String.format("CockroachDB change event on %s with key=%s", source, key);
    }
}
