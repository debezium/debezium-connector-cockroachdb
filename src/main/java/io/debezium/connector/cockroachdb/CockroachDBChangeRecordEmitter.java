/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.cockroachdb.serialization.ChangefeedSchemaParser;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/**
 * Emits change records for CockroachDB changefeed events.
 *
 * This class implements the Debezium ChangeRecordEmitter interface to convert
 * CockroachDB changefeed events into Debezium-compatible change records that
 * can be processed by the Debezium pipeline.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeRecordEmitter implements ChangeRecordEmitter<CockroachDBPartition> {
    private final CockroachDBPartition partition;
    private final ChangefeedSchemaParser.ParsedChange change;
    private final CockroachDBOffsetContext offsetContext;
    private final Clock clock;
    private final Envelope.Operation operation;

    public CockroachDBChangeRecordEmitter(CockroachDBPartition partition, ChangefeedSchemaParser.ParsedChange change, CockroachDBOffsetContext offsetContext, Clock clock,
                                          Envelope.Operation operation) {
        this.partition = partition;
        this.change = change;
        this.offsetContext = offsetContext;
        this.clock = clock;
        this.operation = operation;
    }

    @Override
    public CockroachDBPartition getPartition() {
        return partition;
    }

    @Override
    public CockroachDBOffsetContext getOffset() {
        return offsetContext;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, ChangeRecordEmitter.Receiver<CockroachDBPartition> receiver) throws InterruptedException {
        receiver.changeRecord(partition, schema, operation, change.key(), (Struct) change.value(), offsetContext, null);
    }
}
