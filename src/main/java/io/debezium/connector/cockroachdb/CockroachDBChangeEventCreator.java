/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.ChangeEventCreator;

/**
 * Stubbed ChangeEventCreator for CockroachDB.
 * Responsible for creating DataChangeEvent(s) from ChangeRecordEmitters.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeEventCreator implements ChangeEventCreator {

    @Override
    public DataChangeEvent createDataChangeEvent(SourceRecord sourceRecord) {
        return new DataChangeEvent(sourceRecord);
    }
}
