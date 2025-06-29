/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.List;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;

/**
 * Kafka Connect SourceTask implementation for CockroachDB changefeeds.
 * Basic implementation that can be extended with full functionality.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTask extends BaseSourceTask<CockroachDBPartition, CockroachDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnectorTask.class);
    private static final String CONTEXT_NAME = "cockroachdb-connector-task";

    private CockroachDBConnectorConfig connectorConfig;
    private CockroachDBTaskContext taskContext;
    private CockroachDBSchema schema;
    private CockroachDBErrorHandler errorHandler;

    @Override
    public ChangeEventSourceCoordinator<CockroachDBPartition, CockroachDBOffsetContext> start(Configuration config) {
        this.connectorConfig = new CockroachDBConnectorConfig(config);
        LOGGER.info("Starting CockroachDB connector task with config: {}", config);

        // Initialize error handler with null queue for now
        this.errorHandler = new CockroachDBErrorHandler(connectorConfig, null);

        // TODO: Initialize schema and task context
        // TODO: Set up changefeed streaming

        // Return null for now - this will be implemented properly later
        return null;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        // TODO: Implement changefeed polling
        LOGGER.debug("Polling for changes...");
        return List.of(); // Return empty list for now
    }

    @Override
    protected void doStop() {
        LOGGER.info("Stopping CockroachDB connector task");
        // TODO: Clean up resources
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return CockroachDBConnectorConfig.ALL_FIELDS;
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        // No-op — offset flushing handled by Kafka Connect
    }

    @Override
    public void commit() throws InterruptedException {
        // Optional: implement offset commit logic if needed
        LOGGER.debug("CockroachDBConnectorTask commit() called");
    }

    public CockroachDBErrorHandler getErrorHandler() {
        return errorHandler;
    }
}
