/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * Kafka Connect SourceTask for CockroachDB.
 *
 * This task implements the Debezium connector pattern for CockroachDB,
 * using native changefeeds to capture row-level changes and stream them
 * to Kafka topics in Debezium's enriched envelope format.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnectorTask.class);
    private static final String CONTEXT_NAME = "cockroachdb-connector-task";

    private volatile CockroachDBTaskContext taskContext;
    private volatile CockroachDBSchema schema;
    private volatile CockroachDBErrorHandler errorHandler;
    private volatile CockroachDBOffsetContext offsetContext;
    private volatile CockroachDBPartition partition;
    private volatile boolean running = false;
    private volatile List<SourceRecord> pendingRecords = new ArrayList<>();

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        final Configuration config = Configuration.from(props);
        final CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        // Log configuration in a readable multi-line format
        LOGGER.info("Starting CockroachDB connector task with configuration:");
        LOGGER.info("  Database Host: {}", connectorConfig.getHostname());
        LOGGER.info("  Database Port: {}", connectorConfig.getPort());
        LOGGER.info("  Database Name: {}", connectorConfig.getDatabaseName());
        LOGGER.info("  Database User: {}", connectorConfig.getUser());
        LOGGER.info("  Changefeed Envelope: {}", connectorConfig.getChangefeedEnvelope());
        LOGGER.info("  Changefeed Enriched Properties: {}", connectorConfig.getChangefeedEnrichedProperties());
        LOGGER.info("  Changefeed Include Updated: {}", connectorConfig.isChangefeedIncludeUpdated());
        LOGGER.info("  Changefeed Include Diff: {}", connectorConfig.isChangefeedIncludeDiff());
        LOGGER.info("  Changefeed Resolved Interval: {}", connectorConfig.getChangefeedResolvedInterval());
        LOGGER.info("  Changefeed Poll Interval: {}ms", connectorConfig.getChangefeedPollIntervalMs());
        LOGGER.info("  Connection Max Retries: {}", connectorConfig.getConnectionMaxRetries());
        LOGGER.info("  Connection Retry Delay: {}ms", connectorConfig.getConnectionRetryDelayMs());
        LOGGER.info("  Connection Timeout: {}ms", connectorConfig.getConnectionTimeoutMs());
        LOGGER.info("  SSL Mode: {}", connectorConfig.getSslMode());
        LOGGER.info("  TCP Keep Alive: {}", connectorConfig.isTcpKeepAlive());

        // Topic naming strategy and schema name adjuster
        final TopicNamingStrategy<TableId> topicNamingStrategy = new TopicNamingStrategy<TableId>() {
            @Override
            public String dataChangeTopic(TableId tableId) {
                return connectorConfig.getLogicalName() + "." + tableId.schema() + "." + tableId.table();
            }

            @Override
            public String schemaChangeTopic() {
                return connectorConfig.getLogicalName() + ".schema-changes";
            }

            @Override
            public String sanitizedTopicName(String topicName) {
                return topicName.replaceAll("[^a-zA-Z0-9._-]", "_");
            }

            @Override
            public String transactionTopic() {
                return connectorConfig.getLogicalName() + ".transaction";
            }

            @Override
            public String heartbeatTopic() {
                return connectorConfig.getLogicalName() + ".heartbeat";
            }

            @Override
            public void configure(Properties props) {
                // No configuration needed for this simple implementation
            }
        };
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        // Schema and task context
        this.schema = new CockroachDBSchema(connectorConfig, topicNamingStrategy);
        this.schema.initialize(connectorConfig);
        this.taskContext = new CockroachDBTaskContext(connectorConfig, schema, topicNamingStrategy);

        // Initialize partition and offset context
        this.partition = new CockroachDBPartition();
        this.offsetContext = new CockroachDBOffsetContext(connectorConfig);

        // Load offset from Kafka Connect
        Map<String, Object> offset = context.offsetStorageReader().offset(partition.getSourcePartition());
        if (offset != null) {
            this.offsetContext = new CockroachDBOffsetContext.Loader(connectorConfig).load(offset);
        }

        // Start streaming in background thread
        running = true;
        Thread streamingThread = new Thread(() -> {
            try {
                executeStreaming(connectorConfig);
            }
            catch (Exception e) {
                LOGGER.error("Error in streaming thread", e);
            }
        });
        streamingThread.setName("cockroachdb-streaming");
        streamingThread.setDaemon(true);
        streamingThread.start();

        LOGGER.info("CockroachDB connector task started successfully");
    }

    private void executeStreaming(CockroachDBConnectorConfig connectorConfig) throws InterruptedException {
        ChangeEventSourceContext changeEventSourceContext = new ChangeEventSourceContext() {
            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public boolean isPaused() {
                return false;
            }

            @Override
            public void waitSnapshotCompletion() throws InterruptedException {
                // No-op for now
            }

            @Override
            public void waitStreamingPaused() throws InterruptedException {
                // No-op for now
            }

            @Override
            public void streamingPaused() {
                // No-op for now
            }

            @Override
            public void resumeStreaming() {
                // No-op for now
            }
        };

        // Re-initialize topicNamingStrategy and schemaNameAdjuster here
        final TopicNamingStrategy<TableId> topicNamingStrategy = new TopicNamingStrategy<TableId>() {
            @Override
            public String dataChangeTopic(TableId tableId) {
                return connectorConfig.getLogicalName() + "." + tableId.schema() + "." + tableId.table();
            }

            @Override
            public String schemaChangeTopic() {
                return connectorConfig.getLogicalName() + ".schema-changes";
            }

            @Override
            public String sanitizedTopicName(String topicName) {
                return topicName.replaceAll("[^a-zA-Z0-9._-]", "_");
            }

            @Override
            public String transactionTopic() {
                return connectorConfig.getLogicalName() + ".transaction";
            }

            @Override
            public String heartbeatTopic() {
                return connectorConfig.getLogicalName() + ".heartbeat";
            }

            @Override
            public void configure(Properties props) {
                // No configuration needed for this simple implementation
            }
        };
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        // Properly initialize the EventDispatcher with correct constructor signature
        EventDispatcher<CockroachDBPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                null, // No queue for now
                connectorConfig.getTableFilters().dataCollectionFilter(),
                new CockroachDBChangeEventCreator(),
                new CockroachDBEventMetadataProvider(),
                schemaNameAdjuster,
                null, // SignalProcessor - not needed for basic streaming
                null // DebeziumHeaderProducer - not needed for basic streaming
        );

        StreamingChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> streamingChangeEventSource = new CockroachDBStreamingChangeEventSource(connectorConfig,
                dispatcher, schema, Clock.SYSTEM);

        streamingChangeEventSource.execute(changeEventSourceContext, partition, offsetContext);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (!running) {
            return List.of();
        }

        // For now, return any pending records and clear the list
        synchronized (pendingRecords) {
            if (pendingRecords.isEmpty()) {
                return List.of();
            }
            List<SourceRecord> records = new ArrayList<>(pendingRecords);
            pendingRecords.clear();
            return records;
        }
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping CockroachDB connector task");
        running = false;

        if (schema != null) {
            schema.close();
        }
    }
}
