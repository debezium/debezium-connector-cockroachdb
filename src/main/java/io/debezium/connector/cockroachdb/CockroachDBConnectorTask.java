/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Kafka Connect SourceTask for CockroachDB.
 *
 * <p>Extends Debezium's {@link BaseSourceTask} to integrate with the standard
 * coordinator/queue/dispatcher pipeline. Consumes CockroachDB sink changefeeds
 * (via Kafka) and produces Debezium-formatted change events.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTask extends BaseSourceTask<CockroachDBPartition, CockroachDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnectorTask.class);
    private static final String CONTEXT_NAME = "cockroachdb-connector-task";

    private volatile CockroachDBTaskContext taskContext;
    private volatile CockroachDBSchema schema;
    private volatile CockroachDBErrorHandler errorHandler;
    private volatile ChangeEventQueue<DataChangeEvent> queue;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {
        final CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        taskContext = new CockroachDBTaskContext(config, connectorConfig);
        return taskContext;
    }

    @Override
    protected ChangeEventSourceCoordinator<CockroachDBPartition, CockroachDBOffsetContext> start(Configuration config) {
        final CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(
                CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final Clock clock = Clock.system();

        schema = new CockroachDBSchema(taskContext, topicNamingStrategy);
        try {
            schema.initialize(connectorConfig);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize CockroachDB schema", e);
        }

        final CockroachDBPartitionProvider partitionProvider = new CockroachDBPartitionProvider();
        final CockroachDBOffsetContext.Loader offsetLoader = new CockroachDBOffsetContext.Loader(connectorConfig);
        final Offsets<CockroachDBPartition, CockroachDBOffsetContext> previousOffsets = getPreviousOffsets(
                partitionProvider, offsetLoader);
        final CockroachDBOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        if (previousOffset == null) {
            LOGGER.info("No previous offset found");
        }
        else {
            LOGGER.info("Found previous offset {}", previousOffset);
        }

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .queueProvider(new DefaultQueueProvider<>(connectorConfig.getMaxQueueSize()))
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        final ErrorHandler previousErrorHandler = this.errorHandler;
        this.errorHandler = new CockroachDBErrorHandler(connectorConfig, queue, previousErrorHandler);

        final CockroachDBEventMetadataProvider metadataProvider = new CockroachDBEventMetadataProvider();

        final EventDispatcher<CockroachDBPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                new CockroachDBChangeEventCreator(),
                metadataProvider,
                schemaNameAdjuster,
                null,
                new DebeziumHeaderProducer(taskContext));

        final CockroachDBChangeEventSourceFactory changeEventSourceFactory = new CockroachDBChangeEventSourceFactory(
                connectorConfig, dispatcher, schema, clock);

        NotificationService<CockroachDBPartition, CockroachDBOffsetContext> notificationService = new NotificationService<>(
                getNotificationChannels(),
                connectorConfig,
                SchemaFactory.get(),
                dispatcher::enqueueNotification);

        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);

        try {
            ChangeEventSourceCoordinator<CockroachDBPartition, CockroachDBOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                    previousOffsets,
                    errorHandler,
                    CockroachDBConnector.class,
                    connectorConfig,
                    changeEventSourceFactory,
                    new DefaultChangeEventSourceMetricsFactory<>(),
                    dispatcher,
                    schema,
                    null,
                    notificationService,
                    null);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        return records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
        LOGGER.info("Stopping CockroachDB connector task");
        if (schema != null) {
            try {
                schema.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing schema", e);
            }
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return CockroachDBConnectorConfig.ALL_FIELDS;
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }
}
