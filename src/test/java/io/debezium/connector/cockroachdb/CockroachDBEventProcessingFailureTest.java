/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Tests what the streaming source does with a changefeed event that cannot be processed. Like the
 * other connectors, it follows {@code event.processing.failure.handling.mode}, whose default stops the
 * task instead of skipping the event.
 */
class CockroachDBEventProcessingFailureTest {

    private static final TableId ORDERS = new TableId("testdb", "public", "orders");
    private static final String KEY = "{\"id\":1}";
    private static final String INSERT = "{\"after\":{\"id\":1,\"name\":\"Alice\"},\"op\":\"c\","
            + "\"source\":{\"ts_hlc\":\"1700000000000000000.0000000000\"},\"ts_ns\":1700000000000000000}";
    private static final String NOT_JSON = "{\"after\":{\"id\":1,";

    @Test
    void failsTheTaskWhenAnEventCannotBeProcessed() {
        final EventDispatcher<CockroachDBPartition, TableId> dispatcher = dispatcher();
        final CockroachDBStreamingChangeEventSource source = source(null, dispatcher, schemaWithOrders());

        assertThatThrownBy(() -> source.processChangefeedEvent(KEY, NOT_JSON, ORDERS, mock(CockroachDBOffsetContext.class)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("testdb.public.orders")
                .satisfies(failure -> assertThat(failure.getSuppressed()).isNotEmpty());
    }

    @Test
    void doesNotRetryAnEventThatCannotBeProcessed() {
        // A JSON parsing error is an IOException, which the error handler takes for a communication problem
        // when it finds one in the cause chain. A restart would fail on the same event again.
        final CockroachDBStreamingChangeEventSource source = source(null, dispatcher(), schemaWithOrders());

        assertThatThrownBy(() -> source.processChangefeedEvent(KEY, NOT_JSON, ORDERS, mock(CockroachDBOffsetContext.class)))
                .satisfies(failure -> assertThat(errorHandler().isRetriable(new RuntimeException("Failed to consume from Kafka topics", failure)))
                        .isFalse());
    }

    @Test
    void skipsAnEventThatCannotBeProcessedWhenTheModeSaysSo() throws Exception {
        for (String mode : new String[]{ "warn", "skip" }) {
            final EventDispatcher<CockroachDBPartition, TableId> dispatcher = dispatcher();
            final CockroachDBStreamingChangeEventSource source = source(mode, dispatcher, schemaWithOrders());

            assertThatCode(() -> source.processChangefeedEvent(KEY, NOT_JSON, ORDERS, mock(CockroachDBOffsetContext.class)))
                    .as(mode).doesNotThrowAnyException();
            // The next event is processed.
            source.processChangefeedEvent(KEY, INSERT, ORDERS, mock(CockroachDBOffsetContext.class));
            verify(dispatcher, times(1)).dispatchDataChangeEvent(any(), any(), any());
        }
    }

    @Test
    void passesOnTheFailureOfTheDispatcher() throws Exception {
        // With event.processing.failure.handling.mode=fail the dispatcher answers an event that it cannot
        // process with this exception.
        final EventDispatcher<CockroachDBPartition, TableId> dispatcher = dispatcher();
        final ConnectException failure = new ConnectException("Error while processing event at offset {}");
        when(dispatcher.dispatchDataChangeEvent(any(), any(), any())).thenThrow(failure);
        final CockroachDBStreamingChangeEventSource source = source(null, dispatcher, schemaWithOrders());

        assertThatThrownBy(() -> source.processChangefeedEvent(KEY, INSERT, ORDERS, mock(CockroachDBOffsetContext.class)))
                .isSameAs(failure);
    }

    @Test
    void doesNotRememberAnEventThatFailed() throws Exception {
        // The event is read again after the restart. It must not look like a copy of itself then.
        final EventDispatcher<CockroachDBPartition, TableId> dispatcher = dispatcher();
        when(dispatcher.dispatchDataChangeEvent(any(), any(), any()))
                .thenThrow(new ConnectException("Error while processing event at offset {}"))
                .thenReturn(true);
        final CockroachDBStreamingChangeEventSource source = source(null, dispatcher, schemaWithOrders());

        assertThatThrownBy(() -> source.processChangefeedEvent(KEY, INSERT, ORDERS, mock(CockroachDBOffsetContext.class)))
                .isInstanceOf(ConnectException.class);
        source.processChangefeedEvent(KEY, INSERT, ORDERS, mock(CockroachDBOffsetContext.class));

        verify(dispatcher, times(2)).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    void passesOnAnInterruption() throws Exception {
        // Stopping the task interrupts the thread, for example while it waits for room in the queue. The
        // dispatcher reports that like an event that it cannot process: as its failure in the mode fail.
        final EventDispatcher<CockroachDBPartition, TableId> dispatcher = dispatcher();
        when(dispatcher.dispatchDataChangeEvent(any(), any(), any()))
                .thenThrow(new ConnectException("Error while processing event at offset {}", new InterruptedException()));
        final CockroachDBStreamingChangeEventSource source = source(null, dispatcher, schemaWithOrders());

        assertThatThrownBy(() -> source.processChangefeedEvent(KEY, INSERT, ORDERS, mock(CockroachDBOffsetContext.class)))
                .isInstanceOf(InterruptedException.class);
    }

    @Test
    void failsTheTaskWhenTheTableOfAnEventHasNoSchema() throws Exception {
        final EventDispatcher<CockroachDBPartition, TableId> dispatcher = dispatcher();
        final CockroachDBStreamingChangeEventSource source = source(null, dispatcher, mock(CockroachDBSchema.class));

        assertThatThrownBy(() -> source.processChangefeedEvent(KEY, INSERT, ORDERS, mock(CockroachDBOffsetContext.class)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("No schema found for table testdb.public.orders");
        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private static EventDispatcher<CockroachDBPartition, TableId> dispatcher() {
        return mock(EventDispatcher.class);
    }

    private static CockroachDBSchema schemaWithOrders() {
        final Table orders = Table.editor()
                .tableId(ORDERS)
                .addColumn(Column.editor().name("id").type("INT8").jdbcType(Types.BIGINT).optional(false).create())
                .addColumn(Column.editor().name("name").type("STRING").jdbcType(Types.VARCHAR).optional(true).create())
                .setPrimaryKeyNames("id")
                .create();
        final CockroachDBSchema schema = mock(CockroachDBSchema.class);
        when(schema.tableFor(ORDERS)).thenReturn(orders);
        return schema;
    }

    private static CockroachDBConnectorConfig config(String failureHandlingMode) {
        final Configuration.Builder config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                .with("database.hostname", "localhost")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("cockroachdb.changefeed.sink.type", "kafka")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        if (failureHandlingMode != null) {
            config.with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, failureHandlingMode);
        }
        return new CockroachDBConnectorConfig(config.build());
    }

    private static CockroachDBStreamingChangeEventSource source(String failureHandlingMode, EventDispatcher<CockroachDBPartition, TableId> dispatcher,
                                                                CockroachDBSchema schema) {
        return new CockroachDBStreamingChangeEventSource(config(failureHandlingMode), dispatcher, schema, Clock.system(), null);
    }

    private static CockroachDBErrorHandler errorHandler() {
        return new CockroachDBErrorHandler(config(null),
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .queueProvider(queueProvider())
                        .pollInterval(Duration.ofMillis(500))
                        .pollDispatchInterval(Duration.ofMillis(0))
                        .build(),
                null);
    }

    private static DefaultQueueProvider<DataChangeEvent> queueProvider() {
        final DefaultQueueProvider<DataChangeEvent> provider = new DefaultQueueProvider<>();
        provider.configure(Map.of("max.queue.size", String.valueOf(DEFAULT_MAX_QUEUE_SIZE)));
        return provider;
    }
}
