/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.connector.cockroachdb.serialization.ChangefeedSchemaParser;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Streaming change event source for CockroachDB using changefeeds with enriched envelope.
 *
 * This class is responsible for:
 * 1. Connecting to CockroachDB and creating changefeeds for monitored tables
 * 2. Processing changefeed events and converting them to Debezium SourceRecords
 * 3. Managing offset tracking and cursor updates
 * 4. Handling connection lifecycle and error recovery
 *
 * The enriched envelope provides additional metadata like:
 * - Updated column values
 * - Diff information showing what changed
 * - Source and schema information
 * - Resolved timestamps for consistency
 *
 * @author Virag Tripathi
 */
public class CockroachDBStreamingChangeEventSource implements StreamingChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);

    // Polling interval for checking if the context is still running
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    // Template for creating CockroachDB changefeeds with enriched envelope
    // The enriched envelope provides additional metadata beyond just the changed data
    private static final String CHANGEFEED_QUERY_TEMPLATE = "EXPERIMENTAL CHANGEFEED FOR TABLE %s WITH " +
            "envelope = 'enriched', " + // Use enriched envelope for additional metadata
            "enriched_properties = 'source,schema', " + // Include source and schema information
            "updated, " + // Include updated column values
            "diff, " + // Include diff information showing what changed
            "cursor = '%s'"; // Resume from this cursor position

    private final CockroachDBConnectorConfig config;
    private final EventDispatcher<CockroachDBPartition, TableId> dispatcher;
    private final CockroachDBSchema schema;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public CockroachDBStreamingChangeEventSource(
                                                 CockroachDBConnectorConfig config,
                                                 EventDispatcher<CockroachDBPartition, TableId> dispatcher,
                                                 CockroachDBSchema schema,
                                                 Clock clock) {
        this.config = config;
        this.dispatcher = dispatcher;
        this.schema = schema;
        this.clock = clock;
    }

    @Override
    public void execute(ChangeEventSourceContext context, CockroachDBPartition partition, CockroachDBOffsetContext offsetContext) throws InterruptedException {
        LOGGER.info("Starting CockroachDB streaming change event source");

        running.set(true);

        try (CockroachDBConnection connection = new CockroachDBConnection(config)) {
            // Establish connection to CockroachDB with retry logic
            connection.connect();
            LOGGER.info("Successfully connected to CockroachDB");

            // Get the list of tables to monitor from the schema
            List<TableId> tables = List.copyOf(schema.tableIds());
            if (tables.isEmpty()) {
                LOGGER.warn("No tables found to monitor - check your table filters and permissions");
                return;
            }

            LOGGER.info("Monitoring {} tables: {}", tables.size(), tables);

            // Create changefeed for each table to monitor
            // Each table gets its own changefeed to ensure we capture all changes
            for (TableId table : tables) {
                if (!context.isRunning()) {
                    LOGGER.info("Context stopped, breaking out of table processing loop");
                    break;
                }

                createChangefeedForTable(connection, table, offsetContext, context);
            }

            // Keep the connection alive and monitor for changes
            // This loop ensures we stay connected and can detect when the context stops
            Metronome metronome = Metronome.sleeper(POLL_INTERVAL, clock);
            while (context.isRunning() && running.get()) {
                metronome.pause();
            }

        }
        catch (SQLException e) {
            LOGGER.error("Error in CockroachDB streaming: ", e);
            throw new RuntimeException("Failed to stream changes from CockroachDB", e);
        }
        finally {
            running.set(false);
            LOGGER.info("Stopped CockroachDB streaming change event source");
        }
    }

    /**
     * Creates a changefeed for a specific table and processes its events.
     *
     * @param connection The database connection to use
     * @param table The table to monitor for changes
     * @param offsetContext The offset context for tracking position
     * @param context The change event source context for lifecycle management
     * @throws SQLException if database operations fail
     * @throws InterruptedException if the operation is interrupted
     */
    private void createChangefeedForTable(
                                          CockroachDBConnection connection,
                                          TableId table,
                                          CockroachDBOffsetContext offsetContext,
                                          ChangeEventSourceContext context)
            throws SQLException, InterruptedException {

        // Get the cursor position to resume from
        // If no cursor exists, start from "now" (current time)
        String cursor = offsetContext.getCursor();
        if (cursor == null) {
            cursor = "now"; // Start from current time if no cursor
            LOGGER.debug("No cursor found, starting changefeed from current time for table {}", table);
        }
        else {
            LOGGER.debug("Resuming changefeed from cursor {} for table {}", cursor, table);
        }

        // Build the changefeed query with the enriched envelope configuration
        String changefeedQuery = String.format(CHANGEFEED_QUERY_TEMPLATE, table, cursor);
        LOGGER.info("Creating changefeed for table {}: {}", table, changefeedQuery);

        try (Statement stmt = connection.connection().createStatement()) {
            // Set a reasonable fetch size to avoid memory issues with large result sets
            stmt.setFetchSize(1000);

            try (ResultSet rs = stmt.executeQuery(changefeedQuery)) {
                // Process each row from the changefeed result set
                while (context.isRunning() && running.get() && rs.next()) {
                    processChangefeedRow(rs, table, offsetContext);
                }
            }
        }
    }

    /**
     * Processes a single row from the changefeed result set.
     *
     * This method handles two types of messages:
     * 1. Resolved timestamp messages (for consistency tracking)
     * 2. Data change messages (actual table changes)
     *
     * @param rs The result set containing the changefeed row
     * @param table The table this change belongs to
     * @param offsetContext The offset context to update with new position
     * @throws SQLException if database operations fail
     */
    private void processChangefeedRow(ResultSet rs, TableId table, CockroachDBOffsetContext offsetContext) throws SQLException {
        // Extract the key, value, and resolved timestamp from the changefeed row
        String keyJson = rs.getString("key");
        String valueJson = rs.getString("value");
        String resolvedTs = rs.getString("resolved");

        // Check if this is a resolved timestamp message
        // Resolved timestamps are used for consistency tracking and don't contain actual data changes
        if (keyJson == null && valueJson == null && resolvedTs != null) {
            // This is a resolved timestamp message - update our cursor position
            offsetContext.setCursor(resolvedTs);
            LOGGER.debug("Updated cursor to: {} for table {}", resolvedTs, table);
            return;
        }

        // This is a data change message - parse and process it
        try {
            // Parse the enriched envelope changefeed message into a Debezium-compatible format
            ChangefeedSchemaParser.ParsedChange change = ChangefeedSchemaParser.parse(keyJson, valueJson);

            // Update the offset context with the current timestamp
            Instant timestamp = extractTimestamp(valueJson);
            if (timestamp != null) {
                offsetContext.setTimestamp(timestamp);
            }

            // Create a Kafka Connect SourceRecord from the parsed change
            SourceRecord record = createSourceRecord(table, change, offsetContext);
            if (record != null) {
                // For now, just log the record instead of dispatching
                // TODO: Implement proper ChangeRecordEmitter for full integration
                LOGGER.info("Would dispatch change event for table {}: {}", table, record);
            }

        }
        catch (Exception e) {
            LOGGER.error("Error processing changefeed row for table {}: {}", table, e.getMessage(), e);
        }
    }

    /**
     * Creates a Kafka Connect SourceRecord from a parsed changefeed change.
     *
     * @param table The table this change belongs to
     * @param change The parsed change event
     * @param offsetContext The offset context for position tracking
     * @return A SourceRecord ready for Kafka, or null if this should be skipped
     */
    private SourceRecord createSourceRecord(TableId table, ChangefeedSchemaParser.ParsedChange change, CockroachDBOffsetContext offsetContext) {
        // Skip resolved timestamp messages (they don't have key/value data)
        if (change.keySchema() == null && change.valueSchema() == null) {
            return null;
        }

        // Create the source partition for this record
        // The partition helps Kafka Connect organize records
        CockroachDBPartition partition = new CockroachDBPartition();

        // Get the current offset position for this record
        // This allows Kafka Connect to track where we are in the stream
        @SuppressWarnings("unchecked")
        Map<String, Object> sourceOffset = (Map<String, Object>) offsetContext.getOffset();

        // Create the topic name for this record
        // Use a simple naming strategy: {logicalName}.{schema}.{table}
        String topicName = config.getLogicalName() + "." + table.schema() + "." + table.table();

        // Create and return the SourceRecord
        // This is what Kafka Connect will send to Kafka
        return new SourceRecord(
                partition.getSourcePartition(), // Source partition
                sourceOffset, // Source offset
                topicName, // Topic name
                change.keySchema(), // Key schema
                change.key(), // Key data
                change.valueSchema(), // Value schema
                change.value() // Value data
        );
    }

    /**
     * Extracts a timestamp from a changefeed message.
     *
     * This is a simplified implementation. In a production version, you would:
     * 1. Parse the actual JSON to extract the precise timestamp
     * 2. Handle different timestamp formats from CockroachDB
     * 3. Consider timezone handling
     *
     * @param valueJson The JSON value from the changefeed
     * @return The extracted timestamp, or null if extraction fails
     */
    private Instant extractTimestamp(String valueJson) {
        try {
            // TODO: Parse the actual JSON to extract the precise timestamp from the enriched envelope
            // For now, return the current time as a placeholder
            return Instant.now();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to extract timestamp from changefeed message", e);
            return null;
        }
    }

    /**
     * Stops the streaming change event source.
     * This method is called when the connector is stopped or encounters an error.
     */
    public void stop() {
        running.set(false);
        LOGGER.info("Stop requested for CockroachDB streaming change event source");
    }
}
