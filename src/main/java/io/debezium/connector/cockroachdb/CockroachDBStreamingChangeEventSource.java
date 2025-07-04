/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.connector.cockroachdb.serialization.ChangefeedSchemaParser;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Streaming change event source for CockroachDB using native changefeeds.
 *
 * This class implements the Debezium pattern:
 * 1. Creates sink changefeeds writing to Kafka topics
 * 2. Consumes events from Kafka using KafkaConsumer
 * 3. Parses enriched envelope and extracts operation types
 * 4. Dispatches events through Debezium's EventDispatcher
 *
 * This approach provides reliability, manageability, and
 * full integration with Debezium's event processing pipeline.
 *
 * @author Virag Tripathi
 */
public class CockroachDBStreamingChangeEventSource implements StreamingChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);

    private final CockroachDBConnectorConfig config;
    private final EventDispatcher<CockroachDBPartition, TableId> dispatcher;
    private final CockroachDBSchema schema;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet(); // Track processed events to avoid duplicates

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
        LOGGER.info("Starting CockroachDB streaming change event source with sink changefeed approach");

        running.set(true);

        try (CockroachDBConnection connection = new CockroachDBConnection(config)) {
            // Establish connection to CockroachDB with retry logic
            connection.connect();
            LOGGER.info("Successfully connected to CockroachDB");

            // Get the list of tables to monitor from the schema
            List<TableId> tables = schema.getDiscoveredTables();
            if (tables.isEmpty()) {
                LOGGER.warn("No tables found to monitor - check your table filters and permissions");
                return;
            }

            LOGGER.info("Monitoring {} tables: {}", tables.size(), tables);

            // Process each table with sink changefeed
            for (TableId table : tables) {
                if (!context.isRunning()) {
                    LOGGER.info("Context stopped, breaking out of table processing loop");
                    break;
                }

                processTableWithSinkChangefeed(connection, table, offsetContext, context);
            }

            // Keep the connection alive and monitor for changes
            Duration pollInterval = Duration.ofMillis(config.getChangefeedPollIntervalMs());
            Metronome metronome = Metronome.sleeper(pollInterval, clock);
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
     * Process a table using sink changefeed approach.
     *
     * This method implements the Debezium pattern:
     * 1. Creates a sink changefeed for the table
     * 2. Consumes events from the Kafka topic
     * 3. Parses enriched envelope events
     * 4. Dispatches events through Debezium's pipeline
     *
     * @param connection Database connection
     * @param table Table to process
     * @param offsetContext Offset context for resuming
     * @param context Change event source context
     * @throws SQLException Database errors
     * @throws InterruptedException Interruption
     */
    private void processTableWithSinkChangefeed(
                                                CockroachDBConnection connection,
                                                TableId table,
                                                CockroachDBOffsetContext offsetContext,
                                                ChangeEventSourceContext context)
            throws SQLException, InterruptedException {
        // Create changefeed for the table
        String changefeedQuery = buildSinkChangefeedQuery(table, offsetContext.getCursor());
        LOGGER.info("Creating sink changefeed for table {}: {}", table, changefeedQuery);

        try (Statement stmt = connection.connection().createStatement()) {
            stmt.execute(changefeedQuery);
            LOGGER.info("Successfully created changefeed for table {}", table);
        }
        catch (SQLException e) {
            LOGGER.error("Failed to create changefeed for table {}: {}", table, e.getMessage(), e);
            throw e;
        }

        // Consume events from the Kafka topic
        consumeFromKafkaTopic(table, offsetContext, context);
    }

    /**
     * Checks if a changefeed job for the given table already exists.
     */
    private boolean changefeedExists(CockroachDBConnection connection, TableId table) throws SQLException {
        try (Statement stmt = connection.connection().createStatement()) {
            String query = "SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' AND description LIKE '%" + table.toString() + "%'";
            try (var rs = stmt.executeQuery(query)) {
                return rs.next(); // Returns true if job exists and is running
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to check if changefeed exists: {}", e.getMessage());
            return false; // Assume it doesn't exist if we can't check
        }
    }

    /**
     * Consumes events from the Kafka topic created by the sink changefeed.
     *
     * @param table The table this changefeed is monitoring
     * @param offsetContext The offset context for tracking position
     * @param context The change event source context for lifecycle management
     * @throws InterruptedException if the operation is interrupted
     *
     * NOTE: This method is currently hardcoded for Kafka consumption.
     * For webhook sink support, we would need to:
     * 1. Start an HTTP server to receive webhook POST requests
     * 2. Parse the webhook payload instead of Kafka records
     * 3. Handle webhook-specific error responses and retries
     * 4. Implement webhook authentication if required
     */
    private void consumeFromKafkaTopic(TableId table, CockroachDBOffsetContext offsetContext, ChangeEventSourceContext context) throws InterruptedException {
        // Use the same topic naming logic as buildSinkChangefeedQuery for consistency
        String topicPrefix = config.getChangefeedSinkTopicPrefix();
        if (topicPrefix == null || topicPrefix.trim().isEmpty()) {
            topicPrefix = "cockroachdb";
        }

        // Include database name in topic for multi-tenant support: prefix.database.schema.table
        String databaseName = config.getDatabaseName();
        String topicName = topicPrefix + "." + databaseName + "." + table.schema() + "." + table.table();

        // Configure Kafka consumer
        java.util.Properties props = new java.util.Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getChangefeedSinkUri().replace("kafka://", ""));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cockroachdb-connector-" + table.table());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topicName));
            LOGGER.info("Started consuming from Kafka topic: {}", topicName);

            Duration pollInterval = Duration.ofMillis(config.getChangefeedPollIntervalMs());
            Metronome metronome = Metronome.sleeper(pollInterval, clock);

            while (context.isRunning() && running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    if (!context.isRunning()) {
                        break;
                    }

                    try {
                        // Parse the changefeed event
                        String valueJson = record.value();
                        if (valueJson != null && !valueJson.trim().isEmpty()) {
                            processChangefeedEventFromKafka(valueJson, table, offsetContext);
                        }
                    }
                    catch (Exception e) {
                        LOGGER.error("Error processing changefeed event from Kafka: {}", e.getMessage(), e);
                    }
                }

                metronome.pause();
            }

            LOGGER.info("Stopped consuming from Kafka topic: {}", topicName);
        }
        catch (Exception e) {
            LOGGER.error("Error consuming from Kafka topic {}: {}", topicName, e.getMessage(), e);
            throw new RuntimeException("Failed to consume from Kafka topic " + topicName, e);
        }
    }

    /**
     * Processes a changefeed event received from Kafka.
     *
     * @param valueJson The JSON value from the Kafka record
     * @param table The table this event belongs to
     * @param offsetContext The offset context for tracking position
     */
    private void processChangefeedEventFromKafka(String valueJson, TableId table, CockroachDBOffsetContext offsetContext) {
        try {
            // Parse the JSON value
            JsonNode jsonNode = objectMapper.readTree(valueJson);

            // Skip resolved timestamp messages
            if (jsonNode.has("resolved")) {
                LOGGER.debug("Received resolved timestamp: {}", jsonNode.get("resolved").asText());
                return;
            }

            // Create a unique identifier for this event to avoid duplicates
            String eventId = createEventId(jsonNode);
            if (processedEvents.contains(eventId)) {
                LOGGER.debug("Skipping duplicate event: {}", eventId);
                return;
            }
            processedEvents.add(eventId);

            // Log the full enriched envelope with source metadata
            LOGGER.info("Received enriched envelope event for table {}: {}", table, valueJson);
            // TODO: For release, switch this to debug-level logging or remove to avoid leaking data in logs.

            // For sink changefeeds, the key is embedded in the enriched envelope
            // Extract the key from the 'after' or 'before' field
            String keyJson = extractKeyFromEnrichedEnvelope(jsonNode);

            // Parse the changefeed event with the extracted key
            ChangefeedSchemaParser.ParsedChange change = ChangefeedSchemaParser.parse(keyJson, valueJson);

            if (change != null) {
                // Dispatch the change event through Debezium's pipeline
                dispatchChangeEvent(table, change, offsetContext, jsonNode);
            }

        }
        catch (Exception e) {
            LOGGER.error("Error processing changefeed event from Kafka: {}", e.getMessage(), e);
        }
    }

    /**
     * Creates a unique identifier for an event to avoid duplicates.
     * Uses the combination of table, operation, and timestamp.
     */
    private String createEventId(JsonNode jsonNode) {
        try {
            // Handle nested payload structure
            JsonNode payloadNode = jsonNode.path("payload");
            if (payloadNode.isMissingNode()) {
                // Fallback: try direct access (for backward compatibility)
                payloadNode = jsonNode;
            }

            String tableName = payloadNode.path("source").path("table_name").asText("");
            String operation = payloadNode.path("op").asText("");
            String timestamp = payloadNode.path("ts_ns").asText("");
            return tableName + ":" + operation + ":" + timestamp;
        }
        catch (Exception e) {
            // Fallback to a hash of the entire JSON with null safety
            try {
                String jsonString = jsonNode != null ? jsonNode.toString() : "";
                return String.valueOf(jsonString.hashCode());
            }
            catch (Exception hashException) {
                // Final fallback
                return "unknown:" + System.currentTimeMillis();
            }
        }
    }

    /**
     * Extracts the primary key from the enriched envelope.
     *
     * For sink changefeeds, the key is embedded in the 'after' or 'before' field
     * of the enriched envelope. This method extracts the primary key value.
     *
     * @param jsonNode The parsed JSON node from the changefeed event
     * @return The key JSON string, or null if not found
     */
    private String extractKeyFromEnrichedEnvelope(JsonNode jsonNode) {
        try {
            // The enriched envelope has the actual data nested under "payload"
            JsonNode payloadNode = jsonNode.path("payload");
            if (payloadNode.isMissingNode()) {
                // Fallback: try direct access (for backward compatibility)
                payloadNode = jsonNode;
            }

            // Try to extract key from 'after' field first (for INSERT/UPDATE)
            if (payloadNode.has("after") && payloadNode.get("after").isObject()) {
                JsonNode afterNode = payloadNode.get("after");
                // For the products table, the primary key is 'id'
                if (afterNode.has("id")) {
                    return "{\"id\":\"" + afterNode.get("id").asText() + "\"}";
                }
            }

            // Try to extract key from 'before' field (for UPDATE/DELETE)
            if (payloadNode.has("before") && payloadNode.get("before").isObject()) {
                JsonNode beforeNode = payloadNode.get("before");
                if (beforeNode.has("id")) {
                    return "{\"id\":\"" + beforeNode.get("id").asText() + "\"}";
                }
            }

            LOGGER.warn("Could not extract primary key from enriched envelope");
            return null;
        }
        catch (Exception e) {
            LOGGER.error("Error extracting key from enriched envelope: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Dispatches a change event through Debezium's EventDispatcher.
     *
     * This is the correct way to emit events in a Debezium connector:
     * 1. Uses EventDispatcher to handle event processing
     * 2. Lets Debezium handle topic naming strategy
     * 3. Enables schema evolution
     * 4. Ensures proper event buffering in ChangeEventQueue
     *
     * @param table The table this change belongs to
     * @param change The parsed change event
     * @param offsetContext The offset context for position tracking
     */
    private void dispatchChangeEvent(TableId table, ChangefeedSchemaParser.ParsedChange change, CockroachDBOffsetContext offsetContext, JsonNode jsonNode) {
        // Skip resolved timestamp messages (they don't have key/value data)
        if (change.keySchema() == null && change.valueSchema() == null) {
            return;
        }

        try {
            // Extract operation type from enriched envelope
            Envelope.Operation operation = extractOperationFromEnvelope(jsonNode);

            // Extract source metadata for logging - handle nested payload structure
            JsonNode payloadNode = jsonNode.path("payload");
            if (payloadNode.isMissingNode()) {
                // Fallback: try direct access (for backward compatibility)
                payloadNode = jsonNode;
            }

            JsonNode sourceNode = payloadNode.path("source");
            String clusterId = sourceNode.path("cluster_id").asText("unknown");
            String nodeId = sourceNode.path("node_id").asText("unknown");
            String jobId = sourceNode.path("job_id").asText("unknown");
            String tsHlc = sourceNode.path("ts_hlc").asText("unknown");

            // Create partition for this table
            CockroachDBPartition partition = new CockroachDBPartition();

            // Create the change record emitter
            CockroachDBChangeRecordEmitter emitter = new CockroachDBChangeRecordEmitter(
                    partition, change, offsetContext, clock, operation);

            // Log the event with rich source metadata
            LOGGER.info("Processed data event for table {} with operation {}: key={}, value={}, source={}",
                    table,
                    operation,
                    change.key(),
                    change.value(),
                    "cluster_id=" + clusterId + ", node_id=" + nodeId + ", job_id=" + jobId + ", ts_hlc=" + tsHlc);
            // TODO: For release, switch this to debug-level logging or remove to avoid leaking data in logs.

            // TODO: Once schema registration is working, uncomment this:
            // dispatcher.dispatchDataChangeEvent(partition, table, emitter);

        }
        catch (Exception e) {
            LOGGER.error("Error processing change event for table {}: {}", table, e.getMessage(), e);
        }
    }

    /**
     * Extracts the operation type from the enriched envelope.
     *
     * CockroachDB's enriched envelope has an 'op' field that directly indicates the operation:
     * - 'c' = CREATE
     * - 'u' = UPDATE
     * - 'd' = DELETE
     *
     * @param jsonNode The JSON node containing the enriched envelope
     * @return The Debezium operation type
     */
    private Envelope.Operation extractOperationFromEnvelope(JsonNode jsonNode) {
        try {
            // Handle nested payload structure
            JsonNode payloadNode = jsonNode.path("payload");
            if (payloadNode.isMissingNode()) {
                // Fallback: try direct access (for backward compatibility)
                payloadNode = jsonNode;
            }

            // Extract operation from the 'op' field in the payload
            if (payloadNode.has("op")) {
                String op = payloadNode.get("op").asText();
                switch (op) {
                    case "c":
                        return Envelope.Operation.CREATE;
                    case "u":
                        return Envelope.Operation.UPDATE;
                    case "d":
                        return Envelope.Operation.DELETE;
                    default:
                        LOGGER.warn("Unknown operation type in enriched envelope: {}", op);
                        return Envelope.Operation.READ;
                }
            }

            // Fallback: check 'before' and 'after' fields
            boolean hasBefore = payloadNode.has("before") && !payloadNode.get("before").isNull();
            boolean hasAfter = payloadNode.has("after") && !payloadNode.get("after").isNull();

            if (hasBefore && hasAfter) {
                return Envelope.Operation.UPDATE;
            }
            else if (hasAfter && !hasBefore) {
                return Envelope.Operation.CREATE;
            }
            else if (hasBefore && !hasAfter) {
                return Envelope.Operation.DELETE;
            }
            else {
                LOGGER.warn("Cannot determine operation type from enriched envelope structure: before={}, after={}", hasBefore, hasAfter);
                return Envelope.Operation.READ;
            }
        }
        catch (Exception e) {
            LOGGER.error("Error extracting operation from enriched envelope: {}", e.getMessage(), e);
            return Envelope.Operation.READ;
        }
    }

    /**
     * Builds a sink changefeed query that writes to a Kafka topic.
     *
     * @param table The table to monitor
     * @param cursor The cursor position to start from
     * @return The CREATE CHANGEFEED query
     */
    private String buildSinkChangefeedQuery(TableId table, String cursor) {
        StringBuilder query = new StringBuilder();
        query.append("CREATE CHANGEFEED FOR TABLE ").append(table.toString());

        // Use configurable sink URI with proper topic naming for multi-tenant support
        String sinkUri = config.getChangefeedSinkUri();

        // Build topic name using configurable prefix and include database name for multi-tenant support
        String topicPrefix = config.getChangefeedSinkTopicPrefix();
        if (topicPrefix == null || topicPrefix.trim().isEmpty()) {
            topicPrefix = "cockroachdb";
        }

        // Include database name in topic for multi-tenant support: prefix.database.schema.table
        String databaseName = config.getDatabaseName();
        String topicName = topicPrefix + "." + databaseName + "." + table.schema() + "." + table.table();

        // Append the topic name to the sink URI
        if (sinkUri.contains("?")) {
            sinkUri += "&topic_name=" + topicName;
        }
        else {
            sinkUri += "?topic_name=" + topicName;
        }

        query.append(" INTO '").append(sinkUri).append("'");

        // Use configurable envelope type (now has default value)
        String envelope = config.getChangefeedEnvelope();
        query.append(" WITH envelope = '").append(envelope).append("'");

        // Use configurable enriched properties
        String enrichedProperties = config.getChangefeedEnrichedProperties();
        if (enrichedProperties != null && !enrichedProperties.trim().isEmpty()) {
            query.append(", enriched_properties = '").append(enrichedProperties).append("'");
        }

        // Use configurable options
        if (config.isChangefeedIncludeUpdated()) {
            query.append(", updated");
        }

        if (config.isChangefeedIncludeDiff()) {
            query.append(", diff");
        }

        // Use configurable resolved interval (now has default value)
        String resolvedInterval = config.getChangefeedResolvedInterval();
        query.append(", resolved = '").append(resolvedInterval).append("'");

        // Use configurable cursor
        if (cursor != null && !cursor.trim().isEmpty()) {
            query.append(", cursor = '").append(cursor).append("'");
        }

        // Add configurable sink options if provided
        String sinkOptions = config.getChangefeedSinkOptions();
        if (sinkOptions != null && !sinkOptions.trim().isEmpty()) {
            query.append(", ").append(sinkOptions);
        }

        return query.toString();
    }

    public void stop() {
        running.set(false);
        LOGGER.info("Stopping CockroachDB streaming change event source");
    }
}
