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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Streaming change event source for CockroachDB using native sink changefeeds.
 *
 * <p>Creates sink changefeeds that write to an external system (Kafka by default),
 * consumes events from that system, parses the enriched envelope, and dispatches
 * events through Debezium's {@link EventDispatcher} pipeline. Other sink types
 * (webhook, pubsub, cloud storage) can be added by implementing a new
 * {@code consumeFrom*} method and registering it in {@link #consumeChangefeedEvents}.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBStreamingChangeEventSource implements StreamingChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);

    private static final int MAX_DEDUP_CACHE_SIZE = 10_000;

    private final CockroachDBConnectorConfig config;
    private final EventDispatcher<CockroachDBPartition, TableId> dispatcher;
    private final CockroachDBSchema schema;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Bounded LRU cache for deduplication. Evicts oldest entries when the cache
     * exceeds {@link #MAX_DEDUP_CACHE_SIZE} to prevent unbounded memory growth.
     */
    private final Map<String, Boolean> processedEvents = Collections.synchronizedMap(
            new LinkedHashMap<>(MAX_DEDUP_CACHE_SIZE, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > MAX_DEDUP_CACHE_SIZE;
                }
            });

    /**
     * Tracks whether the changefeed is still performing an initial scan.
     * Set to true when a changefeed is created with {@code initial_scan='yes'}
     * and cleared when the first resolved timestamp is received, which signals
     * that the initial scan phase has completed.
     */
    private volatile boolean initialScanInProgress = false;

    public CockroachDBStreamingChangeEventSource(
                                                 CockroachDBConnectorConfig config,
                                                 EventDispatcher<CockroachDBPartition, TableId> dispatcher,
                                                 CockroachDBSchema schema,
                                                 Clock clock) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void execute(ChangeEventSourceContext context, CockroachDBPartition partition,
                        CockroachDBOffsetContext offsetContext)
            throws InterruptedException {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(partition, "partition must not be null");
        Objects.requireNonNull(offsetContext, "offsetContext must not be null");

        LOGGER.info("Starting CockroachDB streaming from cursor '{}', sink type '{}'",
                offsetContext.getCursor(), config.getChangefeedSinkType());
        running.set(true);

        try (CockroachDBConnection connection = new CockroachDBConnection(config)) {
            connection.connect();
            LOGGER.debug("Connected to CockroachDB at {}:{}, database={}",
                    config.getHostname(), config.getPort(), config.getDatabaseName());

            List<TableId> tables = schema.getDiscoveredTables();
            if (tables.isEmpty()) {
                LOGGER.warn("No tables found to monitor -- check table filters and permissions");
                return;
            }

            LOGGER.info("Monitoring {} table(s): {}", tables.size(), tables);

            String cursor = offsetContext.getCursor();
            boolean hasPriorOffset = cursor != null && !cursor.isEmpty()
                    && !"initial".equals(cursor) && !"now".equals(cursor);
            LOGGER.info("Snapshot mode: {}, hasPriorOffset: {}, cursor: '{}'",
                    config.getSnapshotMode().getValue(), hasPriorOffset, cursor);

            for (int i = 0; i < tables.size(); i++) {
                TableId table = tables.get(i);
                if (!context.isRunning()) {
                    LOGGER.debug("Context stopped, exiting table loop");
                    break;
                }
                LOGGER.debug("Processing table {}/{}: {}", i + 1, tables.size(), table);
                processTableWithSinkChangefeed(connection, table, offsetContext, context, hasPriorOffset);
            }

            Duration pollInterval = Duration.ofMillis(config.getChangefeedPollIntervalMs());
            Metronome metronome = Metronome.sleeper(pollInterval, clock);
            while (context.isRunning() && running.get()) {
                metronome.pause();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Error in CockroachDB streaming", e);
            throw new RuntimeException("Failed to stream changes from CockroachDB", e);
        }
        finally {
            running.set(false);
            LOGGER.info("Stopped CockroachDB streaming change event source");
        }
    }

    /**
     * Creates a sink changefeed for the given table (if one is not already running)
     * and starts consuming events from the corresponding sink.
     */
    private void processTableWithSinkChangefeed(
                                                CockroachDBConnection connection,
                                                TableId table,
                                                CockroachDBOffsetContext offsetContext,
                                                ChangeEventSourceContext context,
                                                boolean hasPriorOffset)
            throws SQLException, InterruptedException {

        LOGGER.debug("Checking for existing changefeed job for table {}", table);
        if (changefeedExists(connection, table)) {
            LOGGER.info("Changefeed already running for table {}, skipping creation", table);
        }
        else {
            String changefeedQuery = buildSinkChangefeedQuery(table, offsetContext.getCursor(), hasPriorOffset);
            LOGGER.debug("Built changefeed query for table {}: {}", table, changefeedQuery);

            try (Statement stmt = connection.connection().createStatement()) {
                stmt.execute(changefeedQuery);
                LOGGER.info("Created changefeed for table {}", table);
            }

            String initialScan = config.getInitialScanForSnapshotMode(hasPriorOffset);
            initialScanInProgress = "yes".equals(initialScan);
            if (initialScanInProgress) {
                LOGGER.info("Initial scan in progress for table {} (snapshot.mode={})",
                        table, config.getSnapshotMode().getValue());
            }
        }

        consumeChangefeedEvents(table, offsetContext, context);
    }

    /**
     * Routes event consumption to the appropriate sink-specific implementation.
     * Add new sink types here as they are implemented.
     */
    private void consumeChangefeedEvents(TableId table, CockroachDBOffsetContext offsetContext,
                                         ChangeEventSourceContext context)
            throws InterruptedException {
        String sinkType = config.getChangefeedSinkType();
        LOGGER.debug("Routing to sink consumer type='{}' for table {}", sinkType, table);
        switch (sinkType) {
            case "kafka":
                consumeFromKafkaTopic(table, offsetContext, context);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported changefeed sink type: '" + sinkType
                                + "'. Currently supported: kafka. Planned: webhook, pubsub, cloudstorage.");
        }
    }

    /**
     * Checks whether a running changefeed job already exists for the given table.
     * Uses column index rather than column name for compatibility across CockroachDB versions.
     */
    private boolean changefeedExists(CockroachDBConnection connection, TableId table) throws SQLException {
        String tableName = sanitizeIdentifier(table.table());
        try (Statement stmt = connection.connection().createStatement()) {
            String query = "SELECT description FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'";
            try (var rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    String description = rs.getString(1);
                    if (description != null && description.contains(tableName)) {
                        return true;
                    }
                }
                return false;
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Unable to check existing changefeed jobs: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Consumes changefeed events from a Kafka topic created by the sink changefeed.
     */
    private void consumeFromKafkaTopic(TableId table, CockroachDBOffsetContext offsetContext,
                                       ChangeEventSourceContext context)
            throws InterruptedException {
        String topicName = buildTopicName(table);

        java.util.Properties props = new java.util.Properties();
        String sinkUri = config.getChangefeedSinkUri();
        if (sinkUri != null) {
            String bootstrapServers = sinkUri.replaceFirst("^kafka://", "");
            bootstrapServers = bootstrapServers.replaceFirst("^PLAINTEXT://", "");
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                config.getChangefeedKafkaConsumerGroupPrefix() + "-" + table.table());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getChangefeedKafkaAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        String groupId = config.getChangefeedKafkaConsumerGroupPrefix() + "-" + table.table();
        LOGGER.debug("Kafka consumer config: bootstrapServers={}, groupId={}, autoOffsetReset={}, pollTimeout={}ms",
                props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), groupId,
                config.getChangefeedKafkaAutoOffsetReset(), config.getChangefeedKafkaPollTimeoutMs());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topicName));
            LOGGER.info("Consuming from Kafka topic: {}", topicName);

            Duration pollInterval = Duration.ofMillis(config.getChangefeedPollIntervalMs());
            Metronome metronome = Metronome.sleeper(pollInterval, clock);
            int emptyPollCount = 0;

            while (context.isRunning() && running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(config.getChangefeedKafkaPollTimeoutMs()));

                if (records.isEmpty()) {
                    emptyPollCount++;
                    if (emptyPollCount % 100 == 0) {
                        LOGGER.debug("No events received from topic {} for {} consecutive polls", topicName, emptyPollCount);
                    }
                }
                else {
                    emptyPollCount = 0;
                    LOGGER.trace("Polled {} records from Kafka topic {}", records.count(), topicName);
                }

                for (ConsumerRecord<String, String> record : records) {
                    if (!context.isRunning()) {
                        break;
                    }
                    String valueJson = record.value();
                    if (valueJson != null && !valueJson.trim().isEmpty()) {
                        processChangefeedEvent(valueJson, table, offsetContext);
                    }
                }

                if (config.getSnapshotMode() == CockroachDBConnectorConfig.SnapshotMode.INITIAL_ONLY
                        && !initialScanInProgress) {
                    LOGGER.info("Initial scan completed and snapshot.mode=initial_only, stopping connector");
                    running.set(false);
                    break;
                }

                metronome.pause();
            }

            LOGGER.debug("Stopped consuming from Kafka topic: {}", topicName);
        }
        catch (Exception e) {
            LOGGER.error("Error consuming from Kafka topic {}: {}", topicName, e.getMessage(), e);
            throw new RuntimeException("Failed to consume from Kafka topic " + topicName, e);
        }
    }

    /**
     * Processes a single changefeed event (regardless of sink type).
     * Handles deduplication, resolved-timestamp filtering, and dispatching.
     */
    private void processChangefeedEvent(String valueJson, TableId table,
                                        CockroachDBOffsetContext offsetContext) {
        if (valueJson == null || valueJson.trim().isEmpty()) {
            return;
        }

        try {
            JsonNode jsonNode = objectMapper.readTree(valueJson);

            if (jsonNode != null && jsonNode.has("resolved")) {
                String resolvedTs = jsonNode.get("resolved").asText();
                if (initialScanInProgress) {
                    initialScanInProgress = false;
                    LOGGER.info("Initial scan completed (first resolved timestamp received: {})", resolvedTs);
                }
                LOGGER.debug("Received resolved timestamp: {}", resolvedTs);
                return;
            }

            String eventId = createEventId(jsonNode);
            if (eventId != null && processedEvents.putIfAbsent(eventId, Boolean.TRUE) != null) {
                LOGGER.debug("Skipping duplicate event: {}", eventId);
                return;
            }

            LOGGER.debug("Processing changefeed event for table {}", table);

            JsonNode payloadNode = resolvePayload(jsonNode);
            Envelope.Operation operation = extractOperation(payloadNode);
            JsonNode afterNode = payloadNode.path("after");
            JsonNode beforeNode = payloadNode.path("before");

            io.debezium.relational.Table tableObj = schema.tableFor(table);
            if (tableObj == null) {
                LOGGER.warn("No schema found for table {}, skipping event", table);
                return;
            }

            CockroachDBPartition partition = new CockroachDBPartition();
            CockroachDBChangeRecordEmitter emitter = new CockroachDBChangeRecordEmitter(
                    partition, offsetContext, clock, config, tableObj, operation,
                    afterNode.isMissingNode() ? null : afterNode,
                    beforeNode.isMissingNode() ? null : beforeNode);

            LOGGER.debug("Dispatching {} event for table {}", operation, table);
            dispatcher.dispatchDataChangeEvent(partition, table, emitter);
        }
        catch (Exception e) {
            LOGGER.error("Error processing changefeed event for table {}: {}",
                    table, e.getMessage(), e);
        }
    }

    /**
     * Creates a unique event identifier from the JSON payload for deduplication.
     * Uses table name, operation type, and nanosecond timestamp.
     */
    private String createEventId(JsonNode jsonNode) {
        try {
            JsonNode payloadNode = resolvePayload(jsonNode);
            String tableName = payloadNode.path("source").path("table_name").asText("");
            String operation = payloadNode.path("op").asText("");
            String timestamp = payloadNode.path("ts_ns").asText("");
            return tableName + ":" + operation + ":" + timestamp;
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * Resolves the payload node, handling both direct and nested {@code payload} structures.
     */
    private JsonNode resolvePayload(JsonNode jsonNode) {
        JsonNode payloadNode = jsonNode.path("payload");
        return payloadNode.isMissingNode() ? jsonNode : payloadNode;
    }

    /**
     * Extracts the Debezium {@link Envelope.Operation} from the changefeed payload.
     * During initial scan, all events are treated as READ operations.
     * Otherwise uses the {@code op} field first, falling back to before/after presence detection.
     */
    private Envelope.Operation extractOperation(JsonNode payloadNode) {
        if (initialScanInProgress) {
            return Envelope.Operation.READ;
        }
        if (payloadNode.has("op")) {
            String op = payloadNode.get("op").asText();
            switch (op) {
                case "c":
                    return Envelope.Operation.CREATE;
                case "u":
                    return Envelope.Operation.UPDATE;
                case "d":
                    return Envelope.Operation.DELETE;
                case "r":
                    return Envelope.Operation.READ;
                default:
                    LOGGER.warn("Unknown operation type: {}", op);
            }
        }

        boolean hasBefore = payloadNode.has("before") && !payloadNode.get("before").isNull();
        boolean hasAfter = payloadNode.has("after") && !payloadNode.get("after").isNull();

        if (hasBefore && hasAfter) {
            return Envelope.Operation.UPDATE;
        }
        else if (hasAfter) {
            return Envelope.Operation.CREATE;
        }
        else if (hasBefore) {
            return Envelope.Operation.DELETE;
        }

        LOGGER.warn("Cannot determine operation type from payload structure");
        return Envelope.Operation.READ;
    }

    /**
     * Builds the CockroachDB {@code CREATE CHANGEFEED} SQL statement for the given table.
     * Includes the {@code initial_scan} option based on the configured snapshot mode.
     * All user-configurable values are sanitized before interpolation.
     */
    private String buildSinkChangefeedQuery(TableId table, String cursor, boolean hasPriorOffset) {
        StringBuilder query = new StringBuilder();
        query.append("CREATE CHANGEFEED FOR TABLE ");
        query.append(sanitizeIdentifier(table.toString()));

        String sinkUri = config.getChangefeedSinkUri();
        String topicName = buildTopicName(table);

        if (sinkUri.contains("?")) {
            sinkUri += "&topic_name=" + sanitizeLiteral(topicName);
        }
        else {
            sinkUri += "?topic_name=" + sanitizeLiteral(topicName);
        }

        query.append(" INTO '").append(sanitizeLiteral(sinkUri)).append("'");

        query.append(" WITH envelope = '").append(sanitizeLiteral(config.getChangefeedEnvelope())).append("'");

        String enrichedProperties = config.getChangefeedEnrichedProperties();
        if (enrichedProperties != null && !enrichedProperties.trim().isEmpty()) {
            query.append(", enriched_properties = '").append(sanitizeLiteral(enrichedProperties)).append("'");
        }

        if (config.isChangefeedIncludeUpdated()) {
            query.append(", updated");
        }

        if (config.isChangefeedIncludeDiff()) {
            query.append(", diff");
        }

        String resolvedInterval = config.getChangefeedResolvedInterval();
        query.append(", resolved = '").append(sanitizeLiteral(resolvedInterval)).append("'");

        String initialScan = config.getInitialScanForSnapshotMode(hasPriorOffset);
        if (initialScan != null) {
            query.append(", initial_scan = '").append(sanitizeLiteral(initialScan)).append("'");
        }

        if (cursor != null && !cursor.trim().isEmpty()
                && !"initial".equals(cursor) && !"now".equals(cursor)) {
            query.append(", cursor = '").append(sanitizeLiteral(cursor)).append("'");
        }

        String sinkOptions = config.getChangefeedSinkOptions();
        if (sinkOptions != null && !sinkOptions.trim().isEmpty()) {
            query.append(", ").append(sanitizeLiteral(sinkOptions));
        }

        return query.toString();
    }

    /**
     * Builds the topic name for a table using the configured prefix and database name.
     * Format: {@code {prefix}.{database}.{schema}.{table}}
     */
    private String buildTopicName(TableId table) {
        String topicPrefix = config.getChangefeedSinkTopicPrefix();
        if (topicPrefix == null || topicPrefix.trim().isEmpty()) {
            topicPrefix = "cockroachdb";
        }
        return topicPrefix + "." + config.getDatabaseName() + "." + table.schema() + "." + table.table();
    }

    /**
     * Sanitizes a SQL string literal value by escaping single quotes.
     * Prevents SQL injection in changefeed query parameters.
     */
    private static String sanitizeLiteral(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "''");
    }

    /**
     * Sanitizes a SQL identifier by removing characters that are not alphanumeric,
     * underscores, periods, or hyphens.
     */
    private static String sanitizeIdentifier(String identifier) {
        if (identifier == null) {
            return "";
        }
        return identifier.replaceAll("[^a-zA-Z0-9_.\\-]", "");
    }

    public void stop() {
        running.set(false);
        LOGGER.info("Stopping CockroachDB streaming change event source");
    }
}
