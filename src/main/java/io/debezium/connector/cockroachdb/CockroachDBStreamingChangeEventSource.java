/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.connector.cockroachdb.serialization.ChangefeedJsonMapper;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Streaming change event source for CockroachDB using native sink changefeeds.
 *
 * <p>Creates a single multi-table changefeed that writes to an external system
 * (Kafka by default), consumes events from all per-table topics in a single
 * KafkaConsumer, routes each event to the correct {@link TableId} based on the
 * Kafka topic name, and dispatches events through Debezium's {@link EventDispatcher}
 * pipeline.</p>
 *
 * <p>By default all configured tables are captured by a single multi-table changefeed, which
 * keeps the connector to one changefeed job (CockroachDB recommends keeping the number of
 * changefeed jobs per cluster modest). For large table counts,
 * {@code cockroachdb.changefeed.max.tables.per.changefeed} splits the tables across several
 * changefeeds to avoid CockroachDB's per-table performance coupling.</p>
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
    private final ObjectMapper objectMapper = ChangefeedJsonMapper.create();

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
     * Maps Kafka topic names to their corresponding {@link TableId} for routing
     * events consumed from the multi-table changefeed.
     */
    private final Map<String, TableId> topicToTableMap = new HashMap<>();

    /**
     * Tracks whether the changefeed is still performing an initial scan.
     * Set to true when a changefeed is created with {@code initial_scan='yes'}
     * and cleared when the first resolved timestamp is received, which signals
     * that the initial scan phase has completed.
     */
    private volatile boolean initialScanInProgress = false;

    /**
     * Partition instance from {@link #execute}, stored for heartbeat dispatch
     * in the consumer loop and resolved-timestamp handler.
     */
    private CockroachDBPartition currentPartition;

    /**
     * Connection to CockroachDB, kept alive during streaming for schema
     * refresh queries when DDL changes are detected.
     */
    private CockroachDBConnection schemaRefreshConnection;

    private volatile CockroachDBOffsetContext currentOffsetContext;

    /**
     * Drives the JMX snapshot metrics (SnapshotRunning/SnapshotCompleted) for the changefeed
     * initial scan. May be null when the connector is constructed without a snapshot phase.
     */
    private final SnapshotProgressListener<CockroachDBPartition> snapshotProgressListener;

    public CockroachDBStreamingChangeEventSource(
                                                 CockroachDBConnectorConfig config,
                                                 EventDispatcher<CockroachDBPartition, TableId> dispatcher,
                                                 CockroachDBSchema schema,
                                                 Clock clock,
                                                 SnapshotProgressListener<CockroachDBPartition> snapshotProgressListener) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.dispatcher = dispatcher;
        this.schema = schema;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    public void init(CockroachDBOffsetContext offsetContext) throws InterruptedException {
        this.currentOffsetContext = offsetContext;
    }

    @Override
    public void execute(ChangeEventSourceContext context, CockroachDBPartition partition,
                        CockroachDBOffsetContext offsetContext)
            throws InterruptedException {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(partition, "partition must not be null");
        Objects.requireNonNull(offsetContext, "offsetContext must not be null");
        Objects.requireNonNull(dispatcher, "dispatcher must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(clock, "clock must not be null");

        this.currentOffsetContext = offsetContext;

        LOGGER.info("Starting CockroachDB streaming from cursor '{}', sink type '{}', heartbeat={}ms",
                offsetContext.getCursor(), config.getChangefeedSinkType(),
                config.getHeartbeatInterval().toMillis());
        running.set(true);
        this.currentPartition = partition;

        try (CockroachDBConnection connection = new CockroachDBConnection(config)) {
            connection.connect();
            connection.checkRangefeedEnabled();
            this.schemaRefreshConnection = connection;
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
                    && !CockroachDBOffsetContext.CURSOR_INITIAL.equals(cursor)
                    && !CockroachDBOffsetContext.CURSOR_NOW.equals(cursor);
            LOGGER.info("Snapshot mode: {}, hasPriorOffset: {}, cursor: '{}'",
                    config.getSnapshotMode().getValue(), hasPriorOffset, cursor);

            // Build topic-to-table mapping for all tables
            topicToTableMap.clear();
            for (TableId table : tables) {
                String topicName = buildTopicName(table);
                topicToTableMap.put(topicName, table);
                LOGGER.debug("Mapped topic {} -> table {}", topicName, table);
            }

            // Create a single multi-table changefeed (if not already running). For sinkless mode
            // this is a no-op; the changefeed is created and streamed during consumption.
            createChangefeeds(connection, tables, offsetContext, hasPriorOffset);

            // A sinkless query can discover that a saved cursor is stale only when executeQuery is
            // called. Its snapshot lifecycle is therefore initialized in consumeSinkless, after a
            // possible when_needed fallback has selected the actual initial_scan mode.
            if (!config.isSinklessChangefeed()) {
                initializeSnapshotLifecycle(tables, offsetContext);
            }

            // Consume events: from the intermediate Kafka topics (kafka mode) or directly from a
            // sinkless changefeed streamed over SQL (sinkless mode).
            consumeChangefeedEvents(tables, offsetContext, context, hasPriorOffset);
        }
        catch (SQLException e) {
            // CockroachDB is the authority on changefeed options (sink URI, enriched_properties,
            // privileges, and so on). Surface its message directly so an invalid value or a
            // permission problem is visible in the task status without digging into the cause chain.
            LOGGER.error("Error in CockroachDB streaming", e);
            throw new RuntimeException("Failed to stream changes from CockroachDB: " + e.getMessage(), e);
        }
        finally {
            if (this.schemaRefreshConnection != null) {
                try {
                    this.schemaRefreshConnection.close();
                }
                catch (Exception e) {
                    LOGGER.debug("Closing the schema refresh connection failed: {}", e.getMessage());
                }
            }
            this.schemaRefreshConnection = null;
            running.set(false);
            LOGGER.info("Stopped CockroachDB streaming change event source");
        }
    }

    /**
     * Creates a single multi-table changefeed covering all configured tables.
     * If a changefeed already exists for any of the tables, creation is skipped.
     */
    /**
     * Above this many tables in a single changefeed, the connector logs a recommendation to split
     * the feed (see {@code cockroachdb.changefeed.max.tables.per.changefeed}). This is a connector
     * heuristic to surface CockroachDB's coupling guidance, not a hard CockroachDB limit.
     */
    static final int SINGLE_CHANGEFEED_TABLE_WARN_THRESHOLD = 100;

    /**
     * Creates the changefeed(s) covering all configured tables. By default all tables go into a
     * single changefeed; when {@code cockroachdb.changefeed.max.tables.per.changefeed} is positive,
     * the tables are split into multiple changefeeds of at most that size to avoid CockroachDB's
     * per-table performance coupling. A changefeed is skipped if one already covers its tables (for
     * idempotent restarts and reuse of a pre-provisioned feed).
     */
    private void createChangefeeds(
                                   CockroachDBConnection connection,
                                   List<TableId> tables,
                                   CockroachDBOffsetContext offsetContext,
                                   boolean hasPriorOffset)
            throws SQLException {

        if (config.isSinklessChangefeed()) {
            // A sinkless changefeed is created and streamed in a single executeQuery during
            // consumption (see consumeSinkless), so there is nothing to pre-create here.
            initialScanInProgress = "yes".equals(config.getInitialScanForSnapshotMode(hasPriorOffset));
            return;
        }

        int maxPerChangefeed = config.getChangefeedMaxTablesPerChangefeed();
        List<List<TableId>> groups = partitionTables(tables, maxPerChangefeed);

        if (maxPerChangefeed <= 0 && tables.size() >= SINGLE_CHANGEFEED_TABLE_WARN_THRESHOLD) {
            LOGGER.warn("Capturing {} tables in a single changefeed. CockroachDB's performance can become coupled "
                    + "across many tables in one changefeed. Consider setting "
                    + "cockroachdb.changefeed.max.tables.per.changefeed to split them into multiple changefeeds, "
                    + "or run multiple connector instances each capturing a related subset of tables.", tables.size());
        }

        if (groups.size() > 1) {
            LOGGER.info("Splitting {} table(s) into {} changefeed(s) (max {} tables each)",
                    tables.size(), groups.size(), maxPerChangefeed);
        }

        boolean recoveredFromInvalidCursor = false;
        for (List<TableId> group : groups) {
            boolean alreadyRunning = false;
            for (TableId table : group) {
                if (changefeedExists(connection, table, tables)) {
                    alreadyRunning = true;
                    break;
                }
            }
            if (alreadyRunning) {
                LOGGER.info("Changefeed already running for table(s) {}, skipping creation", group);
                continue;
            }

            LOGGER.info("Creating changefeed for {} table(s)", group.size());

            try (Statement stmt = connection.connection().createStatement()) {
                recoveredFromInvalidCursor |= executeSinkChangefeed(
                        stmt, group, offsetContext.getCursor(), hasPriorOffset);
                LOGGER.info("Created changefeed for tables: {}", group);
            }
        }

        String initialScan = config.getInitialScanForSnapshotMode(hasPriorOffset);
        initialScanInProgress = recoveredFromInvalidCursor || "yes".equals(initialScan);
        if (initialScanInProgress) {
            LOGGER.info("Initial scan in progress (snapshot.mode={})",
                    config.getSnapshotMode().getValue());
        }
    }

    /**
     * Executes a sink changefeed creation and, for {@code snapshot.mode=when_needed} only, retries
     * once with a new initial scan when CockroachDB specifically rejects the stored cursor.
     *
     * @return {@code true} when the invalid-cursor recovery path was used
     */
    boolean executeSinkChangefeed(Statement stmt, List<TableId> tables, String cursor, boolean hasPriorOffset)
            throws SQLException {
        String query = buildSinkChangefeedQuery(tables, cursor, hasPriorOffset);
        LOGGER.debug("Changefeed query: {}", query);
        try {
            stmt.execute(query);
            return false;
        }
        catch (SQLException e) {
            if (!shouldRecoverFromInvalidCursor(e, hasPriorOffset)) {
                throw e;
            }
            logInvalidCursorRecovery(e);
            String recoveryQuery = buildSinkChangefeedQuery(tables, null, false);
            LOGGER.debug("Invalid-cursor recovery changefeed query: {}", recoveryQuery);
            stmt.execute(recoveryQuery);
            return true;
        }
    }

    private void initializeSnapshotLifecycle(List<TableId> tables, CockroachDBOffsetContext offsetContext) {
        // Drive the snapshot lifecycle for the changefeed initial scan so snapshot_completed,
        // source.snapshot, and the JMX snapshot metrics reflect reality. Completion is signalled on
        // the first resolved timestamp (see processChangefeedEvent).
        if (initialScanInProgress) {
            offsetContext.markSnapshotRecord(SnapshotRecord.TRUE);
            if (snapshotProgressListener != null) {
                snapshotProgressListener.snapshotStarted(currentPartition);
            }
            LOGGER.info("Initial scan started for {} table(s)", tables.size());
        }
        else {
            // No initial scan (snapshot.mode no_data/never, or a valid prior cursor): there is
            // nothing to backfill, so mark the snapshot already completed.
            offsetContext.markSnapshotRecord(SnapshotRecord.FALSE);
            offsetContext.preSnapshotCompletion();
        }
    }

    /**
     * Partitions the tables into changefeed groups. A {@code maxPerGroup} of 0 or less (the default)
     * keeps every table in a single group, preserving the single-changefeed behavior. Otherwise the
     * tables are split into consecutive chunks of at most {@code maxPerGroup}.
     */
    static List<List<TableId>> partitionTables(List<TableId> tables, int maxPerGroup) {
        if (maxPerGroup <= 0 || tables.size() <= maxPerGroup) {
            return List.of(new ArrayList<>(tables));
        }
        List<List<TableId>> groups = new ArrayList<>();
        for (int i = 0; i < tables.size(); i += maxPerGroup) {
            groups.add(new ArrayList<>(tables.subList(i, Math.min(i + maxPerGroup, tables.size()))));
        }
        return groups;
    }

    /**
     * Routes event consumption to the appropriate sink-specific implementation.
     * Subscribes to all per-table topics in a single consumer for concurrent processing.
     */
    private void consumeChangefeedEvents(List<TableId> tables, CockroachDBOffsetContext offsetContext,
                                         ChangeEventSourceContext context, boolean hasPriorOffset)
            throws InterruptedException {
        String sinkType = config.getChangefeedSinkType();
        LOGGER.debug("Routing to sink consumer type='{}' for {} table(s)", sinkType, tables.size());
        switch (sinkType) {
            case "kafka":
                consumeFromKafkaTopics(tables, offsetContext, context);
                break;
            case "sinkless":
                consumeSinkless(tables, offsetContext, context, hasPriorOffset);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported changefeed sink type: '" + sinkType
                                + "'. Supported: kafka, sinkless. Planned: webhook, pubsub, cloudstorage.");
        }
    }

    /**
     * Checks whether a running changefeed job already exists for the given table
     * using the same topic prefix as this connector instance.
     * Matches the fully-qualified table name and a {@code topic_prefix=} marker in the
     * changefeed job description to avoid false positives from changefeeds created by
     * other connector instances targeting the same tables with different topic prefixes.
     */
    private boolean changefeedExists(CockroachDBConnection connection, TableId table, List<TableId> allIncludedTables) throws SQLException {
        String fqTableName = sanitizeIdentifier(table.toString());
        String topicPrefix = resolveTopicPrefix();
        String sinkMarker = "topic_prefix=" + topicPrefix;

        try (Statement stmt = connection.connection().createStatement()) {
            String query = "SELECT job_id, description FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'";
            try (var rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    String jobId = rs.getString(1);
                    String description = rs.getString(2);
                    if (description != null && description.contains(fqTableName) && description.contains(sinkMarker)) {
                        // A changefeed with our topic prefix already covers this table. The connector
                        // can only consume the enriched envelope, so refuse to reuse one created with a
                        // different envelope rather than consuming it and failing to parse events. We
                        // cannot fall back to creating our own here either: it would target the same
                        // topics as the existing feed.
                        if (!changefeedUsesEnrichedEnvelope(description)) {
                            throw new IllegalStateException("Found an existing running changefeed for table " + table
                                    + " using topic prefix '" + topicPrefix + "' that was not created with envelope='enriched'. "
                                    + "The CockroachDB connector can only consume the enriched envelope. Drop or recreate that "
                                    + "changefeed with envelope='enriched' (and full_table_name), or use a different topic prefix.");
                        }
                        // Changefeed options are fixed at creation time, so a reused changefeed that
                        // lacks the diff option can never deliver the before images the configuration
                        // promises. Fail fast instead of silently emitting updates without them.
                        if (config.isChangefeedIncludeDiff() && !changefeedHasDiffOption(description)) {
                            throw new IllegalStateException("Found an existing running changefeed for table " + table
                                    + " using topic prefix '" + topicPrefix + "' that was created without the diff option, but "
                                    + "cockroachdb.changefeed.include.diff is enabled. Update events from that changefeed cannot "
                                    + "carry a before image. Cancel the changefeed job and restart the connector so it recreates "
                                    + "the changefeed with diff, or disable cockroachdb.changefeed.include.diff.");
                        }
                        if (!config.isChangefeedIncludeDiff() && changefeedHasDiffOption(description)) {
                            LOGGER.warn("Reusing changefeed for table {} that was created with the diff option while "
                                    + "cockroachdb.changefeed.include.diff is disabled; update events will carry before images.",
                                    table);
                        }
                        // The captured table set is frozen at job creation. If tables were removed
                        // from table.include.list since, the reused job keeps streaming them at full
                        // volume into intermediate topics that nothing consumes.
                        Set<String> includedIdentifiers = allIncludedTables.stream()
                                .map(t -> sanitizeIdentifier(t.toString()))
                                .collect(Collectors.toSet());
                        List<String> extraTables = findExtraCapturedTables(description, includedIdentifiers);
                        if (!extraTables.isEmpty()) {
                            LOGGER.warn("Reused changefeed job {} also captures table(s) {} that are not in "
                                    + "table.include.list. The job keeps streaming them into intermediate topics that "
                                    + "this connector does not consume, wasting changefeed work, network, and broker "
                                    + "storage. To capture only the configured tables, stop the connector, run "
                                    + "CANCEL JOB {}, and restart the connector so it recreates the changefeed.",
                                    jobId, extraTables, jobId);
                        }
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
     * Returns {@code true} if a {@code SHOW CHANGEFEED JOBS} description indicates the changefeed
     * was created with the enriched envelope. The connector can only consume the enriched envelope,
     * so this guards reuse of a pre-existing changefeed. Whitespace is normalized because the
     * description may render the option as {@code envelope = 'enriched'} or {@code envelope='enriched'}.
     */
    /**
     * Returns {@code true} if a {@code SHOW CHANGEFEED JOBS} description lists the {@code diff}
     * option. Only the option list inside {@code WITH OPTIONS (...)} is inspected, so table names
     * that contain the word diff do not match.
     */
    static boolean changefeedHasDiffOption(String description) {
        if (description == null) {
            return false;
        }
        int start = description.indexOf("WITH OPTIONS (");
        if (start < 0) {
            return false;
        }
        String options = description.substring(start + "WITH OPTIONS (".length());
        int end = options.lastIndexOf(')');
        if (end >= 0) {
            options = options.substring(0, end);
        }
        for (String option : options.split(",")) {
            if ("diff".equals(option.trim())) {
                return true;
            }
        }
        return false;
    }

    static boolean changefeedUsesEnrichedEnvelope(String description) {
        if (description == null) {
            return false;
        }
        return description.replaceAll("\\s+", "").contains("envelope='enriched'");
    }

    /**
     * Consumes changefeed events from all per-table Kafka topics in a single consumer.
     * Routes each event to the correct {@link TableId} based on the Kafka topic name.
     */
    private void consumeFromKafkaTopics(List<TableId> tables, CockroachDBOffsetContext offsetContext,
                                        ChangeEventSourceContext context)
            throws InterruptedException {
        List<String> topicNames = tables.stream()
                .map(this::buildTopicName)
                .collect(Collectors.toList());

        Properties props = new Properties();
        String explicitBootstrap = config.getChangefeedKafkaBootstrapServers();
        if (explicitBootstrap != null && !explicitBootstrap.trim().isEmpty()) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, explicitBootstrap);
        }
        else {
            String sinkUri = config.getChangefeedSinkUri();
            if (sinkUri != null) {
                String bootstrapServers = sinkUri.replaceFirst("^(?i)(kafka|PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL)://", "");
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            }
        }
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getChangefeedKafkaConsumerGroupPrefix());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getChangefeedKafkaAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        applyConsumerSecurity(config, props);

        LOGGER.debug("Kafka consumer config: bootstrapServers={}, groupId={}, autoOffsetReset={}, pollTimeout={}ms, topics={}",
                props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                config.getChangefeedKafkaConsumerGroupPrefix(),
                config.getChangefeedKafkaAutoOffsetReset(),
                config.getChangefeedKafkaPollTimeoutMs(),
                topicNames);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // On (re)assignment, resume each partition from the position persisted in the Debezium
            // offset. Without this the consumer never commits and resets to earliest on every restart,
            // re-reading and re-emitting the entire retained changefeed topic (dbz#2154). Partitions
            // with no stored position fall back to auto.offset.reset, preserving first-start behavior.
            consumer.subscribe(topicNames, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    for (TopicPartition tp : partitions) {
                        Long stored = offsetContext.getConsumerOffset(kafkaConsumerOffsetKey(tp.topic(), tp.partition()));
                        if (stored != null) {
                            consumer.seek(tp, stored + 1);
                            LOGGER.info("Resuming {}: last committed offset {}, seeking to next offset {}",
                                    tp, stored, stored + 1);
                        }
                    }
                }
            });
            LOGGER.info("Consuming from {} Kafka topic(s): {}", topicNames.size(), topicNames);

            Duration pollInterval = Duration.ofMillis(config.getChangefeedPollIntervalMs());
            Metronome metronome = Metronome.sleeper(pollInterval, clock);
            int emptyPollCount = 0;

            while (context.isRunning() && running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(config.getChangefeedKafkaPollTimeoutMs()));

                if (records.isEmpty()) {
                    emptyPollCount++;
                    if (emptyPollCount % 100 == 0) {
                        LOGGER.debug("No events received for {} consecutive polls", emptyPollCount);
                    }
                    // Emit heartbeat during idle periods to advance offsets
                    dispatcher.dispatchHeartbeatEventAlsoToIncrementalSnapshot(currentPartition, offsetContext);
                }
                else {
                    emptyPollCount = 0;
                    LOGGER.trace("Polled {} records from {} topic(s)", records.count(), topicNames.size());
                }

                for (ConsumerRecord<String, String> record : records) {
                    if (!context.isRunning()) {
                        break;
                    }
                    // Record the consumed position so it is persisted in the Debezium offset (flushed
                    // by Connect after the dispatched events are produced) and a restart can resume here.
                    offsetContext.setConsumerOffset(kafkaConsumerOffsetKey(record.topic(), record.partition()), record.offset());
                    String valueJson = record.value();
                    if (valueJson != null && !valueJson.trim().isEmpty()) {
                        TableId table = resolveTableFromTopic(record.topic());
                        if (table != null) {
                            processChangefeedEvent(record.key(), valueJson, table, offsetContext);
                        }
                        else {
                            LOGGER.warn("Cannot resolve table for topic '{}', skipping event", record.topic());
                        }
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

            LOGGER.debug("Stopped consuming from Kafka topics: {}", topicNames);
        }
        catch (Exception e) {
            LOGGER.error("Error consuming from Kafka topics {}: {}", topicNames, e.getMessage(), e);
            throw new RuntimeException("Failed to consume from Kafka topics " + topicNames, e);
        }
    }

    /**
     * Builds the sink-agnostic consumer-offset key for a Kafka topic partition, used to persist and
     * resume the intermediate consumer position in the Debezium offset (see
     * {@link CockroachDBOffsetContext#CONSUMER_OFFSET_PREFIX}).
     */
    static String kafkaConsumerOffsetKey(String topic, int partition) {
        return topic + ":" + partition;
    }

    /**
     * Consumes a CockroachDB sinkless changefeed: runs {@code CREATE CHANGEFEED ... } without
     * {@code INTO} on a dedicated SQL connection and streams change events directly from the
     * resulting {@link ResultSet}, with no intermediate Kafka cluster. Each row's {@code value}
     * column is the same enriched JSON the kafka path receives, so it is handed to the shared
     * {@link #processChangefeedEvent} pipeline (parse, dedup, schema refresh, dispatch, and
     * resolved-timestamp offset advancement) unchanged.
     */
    private void consumeSinkless(List<TableId> tables, CockroachDBOffsetContext offsetContext,
                                 ChangeEventSourceContext context, boolean hasPriorOffset)
            throws InterruptedException {
        Map<String, TableId> tablesBySchemaAndName = new HashMap<>();
        for (TableId table : tables) {
            tablesBySchemaAndName.put(table.schema() + "." + table.table(), table);
        }

        String changefeedQuery = buildSinklessChangefeedQuery(tables, offsetContext.getCursor(), hasPriorOffset);
        LOGGER.info("Starting sinkless changefeed for {} table(s) (streaming over SQL, no intermediate Kafka)", tables.size());
        LOGGER.debug("Sinkless changefeed query: {}", changefeedQuery);

        // The streaming query holds its connection open indefinitely, so it must use a dedicated
        // connection separate from the schema-refresh connection opened in execute().
        try (CockroachDBConnection streamConnection = new CockroachDBConnection(config)) {
            streamConnection.connect();
            Connection jdbc = streamConnection.connection();
            // A sinkless changefeed must stream over a server-side cursor: pgjdbc only enables that
            // when autoCommit is false and a fetch size is set. CockroachDB treats CREATE CHANGEFEED
            // as DDL and, by default (the autocommit_before_ddl session setting), auto-commits the
            // surrounding transaction before running it, which tears the cursor down mid-execute and
            // closes the connection (EOFException). Disabling autocommit_before_ddl on this session
            // lets the changefeed run inside pgjdbc's transaction so the cursor streams. See
            // CockroachDB issue #85436.
            try (Statement init = jdbc.createStatement()) {
                init.execute("SET autocommit_before_ddl = false");
            }
            jdbc.setAutoCommit(false);
            try (Statement stmt = jdbc.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                // Fetch one row at a time. The change stream is unbounded and CockroachDB does not
                // return a partial cursor batch, so any fetch size greater than one would withhold
                // events until that many rows accumulated, which is unacceptable latency for CDC.
                // Row-at-a-time delivers each change (and each resolved timestamp) as soon as
                // CockroachDB emits it. This is independent of cockroachdb.changefeed.batch.size,
                // which governs the kafka-mode consumer poll, not the SQL cursor.
                stmt.setFetchSize(1);
                try (ResultSet rs = executeSinklessChangefeed(
                        stmt, jdbc, tables, offsetContext.getCursor(), hasPriorOffset)) {
                    initializeSnapshotLifecycle(tables, offsetContext);
                    while (context.isRunning() && running.get() && rs.next()) {
                        String valueJson = readChangefeedValue(rs);
                        if (valueJson == null || valueJson.trim().isEmpty()) {
                            continue;
                        }
                        String keyJson = readChangefeedKey(rs);
                        TableId table = resolveTableFromSource(valueJson, tablesBySchemaAndName);
                        // Resolved-timestamp rows carry no table; processChangefeedEvent handles
                        // them by their "resolved" field regardless of the table argument.
                        processChangefeedEvent(keyJson, valueJson, table, offsetContext);

                        if (config.getSnapshotMode() == CockroachDBConnectorConfig.SnapshotMode.INITIAL_ONLY
                                && !initialScanInProgress) {
                            LOGGER.info("Initial scan completed and snapshot.mode=initial_only, stopping connector");
                            running.set(false);
                            break;
                        }
                    }
                }
            }
            LOGGER.debug("Stopped sinkless changefeed consumption");
        }
        catch (SQLException e) {
            // On shutdown the dedicated streaming connection is closed out from under the open
            // cursor, which surfaces here as an I/O error ("sending to the backend" / EOF). That is
            // the expected way a never-ending changefeed read ends, not a failure, so do not escalate
            // it when the source is already stopping.
            if (!context.isRunning() || !running.get()) {
                LOGGER.info("Sinkless changefeed consumption stopped during shutdown");
                return;
            }
            LOGGER.error("Error in sinkless changefeed consumption: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to stream sinkless changefeed from CockroachDB: " + e.getMessage(), e);
        }
    }

    /**
     * Executes the sinkless streaming query and retries it once from a fresh initial scan when a
     * stored cursor is rejected under {@code snapshot.mode=when_needed}. The failed statement
     * aborts pgjdbc's explicit transaction, so it must be rolled back before the retry.
     */
    ResultSet executeSinklessChangefeed(Statement stmt, Connection connection, List<TableId> tables,
                                        String cursor, boolean hasPriorOffset)
            throws SQLException {
        String query = buildSinklessChangefeedQuery(tables, cursor, hasPriorOffset);
        try {
            return stmt.executeQuery(query);
        }
        catch (SQLException e) {
            if (!shouldRecoverFromInvalidCursor(e, hasPriorOffset)) {
                throw e;
            }
            logInvalidCursorRecovery(e);
            connection.rollback();
            initialScanInProgress = true;
            String recoveryQuery = buildSinklessChangefeedQuery(tables, null, false);
            LOGGER.debug("Invalid-cursor recovery sinkless query: {}", recoveryQuery);
            return stmt.executeQuery(recoveryQuery);
        }
    }

    private boolean shouldRecoverFromInvalidCursor(SQLException exception, boolean hasPriorOffset) {
        return hasPriorOffset
                && config.getSnapshotMode() == CockroachDBConnectorConfig.SnapshotMode.WHEN_NEEDED
                && isInvalidCursorRejection(exception);
    }

    private void logInvalidCursorRecovery(SQLException exception) {
        LOGGER.warn("CockroachDB rejected the stored changefeed cursor while snapshot.mode=when_needed is configured. "
                + "Retrying once without the cursor and with an initial scan. Existing rows will be re-read and "
                + "duplicate events can be emitted. Deletes that occurred while the connector was offline cannot "
                + "be recovered by this snapshot and will be lost. CockroachDB error: {}", exception.getMessage());
    }

    /**
     * Recognizes only CockroachDB's specific historical-cursor failures. General undefined-table,
     * connectivity, authorization, and syntax failures deliberately do not match.
     */
    static boolean isInvalidCursorRejection(SQLException exception) {
        for (SQLException current = exception; current != null; current = current.getNextException()) {
            if (isInvalidCursorMessage(current.getMessage())) {
                return true;
            }
        }
        for (Throwable current = exception.getCause(); current != null; current = current.getCause()) {
            if (isInvalidCursorMessage(current.getMessage())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isInvalidCursorMessage(String message) {
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("do the targets exist at the specified cursor time")
                || normalized.contains("must be after replica gc threshold");
    }

    /**
     * Reads the changefeed {@code value} column. CockroachDB returns it as BYTES, so prefer
     * {@code getBytes} and decode UTF-8, falling back to {@code getString}.
     */
    private static String readChangefeedKey(ResultSet rs) throws SQLException {
        try {
            byte[] bytes = rs.getBytes("key");
            if (bytes != null) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return rs.getString("key");
        }
        catch (SQLException e) {
            // Resolved-timestamp rows and older server versions may not expose a key column.
            return null;
        }
    }

    private static String readChangefeedValue(ResultSet rs) throws SQLException {
        byte[] bytes = rs.getBytes("value");
        if (bytes != null) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return rs.getString("value");
    }

    /**
     * Resolves the {@link TableId} for a sinkless changefeed row from the event's enriched
     * {@code source} block ({@code schema_name} + {@code table_name}), which is schema-qualified so
     * same-named tables in different schemas route correctly. Returns null for rows without a usable
     * source (for example resolved-timestamp rows), which the caller passes through harmlessly.
     */
    TableId resolveTableFromSource(String valueJson, Map<String, TableId> tablesBySchemaAndName) {
        try {
            JsonNode payload = resolvePayload(objectMapper.readTree(valueJson));
            JsonNode source = payload.path("source");
            String schema = source.path("schema_name").asText(null);
            String table = source.path("table_name").asText(null);
            if (schema != null && table != null) {
                return tablesBySchemaAndName.get(schema + "." + table);
            }
        }
        catch (Exception e) {
            LOGGER.debug("Could not resolve table from sinkless event source block: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Resolves the {@link TableId} for a Kafka topic using the pre-built mapping.
     * Falls back to parsing the topic name if no exact match is found.
     */
    private TableId resolveTableFromTopic(String topic) {
        TableId table = topicToTableMap.get(topic);
        if (table != null) {
            return table;
        }

        // Fallback: parse topic name (format: prefix.database.schema.table)
        String[] parts = topic.split("\\.");
        if (parts.length >= 3) {
            String schemaName = parts[parts.length - 2];
            String tableName = parts[parts.length - 1];
            for (TableId candidate : topicToTableMap.values()) {
                if (candidate.table().equals(tableName) && candidate.schema().equals(schemaName)) {
                    LOGGER.debug("Resolved table {} from topic {} via fallback parsing", candidate, topic);
                    topicToTableMap.put(topic, candidate);
                    return candidate;
                }
            }
        }

        return null;
    }

    /**
     * Processes a single changefeed event (regardless of sink type).
     * Handles deduplication, resolved-timestamp filtering, and dispatching.
     */
    private void processChangefeedEvent(String keyJson, String valueJson, TableId table,
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
                    // The backfill is done: clear source.snapshot, mark snapshot_completed, and update
                    // the JMX snapshot metrics so all snapshot surfaces agree (matching other connectors).
                    offsetContext.markSnapshotRecord(SnapshotRecord.FALSE);
                    offsetContext.preSnapshotCompletion();
                    if (snapshotProgressListener != null && currentPartition != null) {
                        try {
                            snapshotProgressListener.snapshotCompleted(currentPartition);
                        }
                        catch (Exception e) {
                            LOGGER.warn("Snapshot metrics completion notification failed: {}", e.getMessage());
                        }
                    }
                }

                // Update offset with the resolved timestamp so offsets advance
                offsetContext.setCursor(resolvedTs);
                offsetContext.setTimestamp(parseResolvedTimestamp(resolvedTs));
                LOGGER.debug("Received resolved timestamp: {}, offset advanced", resolvedTs);

                // Dispatch heartbeat to commit the advanced offset
                dispatcher.dispatchHeartbeatEventAlsoToIncrementalSnapshot(currentPartition, offsetContext);
                return;
            }

            String eventId = createEventId(table, jsonNode, keyJson);
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

            // Detect schema changes by comparing event fields against registered columns.
            // Deletes carry {@code after: null} (and no before without the diff option), and
            // a null row image says nothing about the schema, so it must not be read as
            // "every column absent" and trigger a refresh per delete (debezium/dbz#2322).
            JsonNode dataNode = !afterNode.isMissingNode() && !afterNode.isNull() ? afterNode : beforeNode;
            if (dataNode != null && !dataNode.isMissingNode() && !dataNode.isNull() && hasSchemaChanged(dataNode, tableObj)) {
                tableObj = refreshTableSchema(table);
            }

            // The changefeed message key carries the primary key columns for every event,
            // including deletes that have no before image.
            JsonNode keyNode = null;
            if (keyJson != null && !keyJson.trim().isEmpty()) {
                JsonNode parsedKey = objectMapper.readTree(keyJson);
                keyNode = (parsedKey != null && parsedKey.has("payload")) ? parsedKey.get("payload") : parsedKey;
            }

            CockroachDBChangeRecordEmitter emitter = new CockroachDBChangeRecordEmitter(
                    currentPartition, offsetContext, clock, config, tableObj, operation,
                    afterNode.isMissingNode() ? null : afterNode,
                    beforeNode.isMissingNode() ? null : beforeNode,
                    keyNode);

            LOGGER.debug("Dispatching {} event for table {}", operation, table);
            dispatcher.dispatchDataChangeEvent(currentPartition, table, emitter);
        }
        catch (DebeziumException e) {
            // Deliberate failure (for example a schema refresh that failed after reconnect):
            // propagate so the task restarts and replays the event instead of skipping it.
            throw e;
        }
        catch (Exception e) {
            LOGGER.error("Error processing changefeed event for table {}: {}",
                    table, e.getMessage(), e);
        }
    }

    /**
     * Creates a unique event identifier for deduplication.
     * <p>The identifier combines the schema-qualified {@link TableId} with the operation
     * type, the event's nanosecond timestamp, and the changefeed message key. The table
     * identity is taken from the {@code TableId} (derived from the Kafka topic the event
     * arrived on), not from the message body: the changefeed {@code source.table_name}
     * field is unqualified, so two same-named tables in different schemas (for example
     * {@code public.orders} and {@code inventory.orders}) that change at the same MVCC
     * timestamp would otherwise collide and have one event silently dropped. Using the
     * {@code TableId} also removes any dependency on the {@code source} enriched property
     * being present.
     * <p>The message key is required to tell apart different rows that share an operation
     * and timestamp: two rows changed in the same transaction can carry the same
     * {@code ts_ns}, and without the key one of the two events is silently discarded as a
     * duplicate (debezium/dbz#2283). A missing key degrades to table, operation and
     * timestamp only.
     */
    static String createEventId(TableId table, JsonNode jsonNode, String keyJson) {
        try {
            JsonNode payloadNode = resolvePayload(jsonNode);
            String operation = payloadNode.path("op").asText("");
            String timestamp = payloadNode.path("ts_ns").asText("");
            String rowKey = keyJson == null ? "" : keyJson.trim();
            return table.identifier() + ":" + operation + ":" + timestamp + ":" + rowKey;
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * Resolves the payload node, handling both direct and nested {@code payload} structures.
     */
    private static JsonNode resolvePayload(JsonNode jsonNode) {
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
     * Builds a multi-table {@code CREATE CHANGEFEED FOR table1, table2, ...} SQL statement.
     * Uses {@code full_table_name} so CockroachDB creates topics in {@code db.schema.table}
     * format, and adds {@code topic_prefix} so the final Kafka topic name is
     * {@code prefix.db.schema.table} -- matching what {@link #buildTopicName(TableId)} produces.
     * When no explicit sink topic prefix is configured, falls back to {@code topic.prefix}.
     */
    String buildSinkChangefeedQuery(List<TableId> tables, String cursor, boolean hasPriorOffset) {
        StringBuilder query = new StringBuilder();
        query.append("CREATE CHANGEFEED FOR TABLE ");
        query.append(tables.stream()
                .map(t -> sanitizeIdentifier(t.toString()))
                .collect(Collectors.joining(", ")));

        String sinkUri = config.getChangefeedSinkUri();
        sinkUri = injectSinkTlsParams(sinkUri);
        // Use the resolved topic prefix verbatim so the user controls the separator. With
        // full_table_name, CockroachDB names topics <prefix><database>.<schema>.<table>, which is
        // exactly what buildTopicName subscribes to.
        String separator = sinkUri.contains("?") ? "&" : "?";
        sinkUri = sinkUri + separator + "topic_prefix=" + sanitizeLiteral(resolveTopicPrefix());
        query.append(" INTO '").append(sanitizeLiteral(sinkUri)).append("'");

        // full_table_name is kafka-specific: it drives the per-table topic naming the consumer
        // subscribes to. The remaining options are shared with the sinkless changefeed.
        query.append(" WITH full_table_name, ").append(buildChangefeedWithOptions(cursor, hasPriorOffset));
        return query.toString();
    }

    /**
     * Builds the {@code CREATE CHANGEFEED} statement for a sinkless changefeed: no {@code INTO},
     * no {@code topic_prefix}, no {@code full_table_name}, and no sink TLS injection. CockroachDB
     * streams the change events back over the SQL connection (see {@link #consumeSinkless}).
     */
    String buildSinklessChangefeedQuery(List<TableId> tables, String cursor, boolean hasPriorOffset) {
        StringBuilder query = new StringBuilder();
        query.append("CREATE CHANGEFEED FOR TABLE ");
        query.append(tables.stream()
                .map(t -> sanitizeIdentifier(t.toString()))
                .collect(Collectors.joining(", ")));
        query.append(" WITH ").append(buildChangefeedWithOptions(cursor, hasPriorOffset));
        return query.toString();
    }

    /**
     * Builds the comma-separated {@code WITH} options shared by the sink-based and sinkless
     * changefeeds: the fixed enriched envelope, enriched_properties, updated/diff, the resolved
     * interval, the snapshot-derived initial_scan, an optional resume cursor, and any user sink
     * options. Returned without a leading comma so callers can prefix it with their own options
     * (for example {@code full_table_name} for the kafka sink).
     */
    private String buildChangefeedWithOptions(String cursor, boolean hasPriorOffset) {
        List<String> options = new ArrayList<>();
        // The connector parses only the enriched envelope (it relies on the 'op' and 'ts_ns' fields
        // it provides), so the envelope is fixed rather than configurable.
        options.add("envelope = 'enriched'");

        String enrichedProperties = config.getChangefeedEnrichedProperties();
        if (enrichedProperties != null && !enrichedProperties.trim().isEmpty()) {
            options.add("enriched_properties = '" + sanitizeLiteral(enrichedProperties) + "'");
        }
        if (config.isChangefeedIncludeUpdated()) {
            options.add("updated");
        }
        if (config.isChangefeedIncludeDiff()) {
            options.add("diff");
        }
        options.add("resolved = '" + sanitizeLiteral(config.getChangefeedResolvedInterval()) + "'");

        String initialScan = config.getInitialScanForSnapshotMode(hasPriorOffset);
        if (initialScan != null) {
            options.add("initial_scan = '" + sanitizeLiteral(initialScan) + "'");
        }
        if (cursor != null && !cursor.trim().isEmpty()
                && !CockroachDBOffsetContext.CURSOR_INITIAL.equals(cursor)
                && !CockroachDBOffsetContext.CURSOR_NOW.equals(cursor)) {
            options.add("cursor = '" + sanitizeLiteral(cursor) + "'");
        }
        String sinkOptions = config.getChangefeedSinkOptions();
        if (sinkOptions != null && !sinkOptions.trim().isEmpty()) {
            options.add(sanitizeLiteral(sinkOptions));
        }
        // kafka_sink_config takes a JSON document as a quoted SQL literal; the value is
        // validated as JSON at configuration time and internal single quotes are doubled here.
        String kafkaSinkConfig = config.getChangefeedKafkaSinkConfig();
        if (kafkaSinkConfig != null && !kafkaSinkConfig.trim().isEmpty()) {
            options.add("kafka_sink_config='" + sanitizeLiteral(kafkaSinkConfig) + "'");
        }
        return String.join(", ", options);
    }

    /**
     * Resolves the intermediate-topic prefix. When the user sets
     * {@code cockroachdb.changefeed.sink.topic.prefix}, it is used verbatim so the user controls the
     * separator (for example {@code crdb.} or {@code env-prod-}). When it is not set, it defaults to
     * {@code <topic.prefix>.} so the default topic layout remains
     * {@code <topic.prefix>.<database>.<schema>.<table>}.
     */
    private String resolveTopicPrefix() {
        String topicPrefix = config.getChangefeedSinkTopicPrefix();
        if (topicPrefix != null && !topicPrefix.trim().isEmpty()) {
            return topicPrefix;
        }
        return config.getLogicalName() + ".";
    }

    /**
     * Builds the topic name for a table: {@code <prefix><database>.<schema>.<table>}, where the
     * prefix already carries its own separator (see {@link #resolveTopicPrefix()}). This must match
     * the topic that CockroachDB produces for {@code topic_prefix=<prefix>} with {@code full_table_name}.
     */
    private String buildTopicName(TableId table) {
        return resolveTopicPrefix() + config.getDatabaseName() + "." + table.schema() + "." + table.table();
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
    /**
     * Extracts the table names a changefeed captures from its {@code SHOW CHANGEFEED JOBS}
     * description. The description echoes the creation statement in the form
     * {@code CREATE CHANGEFEED FOR TABLE a, TABLE b, ... INTO '...' WITH OPTIONS (...)}
     * (format captured from a live v25.4 cluster); quotes around individual name parts are
     * stripped.
     */
    static List<String> parseChangefeedTables(String description) {
        if (description == null) {
            return List.of();
        }
        int forIdx = description.indexOf(" FOR ");
        int intoIdx = description.indexOf(" INTO ");
        if (forIdx < 0 || intoIdx < 0 || intoIdx <= forIdx) {
            return List.of();
        }
        String tableList = description.substring(forIdx + 5, intoIdx);
        List<String> tables = new ArrayList<>();
        for (String token : tableList.split(",")) {
            String name = token.trim();
            if (name.regionMatches(true, 0, "TABLE ", 0, 6)) {
                name = name.substring(6).trim();
            }
            name = name.replace("\"", "");
            if (!name.isEmpty()) {
                tables.add(name);
            }
        }
        return tables;
    }

    /**
     * Returns the tables a changefeed captures that match none of the included identifiers.
     * A captured name matches an included identifier when they are equal or when one is a
     * dot-qualified suffix of the other, so a database-qualified job name matches a
     * schema-qualified include entry.
     */
    static List<String> findExtraCapturedTables(String description, Set<String> includedIdentifiers) {
        List<String> extras = new ArrayList<>();
        for (String captured : parseChangefeedTables(description)) {
            boolean matched = false;
            for (String included : includedIdentifiers) {
                if (captured.equals(included)
                        || captured.endsWith("." + included)
                        || included.endsWith("." + captured)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                extras.add(captured);
            }
        }
        return extras;
    }

    private static String sanitizeIdentifier(String identifier) {
        if (identifier == null) {
            return "";
        }
        return identifier.replaceAll("[^a-zA-Z0-9_.\\-]", "");
    }

    /**
     * Reads any configured sink TLS files, base64-encodes their contents, and
     * appends them as query parameters on the sink URI in the form expected by
     * the configured sink type. File-based values overwrite any same-named
     * parameter that was already present inline in the URI.
     *
     * <p>Currently only the Kafka sink is supported, where the parameters are
     * {@code ca_cert}, {@code client_cert}, {@code client_key}, and
     * {@code tls_enabled=true}. Other sink types pass through unchanged.
     */
    private String injectSinkTlsParams(String sinkUri) {
        if (!config.isChangefeedSinkTlsEnabled()) {
            return sinkUri;
        }
        if (!"kafka".equalsIgnoreCase(config.getChangefeedSinkType())) {
            return sinkUri;
        }
        String result = stripQueryParams(sinkUri, "ca_cert", "client_cert", "client_key", "tls_enabled");
        result = appendParam(result, "ca_cert", encodeTlsFile(config.getChangefeedSinkTlsCaCertFile()));
        result = appendParam(result, "client_cert", encodeTlsFile(config.getChangefeedSinkTlsClientCertFile()));
        result = appendParam(result, "client_key", encodeTlsFile(config.getChangefeedSinkTlsClientKeyFile()));
        result = appendParam(result, "tls_enabled", "true");
        return result;
    }

    private static String encodeTlsFile(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return null;
        }
        try {
            byte[] bytes = Files.readAllBytes(Path.of(filePath));
            String base64 = Base64.getEncoder().encodeToString(bytes);
            return URLEncoder.encode(base64, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to read sink TLS file: " + filePath, e);
        }
    }

    /**
     * Prefix for properties passed verbatim to the connector's changefeed {@link KafkaConsumer}.
     * Anything under {@code cockroachdb.changefeed.kafka.consumer.override.<kafka-property>} is applied
     * directly to the consumer (for example {@code ...override.security.protocol=SSL}), following the
     * same passthrough convention Debezium uses for its schema-history and signal Kafka clients. This
     * is the escape hatch for SASL, custom trust/key stores, or overriding any auto-derived value.
     */
    /**
     * Kafka SSL store type for inline PEM material. Kafka exposes constants for the SSL config keys
     * (see {@link SslConfigs}) but not for this store-type value, so it is named here. It lets the
     * consumer reuse the connector's PEM sink TLS files directly without converting them to a JKS store.
     */
    private static final String PEM_STORE_TYPE = "PEM";

    /**
     * Configures security for the connector's changefeed consumer. The consumer is a normal Kafka
     * client and is separate from the changefeed push to CockroachDB's Kafka sink, so it needs its
     * own security settings to read from a secured broker.
     *
     * <p>Two layers, applied in order so the passthrough wins:
     * <ol>
     * <li>If {@code cockroachdb.changefeed.sink.tls.*} is set (the same PEM material used to push the
     * changefeed over mTLS), derive {@code security.protocol=SSL} plus a PEM truststore (from the CA)
     * and, when a client cert and key are present, a PEM keystore. This makes the mTLS round-trip work
     * with no extra configuration.</li>
     * <li>Apply every {@code cockroachdb.changefeed.kafka.consumer.override.*} property verbatim, which
     * overrides the derived values and covers SASL, JKS stores, or a differently-secured broker.</li>
     * </ol>
     */
    static void applyConsumerSecurity(CockroachDBConnectorConfig config, Properties props) {
        if (config.isChangefeedSinkTlsEnabled()) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
            String caCert = config.getChangefeedSinkTlsCaCertFile();
            if (caCert != null && !caCert.isEmpty()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PEM_STORE_TYPE);
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, caCert);
            }
            String clientCert = config.getChangefeedSinkTlsClientCertFile();
            String clientKey = config.getChangefeedSinkTlsClientKeyFile();
            if (clientCert != null && !clientCert.isEmpty() && clientKey != null && !clientKey.isEmpty()) {
                props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, PEM_STORE_TYPE);
                props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, readFileContent(clientCert));
                props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, readFileContent(clientKey));
            }
            LOGGER.info("Derived SSL config for the changefeed consumer from cockroachdb.changefeed.sink.tls.* (security.protocol=SSL)");
        }

        Configuration overrides = config.getChangefeedKafkaConsumerOverrides();
        int applied = 0;
        for (String key : overrides.keys()) {
            props.put(key, overrides.getString(key));
            applied++;
        }
        if (applied > 0) {
            LOGGER.info("Applied {} '{}*' property/properties to the changefeed consumer", applied,
                    CockroachDBConnectorConfig.CHANGEFEED_KAFKA_CONSUMER_OVERRIDE_PREFIX);
        }
    }

    private static String readFileContent(String filePath) {
        try {
            return new String(Files.readAllBytes(Path.of(filePath)), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to read sink TLS file for the changefeed consumer: " + filePath, e);
        }
    }

    private static String appendParam(String uri, String key, String value) {
        if (value == null) {
            return uri;
        }
        String separator = uri.contains("?") ? "&" : "?";
        return uri + separator + key + "=" + value;
    }

    private static String stripQueryParams(String uri, String... keys) {
        int queryStart = uri.indexOf('?');
        if (queryStart < 0) {
            return uri;
        }
        String base = uri.substring(0, queryStart);
        String query = uri.substring(queryStart + 1);
        Set<String> toRemove = Set.of(keys);
        String kept = Arrays.stream(query.split("&"))
                .filter(pair -> !pair.isEmpty())
                .filter(pair -> {
                    int eq = pair.indexOf('=');
                    String name = eq < 0 ? pair : pair.substring(0, eq);
                    return !toRemove.contains(name);
                })
                .collect(Collectors.joining("&"));
        return kept.isEmpty() ? base : base + "?" + kept;
    }

    /**
     * Parses a CockroachDB resolved timestamp string into an {@link Instant}.
     *
     * <p>CockroachDB uses HLC (Hybrid Logical Clock) timestamps in the format
     * {@code <wall_time_nanos>.<logical_counter>}, e.g. {@code "1772695406971781718.0000000000"}.
     * The integer part is wall-clock time in <b>nanoseconds</b> since Unix epoch;
     * the fractional part is a logical counter (not sub-nanoseconds).</p>
     */
    static Instant parseResolvedTimestamp(String resolvedTs) {
        if (resolvedTs == null || resolvedTs.isEmpty()) {
            return Instant.EPOCH;
        }
        try {
            String[] parts = resolvedTs.split("\\.");
            long wallTimeNanos = Long.parseLong(parts[0]);
            long seconds = wallTimeNanos / 1_000_000_000L;
            long nanos = wallTimeNanos % 1_000_000_000L;
            return Instant.ofEpochSecond(seconds, nanos);
        }
        catch (NumberFormatException | ArithmeticException e) {
            LOGGER.warn("Unable to parse resolved timestamp '{}', using epoch", resolvedTs);
            return Instant.EPOCH;
        }
    }

    /**
     * Detects whether the incoming event's fields differ from the registered table schema.
     * Compares JSON field names in the event data against the table's column names.
     * Returns {@code true} if a new field is present or a registered column is absent.
     */
    static boolean hasSchemaChanged(JsonNode dataNode, io.debezium.relational.Table table) {
        // Check for new columns: event field not in registered schema
        Iterator<String> fieldNames = dataNode.fieldNames();
        int eventFieldCount = 0;
        while (fieldNames.hasNext()) {
            String field = fieldNames.next();
            eventFieldCount++;
            if (table.columnWithName(field) == null) {
                LOGGER.info("Schema change detected for table {}: new field '{}' in event",
                        table.id(), field);
                return true;
            }
        }

        // Check for dropped non-nullable columns: registered column absent from event
        for (io.debezium.relational.Column col : table.columns()) {
            if (!col.isOptional() && !dataNode.has(col.name())) {
                LOGGER.info("Schema change detected for table {}: registered non-nullable column '{}' absent from event",
                        table.id(), col.name());
                return true;
            }
        }

        return false;
    }

    /**
     * Refreshes the schema for a table by re-querying {@code information_schema}
     * through the persistent connection kept alive during streaming.
     */
    io.debezium.relational.Table refreshTableSchema(TableId tableId) {
        try {
            return doRefreshTableSchema(tableId);
        }
        catch (Exception first) {
            LOGGER.warn("Schema refresh for table {} failed ({}); reconnecting and retrying",
                    tableId, first.getMessage());
            try {
                reconnectSchemaRefreshConnection();
                return doRefreshTableSchema(tableId);
            }
            catch (Exception second) {
                // Never skip the event: fail the task so the error handler classifies the
                // SQLException in the cause chain as retriable and the restart replays the
                // event from the stored consumer position (debezium/dbz#2322).
                throw new DebeziumException("Schema refresh for table " + tableId
                        + " failed after reconnecting; failing the task so the event is replayed "
                        + "instead of skipped", second);
            }
        }
    }

    io.debezium.relational.Table doRefreshTableSchema(TableId tableId) throws Exception {
        if (schemaRefreshConnection == null) {
            throw new IllegalStateException("No connection available for schema refresh");
        }
        return schema.refreshTable(schemaRefreshConnection, tableId);
    }

    /**
     * Replaces the long-lived schema refresh connection. Load balancers and proxies close
     * idle connections underneath the connector, and a schema refresh may be the first use
     * in minutes, so a failed refresh reconnects before retrying.
     */
    void reconnectSchemaRefreshConnection() throws SQLException {
        CockroachDBConnection old = schemaRefreshConnection;
        if (old != null) {
            try {
                old.close();
            }
            catch (Exception e) {
                LOGGER.debug("Closing the stale schema refresh connection failed: {}", e.getMessage());
            }
        }
        CockroachDBConnection fresh = new CockroachDBConnection(config);
        fresh.connect();
        schemaRefreshConnection = fresh;
        LOGGER.info("Reconnected the schema refresh connection");
    }

    @Override
    public CockroachDBOffsetContext getOffsetContext() {
        return currentOffsetContext;
    }

    public void stop() {
        running.set(false);
        LOGGER.info("Stopping CockroachDB streaming change event source");
    }
}
