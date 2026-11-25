/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * Unit tests for multi-table changefeed support.
 *
 * @author Virag Tripathi
 */
public class CockroachDBMultiTableTest {

    private CockroachDBStreamingChangeEventSource createSource(Configuration config) {
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        return new CockroachDBStreamingChangeEventSource(connectorConfig, null, null, null, null);
    }

    private Configuration baseConfig() {
        return Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "cockroachdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.enriched.properties", "source,schema")
                .with("cockroachdb.changefeed.resolved.interval", "10s")
                .build();
    }

    @Test
    public void shouldBuildSingleTableChangefeedQuery() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).startsWith("CREATE CHANGEFEED FOR TABLE ");
        assertThat(query).contains("\"testdb\".\"public\".\"orders\"");
        assertThat(query).contains("INTO 'kafka://kafka:9092?topic_prefix=cockroachdb.'");
        assertThat(query).contains("full_table_name");
        assertThat(query).contains("envelope = 'enriched'");
        assertThat(query).contains("resolved = '10s'");
        // Single table: the FOR clause should have exactly one table (no comma before INTO)
        String tablesPart = query.substring(
                query.indexOf("FOR TABLE ") + "FOR TABLE ".length(),
                query.indexOf(" INTO "));
        assertThat(tablesPart.split(",")).hasSize(1);
    }

    @Test
    public void shouldNameTheSinkUriWhenAChangefeedHasToBeCreatedWithoutIt() {
        // The connector runs without the property when it reuses an existing changefeed and takes the Kafka
        // bootstrap servers from their own property. To create a changefeed it needs the sink.
        Configuration config = baseConfig().edit()
                .without(CockroachDBConnectorConfig.CHANGEFEED_SINK_URI.name())
                .with(CockroachDBConnectorConfig.CHANGEFEED_KAFKA_BOOTSTRAP_SERVERS, "kafka:9092")
                .build();
        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(new TableId("testdb", "public", "orders"));

        assertThatThrownBy(() -> source.buildSinkChangefeedQuery(tables, null, false))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(CockroachDBConnectorConfig.CHANGEFEED_SINK_URI.name())
                .hasMessageContaining("testdb.public.orders");
    }

    @Test
    public void shouldWriteEveryChangefeedOptionPropertyIntoTheStatement() {
        // The connector quotes the value. A property without a value gives an option without one.
        Configuration config = baseConfig().edit()
                .with(CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + "min_checkpoint_frequency", "10s")
                .with(CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + "protect_data_from_gc_on_pause", "")
                .build();
        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(new TableId("testdb", "public", "orders"));

        assertThat(source.buildSinkChangefeedQuery(tables, null, false))
                .contains(" WITH full_table_name, min_checkpoint_frequency = '10s', protect_data_from_gc_on_pause, envelope = 'enriched'");
        assertThat(source.buildSinklessChangefeedQuery(tables, null, false))
                .contains(" WITH min_checkpoint_frequency = '10s', protect_data_from_gc_on_pause, envelope = 'enriched'");
    }

    @Test
    public void shouldEscapeAQuoteInTheValueOfAChangefeedOptionProperty() {
        Configuration config = baseConfig().edit()
                .with(CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + "metrics_label", "it's")
                .build();
        List<TableId> tables = Collections.singletonList(new TableId("testdb", "public", "orders"));

        assertThat(createSource(config).buildSinkChangefeedQuery(tables, null, false)).contains("metrics_label = 'it''s'");
    }

    @Test
    public void shouldWriteTheValueOfAChangefeedOptionPropertyWithoutTheSpacesAroundIt() {
        // A blank value gives an option without a value. CockroachDB takes a backslash in a quoted value as it
        // is, so only the quote is escaped.
        Configuration config = baseConfig().edit()
                .with(CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + "min_checkpoint_frequency", " 10s ")
                .with(CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + "protect_data_from_gc_on_pause", "  ")
                .with(CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + "metrics_label", "a\\")
                .build();
        List<TableId> tables = Collections.singletonList(new TableId("testdb", "public", "orders"));

        assertThat(createSource(config).buildSinklessChangefeedQuery(tables, null, false))
                .contains(" WITH metrics_label = 'a\\', min_checkpoint_frequency = '10s', protect_data_from_gc_on_pause, envelope = 'enriched'");
    }

    @Test
    public void shouldPutTheChangefeedOptionPropertiesAfterTheDeprecatedOptions() {
        // When an option is given twice, CockroachDB uses the last value. A property therefore wins over the
        // deprecated list of options, and an option that the connector sets itself wins over both.
        Configuration config = baseConfig().edit()
                .with(CockroachDBConnectorConfig.CHANGEFEED_SINK_OPTIONS, "min_checkpoint_frequency = '10s'")
                .with(CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + "min_checkpoint_frequency", "30s")
                .build();
        List<TableId> tables = Collections.singletonList(new TableId("testdb", "public", "orders"));

        assertThat(createSource(config).buildSinkChangefeedQuery(tables, null, false))
                .contains(" WITH full_table_name, min_checkpoint_frequency = '10s', min_checkpoint_frequency = '30s', envelope = 'enriched'");
    }

    @Test
    public void shouldRefuseAChangefeedOptionPropertyForAnOptionThatIsReservedForTheConnector() {
        // The option, and what the message has to name for it: the property that sets the option where there is one.
        final Map<String, String> reserved = Map.ofEntries(
                Map.entry("envelope", "enriched envelope"),
                Map.entry("format", "JSON"),
                Map.entry("full_table_name", "topics"),
                Map.entry("enriched_properties", CockroachDBConnectorConfig.CHANGEFEED_ENRICHED_PROPERTIES.name()),
                Map.entry("updated", CockroachDBConnectorConfig.CHANGEFEED_INCLUDE_UPDATED.name()),
                Map.entry("diff", CockroachDBConnectorConfig.CHANGEFEED_INCLUDE_DIFF.name()),
                Map.entry("mvcc_timestamp", CockroachDBConnectorConfig.CHANGEFEED_INCLUDE_MVCC_TIMESTAMP.name()),
                Map.entry("resolved", CockroachDBConnectorConfig.CHANGEFEED_RESOLVED_INTERVAL.name()),
                Map.entry("cursor", CockroachDBConnectorConfig.CHANGEFEED_CURSOR.name()),
                Map.entry("initial_scan", CockroachDBConnectorConfig.SNAPSHOT_MODE.name()),
                Map.entry("initial_scan_only", CockroachDBConnectorConfig.SNAPSHOT_MODE.name()),
                Map.entry("no_initial_scan", CockroachDBConnectorConfig.SNAPSHOT_MODE.name()),
                Map.entry("kafka_sink_config", CockroachDBConnectorConfig.CHANGEFEED_KAFKA_SINK_CONFIG.name()));

        reserved.forEach((option, hint) -> {
            final String property = CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + option;
            // The property is refused when the task starts, and not only when a statement is built: a changefeed
            // that exists already is reused, and no statement is built for it.
            assertThatThrownBy(() -> createSource(baseConfig().edit().with(property, "x").build()), "property %s", property)
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining(property)
                    .hasMessageContaining("reserved for the connector")
                    .hasMessageContaining(hint);
        });
    }

    @Test
    public void shouldRefuseAReservedChangefeedOptionInAnyCase() {
        // CockroachDB does not distinguish the case of an option name.
        final String property = CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + "ENVELOPE";

        assertThatThrownBy(() -> createSource(baseConfig().edit().with(property, "wrapped").build()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(property)
                .hasMessageContaining("reserved for the connector");
    }

    @Test
    public void shouldReserveEveryOptionThatTheConnectorWrites() {
        Configuration everything = baseConfig().edit()
                .with(CockroachDBConnectorConfig.CHANGEFEED_INCLUDE_UPDATED, true)
                .with(CockroachDBConnectorConfig.CHANGEFEED_INCLUDE_DIFF, true)
                .with(CockroachDBConnectorConfig.CHANGEFEED_INCLUDE_MVCC_TIMESTAMP, true)
                .with(CockroachDBConnectorConfig.CHANGEFEED_KAFKA_SINK_CONFIG, "{\"Flush\": {\"Messages\": 100}}")
                .build();
        CockroachDBStreamingChangeEventSource source = createSource(everything);
        List<TableId> tables = Collections.singletonList(new TableId("testdb", "public", "orders"));
        Set<String> written = new TreeSet<>();
        written.addAll(optionNames(source.buildSinkChangefeedQuery(tables, null, false)));
        written.addAll(optionNames(source.buildSinkChangefeedQuery(tables, "1700000000000000000.0000000000", true)));
        written.addAll(optionNames(source.buildSinklessChangefeedQuery(tables, null, false)));

        assertThat(written).contains("full_table_name", "envelope", "enriched_properties", "updated", "diff", "mvcc_timestamp",
                "resolved", "initial_scan", "cursor", "kafka_sink_config");
        for (String option : written) {
            final String property = CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + option;
            assertThatThrownBy(() -> createSource(baseConfig().edit().with(property, "x").build()), "property %s", property)
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining("reserved for the connector");
        }
    }

    /** The names of the options in the WITH clause of a statement. A comma in a quoted value separates nothing. */
    private static Set<String> optionNames(String statement) {
        final String options = statement.substring(statement.indexOf(" WITH ") + " WITH ".length()) + ",";
        final Set<String> names = new TreeSet<>();
        final StringBuilder option = new StringBuilder();
        boolean quoted = false;
        for (char current : options.toCharArray()) {
            if (current == '\'') {
                quoted = !quoted;
            }
            if (current == ',' && !quoted) {
                names.add(option.toString().split("=", 2)[0].trim());
                option.setLength(0);
            }
            else {
                option.append(current);
            }
        }
        return names;
    }

    @Test
    public void shouldRefuseAChangefeedOptionPropertyWhoseNameIsNoOptionName() {
        // The name goes into the statement as it is, so it must be a plain option name. The empty name is the
        // property that consists of the prefix alone.
        for (String name : List.of("min_checkpoint_frequency = '1s', envelope", "", "min-checkpoint-frequency", "1s", "metrics label",
                " metrics_label", "metrics_label\n", "m\u00e9trique")) {
            final String property = CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + name;
            // From a map, as Kafka Connect passes the properties: the name is taken as it is.
            final Map<String, String> properties = baseConfig().asMap();
            properties.put(property, "x");

            assertThatThrownBy(() -> createSource(Configuration.from(properties)), "property %s", property)
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining(property)
                    .hasMessageContaining("does not name a changefeed option");
        }
    }

    @Test
    public void shouldLeaveSplitColumnFamiliesOutOfTheStatement() {
        // With this option CockroachDB sends one event per column family for a table with more than one, with
        // the columns of that family only, and the connector reads every event as a complete row. The rest of
        // the list goes into the statement as it is written. A plain quoted value ends at its quote, also after
        // a backslash, and a value with the prefix e takes a backslash in front of a quote as an escape.
        final Map<String, String> listAndWhatIsLeft = Map.ofEntries(
                Map.entry("split_column_families", ""),
                Map.entry("min_checkpoint_frequency = '10s', split_column_families", "min_checkpoint_frequency = '10s', "),
                Map.entry("split_column_families, min_checkpoint_frequency = '10s'", "min_checkpoint_frequency = '10s', "),
                Map.entry(" SPLIT_COLUMN_FAMILIES , protect_data_from_gc_on_pause", "protect_data_from_gc_on_pause, "),
                Map.entry("\"split_column_families\"", ""),
                Map.entry("'split_column_families'", ""),
                Map.entry("metrics_label = 'a, b', split_column_families", "metrics_label = 'a, b', "),
                Map.entry("metrics_label = 'a\\', split_column_families", "metrics_label = 'a\\', "),
                Map.entry("metrics_label = e'a\\'b', split_column_families", "metrics_label = e'a\\'b', "),
                Map.entry("metrics_label = 'it''s', split_column_families, diff", "metrics_label = 'it''s', diff, "));
        List<TableId> tables = Collections.singletonList(new TableId("testdb", "public", "orders"));

        listAndWhatIsLeft.forEach((sinkOptions, left) -> {
            Configuration config = baseConfig().edit().with(CockroachDBConnectorConfig.CHANGEFEED_SINK_OPTIONS, sinkOptions).build();

            assertThat(createSource(config).buildSinklessChangefeedQuery(tables, null, false)).as("options %s", sinkOptions)
                    .contains(" WITH " + left + "envelope = 'enriched'")
                    .doesNotContainIgnoringCase("split_column_families");
        });
    }

    @Test
    public void shouldWarnThatSplitColumnFamiliesIsLeftOut() {
        final Logger logger = ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        logger.addAppender(log);
        try {
            createSource(baseConfig().edit().with(CockroachDBConnectorConfig.CHANGEFEED_SINK_OPTIONS, "min_checkpoint_frequency = '10s'").build());
            assertThat(log.list).noneMatch(event -> event.getFormattedMessage().contains("split_column_families"));

            createSource(baseConfig().edit()
                    .with(CockroachDBConnectorConfig.CHANGEFEED_SINK_OPTIONS, "min_checkpoint_frequency = '10s', split_column_families").build());
            assertThat(log.list).filteredOn(event -> event.getLevel() == Level.WARN)
                    .anyMatch(event -> event.getFormattedMessage().contains(CockroachDBConnectorConfig.CHANGEFEED_SINK_OPTIONS.name())
                            && event.getFormattedMessage().contains("split_column_families")
                            && event.getFormattedMessage().contains("more than one column family"));
        }
        finally {
            logger.detachAppender(log);
        }
    }

    @Test
    public void shouldNotTakeAQuotedValueForTheOptionSplitColumnFamilies() {
        List<TableId> tables = Collections.singletonList(new TableId("testdb", "public", "orders"));
        for (String sinkOptions : List.of(
                "metrics_label = 'a, split_column_families, b'",
                "metrics_label = e'a\\', split_column_families, b'",
                "metrics_label = e'a''b\\', split_column_families, c'",
                "metrics_label = 'it''s, split_column_families'",
                "metrics_label = 'split_column_families'",
                "min_checkpoint_frequency = '10s', protect_data_from_gc_on_pause")) {
            Configuration config = baseConfig().edit().with(CockroachDBConnectorConfig.CHANGEFEED_SINK_OPTIONS, sinkOptions).build();

            // The list goes into the statement as it is written.
            assertThat(createSource(config).buildSinklessChangefeedQuery(tables, null, false)).contains(" WITH " + sinkOptions + ", ");
        }
    }

    @Test
    public void shouldRemoveOnlyTheOptionSplitColumnFamiliesFromTheSinkOptions() {
        // A list without the option is returned as it is.
        for (String sinkOptions : List.of(
                "min_checkpoint_frequency = '10s', protect_data_from_gc_on_pause",
                // The prefix E also makes a backslash an escape.
                "metrics_label = E'a\\', split_column_families, b', diff",
                // A comma in a quoted identifier separates nothing either.
                "metrics_label = \"a, split_column_families, b\", diff",
                // A quote that never ends takes the rest of the list.
                "metrics_label = 'a, split_column_families",
                "  ",
                "diff,")) {
            assertThat(CockroachDBStreamingChangeEventSource.withoutSplitColumnFamilies(sinkOptions)).isSameAs(sinkOptions);
        }
        // A name in upper case or in quotes, and the option with a value that it does not take.
        assertThat(CockroachDBStreamingChangeEventSource.withoutSplitColumnFamilies("Split_Column_Families = 'x', \"Quoted_Name\""))
                .isEqualTo(" \"Quoted_Name\"");
        // Options on several lines.
        assertThat(CockroachDBStreamingChangeEventSource.withoutSplitColumnFamilies("min_checkpoint_frequency = '10s',\n  split_column_families\n"))
                .isEqualTo("min_checkpoint_frequency = '10s'");
        // The option more than once, and a comma at the end.
        assertThat(CockroachDBStreamingChangeEventSource.withoutSplitColumnFamilies("split_column_families, diff, split_column_families,"))
                .isEqualTo(" diff,");
        // What follows the option stays in the list, also a value that never ends.
        assertThat(CockroachDBStreamingChangeEventSource.withoutSplitColumnFamilies("split_column_families, metrics_label = e'a\\"))
                .isEqualTo(" metrics_label = e'a\\");
    }

    @Test
    public void shouldRefuseAChangefeedOptionPropertyForSplitColumnFamilies() {
        for (String name : List.of("split_column_families", "Split_Column_Families")) {
            final String property = CockroachDBConnectorConfig.CHANGEFEED_OPTION_PREFIX + name;

            assertThatThrownBy(() -> createSource(baseConfig().edit().with(property, "").build()), "property %s", property)
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining(property)
                    .hasMessageContaining("more than one column family");
        }
    }

    @Test
    public void shouldBuildMultiTableChangefeedQuery() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Arrays.asList(
                new TableId("testdb", "public", "orders"),
                new TableId("testdb", "public", "customers"),
                new TableId("testdb", "public", "products"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).startsWith("CREATE CHANGEFEED FOR TABLE ");
        assertThat(query).contains("\"testdb\".\"public\".\"orders\"");
        assertThat(query).contains("\"testdb\".\"public\".\"customers\"");
        assertThat(query).contains("\"testdb\".\"public\".\"products\"");
        // All three tables should be in a single FOR clause separated by commas
        String tablesPart = query.substring(
                query.indexOf("FOR TABLE ") + "FOR TABLE ".length(),
                query.indexOf(" INTO "));
        assertThat(tablesPart.split(",")).hasSize(3);
    }

    @Test
    public void shouldIncludeInitialScanInMultiTableQuery() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Arrays.asList(
                new TableId("testdb", "public", "orders"),
                new TableId("testdb", "public", "customers"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).contains("initial_scan = 'yes'");
    }

    @Test
    public void shouldSkipInitialScanWithPriorOffset() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Arrays.asList(
                new TableId("testdb", "public", "orders"),
                new TableId("testdb", "public", "customers"));

        String query = source.buildSinkChangefeedQuery(tables, "1234567890.0000000000", true);

        assertThat(query).contains("initial_scan = 'no'");
        assertThat(query).contains("cursor = '1234567890.0000000000'");
    }

    @Test
    public void shouldIncludeDiffAndUpdatedOptions() {
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "cockroachdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.enriched.properties", "source,schema")
                .with("cockroachdb.changefeed.resolved.interval", "10s")
                .with("cockroachdb.changefeed.include.diff", "true")
                .with("cockroachdb.changefeed.include.updated", "true")
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Arrays.asList(
                new TableId("testdb", "public", "orders"),
                new TableId("testdb", "public", "customers"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).contains(", diff");
        assertThat(query).contains(", updated");
    }

    @Test
    public void shouldBuildQueryWithSnapshotModeAlways() {
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "cockroachdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.resolved.interval", "10s")
                .with("snapshot.mode", "always")
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Arrays.asList(
                new TableId("testdb", "public", "orders"),
                new TableId("testdb", "public", "customers"));

        // Even with prior offset, always mode should use initial_scan='yes'
        String query = source.buildSinkChangefeedQuery(tables, "1234567890.0000000000", true);

        assertThat(query).contains("initial_scan = 'yes'");
    }

    @Test
    public void shouldBuildQueryWithNoDataMode() {
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "cockroachdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.resolved.interval", "10s")
                .with("snapshot.mode", "no_data")
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).contains("initial_scan = 'no'");
    }

    @Test
    public void shouldIncludeTopicPrefixAndFullTableNameInQuery() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Arrays.asList(
                new TableId("testdb", "public", "orders"),
                new TableId("testdb", "public", "customers"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        // Multi-table changefeed uses topic_prefix in URI and full_table_name in WITH clause
        // so topics are named: prefix.db.schema.table
        assertThat(query).contains("topic_prefix=cockroachdb.");
        assertThat(query).contains("full_table_name");
        // Should NOT have topic_name= (that's for single-topic override)
        assertThat(query).doesNotContain("topic_name=");
    }

    @Test
    public void shouldUseTopicPrefixAsFallbackWhenSinkTopicPrefixEmpty() {
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "myapp")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.resolved.interval", "10s")
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).contains("topic_prefix=myapp.");
        assertThat(query).doesNotContain("topic_prefix=cockroachdb.");
    }

    @Test
    public void shouldUseExplicitSinkTopicPrefixVerbatim() {
        // An explicitly set sink topic prefix is used verbatim (no separator is appended), so the
        // user controls the separator. Here "custom" yields topic_prefix=custom (not custom.).
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "myapp")
                .with("cockroachdb.changefeed.sink.topic.prefix", "custom")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.resolved.interval", "10s")
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).contains("topic_prefix=custom");
        assertThat(query).doesNotContain("topic_prefix=custom.");
        assertThat(query).doesNotContain("myapp");
    }

    @Test
    public void shouldUseSinkTopicPrefixWithCustomSeparatorVerbatim() {
        // A prefix that carries its own separator (e.g. a dash convention) is preserved exactly,
        // so the topic becomes <prefix><database>.<schema>.<table> with no connector-added dot.
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "bankdb")
                .with("topic.prefix", "myapp")
                .with("cockroachdb.changefeed.sink.topic.prefix", "env-prod-")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.resolved.interval", "10s")
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        String query = source.buildSinkChangefeedQuery(
                Collections.singletonList(new TableId("bankdb", "public", "orders")), null, false);

        assertThat(query).contains("topic_prefix=env-prod-");
        assertThat(query).doesNotContain("topic_prefix=env-prod-.");
    }

    @Test
    public void shouldOmitCursorClauseForSentinelValues() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String queryInitial = source.buildSinkChangefeedQuery(tables, CockroachDBOffsetContext.CURSOR_INITIAL, false);
        assertThat(queryInitial).doesNotContain("cursor =");

        String queryNow = source.buildSinkChangefeedQuery(tables, CockroachDBOffsetContext.CURSOR_NOW, false);
        assertThat(queryNow).doesNotContain("cursor =");
    }

    @Test
    public void shouldIncludeCursorClauseForRealTimestamp() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, "1234567890.0000000000", true);

        assertThat(query).contains("cursor = '1234567890.0000000000'");
    }

    @Test
    public void shouldNotInjectTlsParamsWhenNoFilesConfigured() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).doesNotContain("ca_cert=");
        assertThat(query).doesNotContain("client_cert=");
        assertThat(query).doesNotContain("client_key=");
        assertThat(query).doesNotContain("tls_enabled=");
    }

    @Test
    public void shouldInjectCaCertFromFile(@TempDir Path tmp) throws Exception {
        Path caCert = tmp.resolve("ca.pem");
        Files.writeString(caCert, "ca-cert-bytes");
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "cockroachdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9093")
                .with("cockroachdb.changefeed.enriched.properties", "source,schema")
                .with("cockroachdb.changefeed.resolved.interval", "10s")
                .with("cockroachdb.changefeed.sink.tls.ca.cert.file", caCert.toString())
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        String expectedB64 = URLEncoder.encode(
                Base64.getEncoder().encodeToString("ca-cert-bytes".getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);
        assertThat(query).contains("ca_cert=" + expectedB64);
        assertThat(query).contains("tls_enabled=true");
        assertThat(query).doesNotContain("client_cert=");
        assertThat(query).doesNotContain("client_key=");
    }

    @Test
    public void shouldInjectAllThreeTlsFilesAndTlsEnabled(@TempDir Path tmp) throws Exception {
        Path ca = tmp.resolve("ca.pem");
        Path clientCert = tmp.resolve("client.crt");
        Path clientKey = tmp.resolve("client.key");
        Files.writeString(ca, "ca");
        Files.writeString(clientCert, "cert");
        Files.writeString(clientKey, "key");

        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "cockroachdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9093")
                .with("cockroachdb.changefeed.sink.tls.ca.cert.file", ca.toString())
                .with("cockroachdb.changefeed.sink.tls.client.cert.file", clientCert.toString())
                .with("cockroachdb.changefeed.sink.tls.client.key.file", clientKey.toString())
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).contains("ca_cert=" + urlEncodedB64("ca"));
        assertThat(query).contains("client_cert=" + urlEncodedB64("cert"));
        assertThat(query).contains("client_key=" + urlEncodedB64("key"));
        assertThat(query).contains("tls_enabled=true");
    }

    @Test
    public void shouldOverrideInlineSinkUriParamsWithFileValues(@TempDir Path tmp) throws Exception {
        Path ca = tmp.resolve("ca.pem");
        Files.writeString(ca, "file-ca");

        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "cockroachdb")
                .with("cockroachdb.changefeed.sink.uri",
                        "kafka://kafka:9093?ca_cert=INLINE_BAD&tls_enabled=false&compression=gzip")
                .with("cockroachdb.changefeed.sink.tls.ca.cert.file", ca.toString())
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        // Inline values for managed params are replaced
        assertThat(query).doesNotContain("ca_cert=INLINE_BAD");
        assertThat(query).doesNotContain("tls_enabled=false");
        // File-based ca_cert and derived tls_enabled=true take their place
        assertThat(query).contains("ca_cert=" + urlEncodedB64("file-ca"));
        assertThat(query).contains("tls_enabled=true");
        // Unrelated inline params are preserved
        assertThat(query).contains("compression=gzip");
    }

    @Test
    public void shouldPreserveExistingQuerySeparatorWhenAppendingTlsParams(@TempDir Path tmp) throws Exception {
        Path ca = tmp.resolve("ca.pem");
        Files.writeString(ca, "ca");

        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "cockroachdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9093?compression=gzip")
                .with("cockroachdb.changefeed.sink.tls.ca.cert.file", ca.toString())
                .build();

        CockroachDBStreamingChangeEventSource source = createSource(config);
        List<TableId> tables = Collections.singletonList(
                new TableId("testdb", "public", "orders"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        // No double '?', no duplicated separators
        String into = query.substring(query.indexOf("INTO '") + "INTO '".length(), query.indexOf("' WITH"));
        assertThat(into.chars().filter(c -> c == '?').count()).isEqualTo(1L);
        assertThat(into).doesNotContain("??");
        assertThat(into).doesNotContain("&&");
        assertThat(into).contains("compression=gzip");
        assertThat(into).contains("ca_cert=");
        assertThat(into).contains("tls_enabled=true");
        assertThat(into).contains("topic_prefix=");
    }

    private static String urlEncodedB64(String content) {
        return URLEncoder.encode(
                Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);
    }

    @Test
    public void shouldKeepAllTablesInOneGroupWhenMaxIsZero() {
        List<TableId> tables = List.of(
                new TableId("db", "public", "t1"),
                new TableId("db", "public", "t2"),
                new TableId("db", "public", "t3"));

        List<List<TableId>> groups = CockroachDBStreamingChangeEventSource.partitionTables(tables, 0);

        assertThat(groups).hasSize(1);
        assertThat(groups.get(0)).containsExactlyElementsOf(tables);
    }

    @Test
    public void shouldKeepAllTablesInOneGroupWhenCountUnderMax() {
        List<TableId> tables = List.of(
                new TableId("db", "public", "t1"),
                new TableId("db", "public", "t2"));

        List<List<TableId>> groups = CockroachDBStreamingChangeEventSource.partitionTables(tables, 5);

        assertThat(groups).hasSize(1);
        assertThat(groups.get(0)).containsExactlyElementsOf(tables);
    }

    @Test
    public void shouldSplitTablesIntoEvenChunks() {
        List<TableId> tables = List.of(
                new TableId("db", "public", "t1"),
                new TableId("db", "public", "t2"),
                new TableId("db", "public", "t3"),
                new TableId("db", "public", "t4"));

        List<List<TableId>> groups = CockroachDBStreamingChangeEventSource.partitionTables(tables, 2);

        assertThat(groups).hasSize(2);
        assertThat(groups.get(0)).hasSize(2);
        assertThat(groups.get(1)).hasSize(2);
    }

    @Test
    public void shouldSplitTablesWithRemainderChunk() {
        List<TableId> tables = List.of(
                new TableId("db", "public", "t1"),
                new TableId("db", "public", "t2"),
                new TableId("db", "public", "t3"),
                new TableId("db", "public", "t4"),
                new TableId("db", "public", "t5"));

        List<List<TableId>> groups = CockroachDBStreamingChangeEventSource.partitionTables(tables, 2);

        assertThat(groups).hasSize(3);
        assertThat(groups.get(0)).hasSize(2);
        assertThat(groups.get(1)).hasSize(2);
        assertThat(groups.get(2)).hasSize(1);
        // Every table is present exactly once across all groups.
        assertThat(groups.stream().flatMap(List::stream)).containsExactlyElementsOf(tables);
    }
}
