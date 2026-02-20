/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

/**
 * Unit tests for multi-table changefeed support.
 *
 * @author Virag Tripathi
 */
public class CockroachDBMultiTableTest {

    private CockroachDBStreamingChangeEventSource createSource(Configuration config) {
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        return new CockroachDBStreamingChangeEventSource(connectorConfig, null, null, null);
    }

    private Configuration baseConfig() {
        return Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("topic.prefix", "crdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.envelope", "enriched")
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
        assertThat(query).contains("testdb.public.orders");
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
    public void shouldBuildMultiTableChangefeedQuery() {
        CockroachDBStreamingChangeEventSource source = createSource(baseConfig());
        List<TableId> tables = Arrays.asList(
                new TableId("testdb", "public", "orders"),
                new TableId("testdb", "public", "customers"),
                new TableId("testdb", "public", "products"));

        String query = source.buildSinkChangefeedQuery(tables, null, false);

        assertThat(query).startsWith("CREATE CHANGEFEED FOR TABLE ");
        assertThat(query).contains("testdb.public.orders");
        assertThat(query).contains("testdb.public.customers");
        assertThat(query).contains("testdb.public.products");
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
                .with("topic.prefix", "crdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.envelope", "enriched")
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
                .with("topic.prefix", "crdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.envelope", "enriched")
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
                .with("topic.prefix", "crdb")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("cockroachdb.changefeed.envelope", "enriched")
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
}
