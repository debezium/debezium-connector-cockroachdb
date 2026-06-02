/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
