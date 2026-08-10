/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

/**
 * Unit tests for the sinkless changefeed delivery mode (cockroachdb.changefeed.sink.type=sinkless):
 * the no-INTO changefeed query and the source-block table routing.
 *
 * @author Virag Tripathi
 */
public class CockroachDBSinklessTest {

    private CockroachDBStreamingChangeEventSource createSource(Configuration config) {
        return new CockroachDBStreamingChangeEventSource(new CockroachDBConnectorConfig(config), null, null, null, null);
    }

    private Configuration.Builder sinklessConfig() {
        return Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "testdb")
                .with("database.server.name", "test")
                .with("topic.prefix", "crdb")
                .with("cockroachdb.changefeed.sink.type", "sinkless")
                .with("cockroachdb.changefeed.include.diff", "true")
                .with("cockroachdb.changefeed.resolved.interval", "10s");
    }

    @Test
    public void shouldBuildSinklessQueryWithoutSinkClauses() {
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().build());
        String query = source.buildSinklessChangefeedQuery(
                List.of(new TableId("testdb", "public", "orders")), null, false);

        assertThat(query).startsWith("CREATE CHANGEFEED FOR TABLE");
        assertThat(query).contains("envelope = 'enriched'");
        assertThat(query).contains("resolved = '10s'");
        assertThat(query).contains("diff");
        // No sink-specific clauses for a sinkless changefeed.
        assertThat(query).doesNotContain("INTO");
        assertThat(query).doesNotContain("topic_prefix");
        assertThat(query).doesNotContain("full_table_name");
    }

    @Test
    public void shouldIncludeCursorWhenResuming() {
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().build());
        String query = source.buildSinklessChangefeedQuery(
                List.of(new TableId("testdb", "public", "orders")), "1700000000000000000.0000000000", true);
        assertThat(query).contains("cursor = '1700000000000000000.0000000000'");
    }

    @Test
    public void shouldSupportMultipleTablesInOneSinklessChangefeed() {
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().build());
        String query = source.buildSinklessChangefeedQuery(
                List.of(new TableId("testdb", "public", "orders"),
                        new TableId("testdb", "inventory", "warehouse_items")),
                null, false);
        assertThat(query).contains("public.orders");
        assertThat(query).contains("inventory.warehouse_items");
    }

    @Test
    public void shouldRollbackAndRetrySinklessQueryForWhenNeeded() throws Exception {
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig()
                .with("snapshot.mode", "when_needed")
                .build());
        Statement statement = mock(Statement.class);
        Connection connection = mock(Connection.class);
        ResultSet recoveredResult = mock(ResultSet.class);
        SQLException staleCursor = new SQLException(
                "batch timestamp 1 must be after replica GC threshold 2", "XXUUU");
        when(statement.executeQuery(anyString())).thenThrow(staleCursor).thenReturn(recoveredResult);

        ResultSet result = source.executeSinklessChangefeed(
                statement, connection, List.of(new TableId("testdb", "public", "orders")), "1.0000000000", true);

        assertThat(result).isSameAs(recoveredResult);
        verify(connection).rollback();
        org.mockito.ArgumentCaptor<String> queries = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(statement, times(2)).executeQuery(queries.capture());
        assertThat(queries.getAllValues().get(0)).contains("cursor = '1.0000000000'");
        assertThat(queries.getAllValues().get(1))
                .contains("initial_scan = 'yes'")
                .doesNotContain("cursor =");
    }

    @Test
    public void shouldResolveTableFromSourceBlockBySchemaAndName() throws Exception {
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().build());
        Map<String, TableId> map = new HashMap<>();
        TableId publicOrders = new TableId("testdb", "public", "orders");
        TableId inventoryOrders = new TableId("testdb", "inventory", "orders");
        map.put("public.orders", publicOrders);
        map.put("inventory.orders", inventoryOrders);

        String publicEvent = "{\"after\":{\"id\":1},\"op\":\"c\",\"source\":{\"schema_name\":\"public\",\"table_name\":\"orders\"}}";
        String inventoryEvent = "{\"after\":{\"id\":1},\"op\":\"c\",\"source\":{\"schema_name\":\"inventory\",\"table_name\":\"orders\"}}";

        // Same table name, different schema: must route to the correct TableId.
        assertThat(source.resolveTableFromSource(publicEvent, map)).isEqualTo(publicOrders);
        assertThat(source.resolveTableFromSource(inventoryEvent, map)).isEqualTo(inventoryOrders);
    }

    @Test
    public void shouldReturnNullForRowsWithoutSource() throws Exception {
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().build());
        Map<String, TableId> map = new HashMap<>();
        map.put("public.orders", new TableId("testdb", "public", "orders"));

        // Resolved-timestamp rows have no source block; routing returns null (handled by caller).
        assertThat(source.resolveTableFromSource("{\"resolved\":\"1700000000000000000.0000000000\"}", map)).isNull();
        // Unknown table is not in the map.
        assertThat(source.resolveTableFromSource(
                "{\"op\":\"c\",\"source\":{\"schema_name\":\"public\",\"table_name\":\"unknown\"}}", map)).isNull();
    }
}
