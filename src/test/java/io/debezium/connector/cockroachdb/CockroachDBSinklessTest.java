/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.EventDispatcher;
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
        assertThat(query).contains("\"public\".\"orders\"");
        assertThat(query).contains("\"inventory\".\"warehouse_items\"");
    }

    @Test
    public void shouldQuoteIdentifiersThatRequireQuoting() {
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().build());
        String query = source.buildSinklessChangefeedQuery(
                List.of(new TableId("test.db", "stock-trading", "ord\"er")), null, false);

        assertThat(query).contains("FOR TABLE \"test.db\".\"stock-trading\".\"ord\"\"er\" WITH");
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
    public void shouldNameTheSnapshotModeThatScansAgainWhenTheStoredCursorIsRejected() throws Exception {
        // The text as CockroachDB v25.4.21 and v26.4.1 return it for a sinkless changefeed.
        SQLException tooOld = new SQLException("ERROR: could not create changefeed: cursor 1795332885584348201.0000000000 is older than "
                + "the GC threshold 1795332894103672556: batch timestamp 1795332885.584348201,0 must be after replica GC threshold "
                + "1795332894.103672556,0 (r79: /{Table/106-Max})\n  Hint: use a more recent cursor", "XXUUU");
        Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenThrow(tooOld);
        List<TableId> tables = List.of(new TableId("testdb", "public", "orders"));

        // With a stored cursor and a snapshot mode that does not scan again, the error says which mode does.
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().with("snapshot.mode", "initial").build());
        assertThatThrownBy(() -> source.executeSinklessChangefeed(statement, mock(Connection.class), tables, "1795332885584348201.0000000000", true))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("must be after replica GC threshold")
                .hasMessageContaining("snapshot mode when_needed")
                .hasCause(tooOld);
        // Without a stored cursor the error is passed on as it is.
        CockroachDBStreamingChangeEventSource firstStart = createSource(sinklessConfig().with("snapshot.mode", "no_data").build());
        assertThatThrownBy(() -> firstStart.executeSinklessChangefeed(statement, mock(Connection.class), tables, null, false))
                .isSameAs(tooOld);
    }

    /** The error with which a changefeed ends when the data that it reads has been garbage-collected. */
    private static final String TOO_OLD = "ERROR: batch timestamp 1795332885.584348201,0 must be after replica GC threshold "
            + "1795332894.103672556,0 (r79: /{Table/106-Max})";

    @Test
    public void shouldScanAgainWhenTheChangefeedEndsBecauseItsCursorIsTooOld() throws Exception {
        // CockroachDB can accept the stored cursor and deliver rows before it reports that the cursor is older
        // than the garbage collection threshold of one of the ranges. A restart would see the same again, so
        // snapshot.mode=when_needed scans again right away instead of leaving it to a restart.
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().with("snapshot.mode", "when_needed").build(), dispatcher());
        Statement statement = mock(Statement.class);
        Connection connection = streamingConnection();
        ResultSet atTheCursor = rows(new SQLException(TOO_OLD, "XXUUU"), "{\"resolved\":\"1795332890000000000.0000000000\"}");
        ResultSet rescan = rows(null);
        when(statement.executeQuery(anyString())).thenReturn(atTheCursor).thenReturn(rescan);
        CockroachDBOffsetContext offsets = mock(CockroachDBOffsetContext.class);
        when(offsets.getCursor()).thenReturn("1795332885584348201.0000000000");
        LogInterceptor log = new LogInterceptor(CockroachDBStreamingChangeEventSource.class);

        source.streamSinkless(statement, connection, List.of(new TableId("testdb", "public", "orders")), offsets, () -> true, true);

        verify(connection, atLeastOnce()).rollback();
        ArgumentCaptor<String> queries = ArgumentCaptor.forClass(String.class);
        verify(statement, times(2)).executeQuery(queries.capture());
        assertThat(queries.getAllValues().get(0)).contains("cursor = '1795332885584348201.0000000000'");
        assertThat(queries.getAllValues().get(1)).contains("initial_scan = 'yes'").doesNotContain("cursor = '1795332885584348201.0000000000'");
        assertThat(log.containsWarnMessage("Starting again with an initial scan")).isTrue();
    }

    @Test
    public void shouldNotBlameTheScanForACursorThatIsRejectedBeforeAnyRow() throws Exception {
        // snapshot.mode=always scans from the stored cursor at every start. When CockroachDB rejects that cursor,
        // no scan has taken place, and a higher gc.ttlseconds does not bring the changes back.
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().with("snapshot.mode", "always").build(), dispatcher());
        Statement statement = mock(Statement.class);
        SQLException tooOld = new SQLException("ERROR: could not create changefeed: cursor 1795332885584348201.0000000000 is older than the "
                + "GC threshold 1795332894103672556: " + TOO_OLD.substring("ERROR: ".length()), "XXUUU");
        when(statement.executeQuery(anyString())).thenThrow(tooOld);
        CockroachDBOffsetContext offsets = mock(CockroachDBOffsetContext.class);
        when(offsets.getCursor()).thenReturn("1795332885584348201.0000000000");

        assertThatThrownBy(() -> source.streamSinkless(statement, streamingConnection(), List.of(new TableId("testdb", "public", "orders")),
                offsets, () -> true, true))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("snapshot mode when_needed")
                .hasMessageNotContaining("initial scan of the sinkless changefeed")
                .hasCause(tooOld);
        verify(statement, times(1)).executeQuery(anyString());
    }

    @Test
    public void shouldStopWhenTheInitialScanIsOvertakenByTheGarbageCollection() throws Exception {
        // The scan reads the tables as of its start. When CockroachDB has garbage-collected that data before
        // the scan is through, a new scan would start from the beginning and end the same way.
        for (String snapshotMode : List.of("initial", "when_needed")) {
            CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().with("snapshot.mode", snapshotMode).build(), dispatcher());
            Statement statement = mock(Statement.class);
            SQLException tooOld = new SQLException(TOO_OLD, "XXUUU");
            // The changefeed has returned a row, here one without a value, which the connector passes over.
            ResultSet scan = rows(tooOld, " ");
            when(statement.executeQuery(anyString())).thenReturn(scan);
            assertThatThrownBy(() -> source.streamSinkless(statement, streamingConnection(), List.of(new TableId("testdb", "public", "orders")),
                    mock(CockroachDBOffsetContext.class), () -> true, false))
                    .as(snapshotMode)
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining("initial scan")
                    .hasMessageContaining("gc.ttlseconds")
                    .hasMessageContaining("must be after replica GC threshold")
                    // An SQLException in the cause chain would make the failure retriable.
                    .hasNoCause()
                    .hasSuppressedException(tooOld);
            verify(statement, times(1)).executeQuery(anyString());
        }
    }

    @Test
    public void shouldPassOnTheEndOfTheChangefeedInASnapshotModeThatDoesNotScanAgain() throws Exception {
        CockroachDBStreamingChangeEventSource source = createSource(sinklessConfig().with("snapshot.mode", "initial").build(), dispatcher());
        Statement statement = mock(Statement.class);
        SQLException tooOld = new SQLException(TOO_OLD, "XXUUU");
        ResultSet atTheCursor = rows(tooOld, "{\"resolved\":\"1795332890000000000.0000000000\"}");
        when(statement.executeQuery(anyString())).thenReturn(atTheCursor);

        // The changefeed ran from the cursor in the offsets, so the error says which snapshot mode scans again.
        CockroachDBOffsetContext offsets = mock(CockroachDBOffsetContext.class);
        when(offsets.getCursor()).thenReturn("1795332885584348201.0000000000");
        assertThatThrownBy(() -> source.streamSinkless(statement, streamingConnection(), List.of(new TableId("testdb", "public", "orders")),
                offsets, () -> true, true))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("must be after replica GC threshold")
                .hasMessageContaining("snapshot mode when_needed")
                .hasCause(tooOld);
        verify(statement, times(1)).executeQuery(anyString());
    }

    /**
     * The connection of a sinkless changefeed. It also answers a query for the cluster timestamp, which a
     * statement that starts with a scan can ask for.
     */
    private static Connection streamingConnection() throws SQLException {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet timestamp = mock(ResultSet.class);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(timestamp);
        when(timestamp.next()).thenReturn(true);
        when(timestamp.getString(1)).thenReturn("1795332999000000000.0000000000");
        return connection;
    }

    /** The rows of a sinkless changefeed with the given values, which ends with the failure, or normally without one. */
    private static ResultSet rows(SQLException failure, String... values) throws SQLException {
        ResultSet rows = mock(ResultSet.class);
        int[] row = { -1 };
        when(rows.next()).thenAnswer(invocation -> {
            if (++row[0] < values.length) {
                return true;
            }
            if (failure != null) {
                throw failure;
            }
            return false;
        });
        when(rows.getBytes("value")).thenAnswer(invocation -> values[row[0]].getBytes(StandardCharsets.UTF_8));
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static EventDispatcher<CockroachDBPartition, TableId> dispatcher() {
        return mock(EventDispatcher.class);
    }

    private CockroachDBStreamingChangeEventSource createSource(Configuration config, EventDispatcher<CockroachDBPartition, TableId> dispatcher) {
        return new CockroachDBStreamingChangeEventSource(new CockroachDBConnectorConfig(config), dispatcher, null, null, null);
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
