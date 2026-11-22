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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * Tests the look that the streaming source takes at its changefeed jobs while it streams. CockroachDB
 * reports the end of a changefeed job nowhere else: the intermediate topics just stay silent.
 */
class CockroachDBChangefeedJobCheckTest {

    private static final TableId ACCT = new TableId("reprodb", "public", "acct");
    private static final TableId ORDERS = new TableId("reprodb", "public", "orders");
    private static final String CLUSTER_TIMESTAMP = "1795243641446371339.0000000000";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TOO_OLD = "batch timestamp 1795332729.354355990,0 must be after replica GC threshold "
            + "1795332737.923743767,0 (r79: /{Table/106-Max})";

    /**
     * A source with two reused changefeed jobs, 1101 for acct and 1102 for orders, on a cluster that answers
     * the job status query with scripted rows and gives the changefeeds that the source creates the job
     * ids 2000, 2001, and so on.
     */
    private static final class ScriptedSource extends CockroachDBStreamingChangeEventSource {
        private final Connection jdbc = mock(Connection.class);
        private final CockroachDBOffsetContext offsets = mock(CockroachDBOffsetContext.class);
        private final Deque<String[][]> answers = new ArrayDeque<>();
        private final List<String> statusQueries = new ArrayList<>();
        private final List<String> createdChangefeeds = new ArrayList<>();
        private final long[] now;
        private long nextJobId = 2000;
        private boolean queryFails;

        ScriptedSource(String snapshotMode, long[] now) throws Exception {
            this(snapshotMode, now, false);
        }

        /** With {@code oneJob}, job 1101 covers both tables, and there is no job 1102. */
        ScriptedSource(String snapshotMode, long[] now, boolean oneJob) throws Exception {
            super(config(snapshotMode), dispatcher(), null, () -> now[0], null);
            this.now = now;
            final Statement statement = mock(Statement.class);
            when(jdbc.createStatement()).thenReturn(statement);
            when(jdbc.getAutoCommit()).thenReturn(true);
            when(statement.getConnection()).thenReturn(jdbc);
            when(statement.executeQuery(anyString())).thenAnswer(invocation -> {
                final String query = invocation.getArgument(0);
                if (query.contains("cluster_logical_timestamp")) {
                    return rows(new String[]{ CLUSTER_TIMESTAMP });
                }
                statusQueries.add(query);
                if (queryFails) {
                    throw new SQLException("An I/O error occurred while sending to the backend.", "08006");
                }
                return rows(answers.isEmpty() ? new String[0][] : answers.poll());
            });
            when(statement.execute(anyString())).thenAnswer(invocation -> {
                final String sql = invocation.getArgument(0);
                if (sql.startsWith("CREATE CHANGEFEED")) {
                    createdChangefeeds.add(sql);
                }
                return true;
            });
            when(statement.getResultSet()).thenAnswer(invocation -> rows(new String[]{ String.valueOf(nextJobId++) }));
            when(offsets.getCursor()).thenReturn("1795243600000000000.0000000000");

            createChangefeeds(oneJob
                    ? connectionListing(
                            new String[]{ "1101", "running", changefeedFor("acct").replace("reprodb.public.acct", "reprodb.public.acct, reprodb.public.orders") })
                    : connectionListing(
                            new String[]{ "1101", "running", changefeedFor("acct") },
                            new String[]{ "1102", "running", changefeedFor("orders") }),
                    List.of(ACCT, ORDERS), offsets, true);
            // The first look at the jobs comes a few seconds after the start.
            checkChangefeedJobs(offsets);
        }

        /** The answer to the next job status query: the status of job 1101, and the status and error of another job. */
        ScriptedSource answer(String statusOf1101, String otherJob, String itsStatus, String itsError) {
            answers.add(new String[][]{ { "1101", statusOf1101, "" }, { otherJob, itsStatus, itsError } });
            return this;
        }

        ScriptedSource answer(String statusOf1101, String statusOf1102, String errorOf1102) {
            return answer(statusOf1101, "1102", statusOf1102, errorOf1102);
        }

        /** The answer to the next job status query when one job is all that the connector has. */
        ScriptedSource answerForOneJob(String job, String itsStatus, String itsError) {
            answers.add(new String[][]{ { job, itsStatus, itsError } });
            return this;
        }

        void checkAfter(long millis) throws SQLException {
            now[0] += millis;
            checkChangefeedJobs(offsets);
        }

        @Override
        Connection jobConnection() {
            return jdbc;
        }

        @Override
        void reconnectSchemaRefreshConnection() {
        }
    }

    @Test
    void goesOnWhileItsChangefeedJobsRun() throws Exception {
        final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 }).answer("running", "running", "");

        assertThatCode(() -> source.checkAfter(60_000)).doesNotThrowAnyException();
        assertThat(source.statusQueries).singleElement().satisfies(query -> assertThat(query).contains("job_id IN (1101, 1102)"));
    }

    @Test
    void asksOnceInAWhile() throws Exception {
        final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 })
                .answer("running", "running", "")
                .answer("running", "running", "");

        // The first look comes soon after the start, the later ones every 30 seconds.
        source.checkAfter(1_000);
        assertThat(source.statusQueries).isEmpty();
        source.checkAfter(5_000);
        assertThat(source.statusQueries).hasSize(1);
        source.checkAfter(10_000);
        assertThat(source.statusQueries).hasSize(1);
        source.checkAfter(30_000);
        assertThat(source.statusQueries).hasSize(2);
    }

    @Test
    void limitsTheTimeThatTheQueryCanTake() throws Exception {
        // The query runs on the thread that reads the intermediate topics.
        final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 }).answer("running", "running", "");
        when(source.jdbc.getNetworkTimeout()).thenReturn(0);

        source.checkAfter(60_000);

        final InOrder order = inOrder(source.jdbc);
        order.verify(source.jdbc).setNetworkTimeout(any(), eq(CockroachDBStreamingChangeEventSource.CHANGEFEED_JOB_QUERY_TIMEOUT_MS));
        order.verify(source.jdbc).createStatement();
        order.verify(source.jdbc).setNetworkTimeout(any(), eq(0));
    }

    @Test
    void stopsWhenAChangefeedJobFailed() throws Exception {
        final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 })
                .answer("running", "failed", "table was dropped or truncated");

        assertThatThrownBy(() -> source.checkAfter(60_000))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Changefeed job 1102")
                .hasMessageContaining("reprodb.public.orders")
                .hasMessageContaining("'failed': table was dropped or truncated")
                .hasMessageContaining("Restart the connector")
                .satisfies(failure -> assertThat(errorHandler().isRetriable(new RuntimeException("Failed to consume from Kafka topics", failure)))
                        .as("retriable").isFalse());
    }

    @Test
    void stopsWhenAChangefeedJobWasCanceled() throws Exception {
        for (String status : new String[]{ "reverting", "canceled" }) {
            final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 }).answer("running", status, "");

            assertThatThrownBy(() -> source.checkAfter(60_000))
                    .as(status)
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining("Changefeed job 1102")
                    .hasMessageContaining("'" + status + "'");
        }
    }

    @Test
    void waitsUntilACanceledChangefeedJobHasStopped() throws Exception {
        // A job whose cancellation was requested writes until CockroachDB carries the cancellation out. A
        // restart at that point would create a changefeed next to it.
        final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 })
                .answer("running", "cancel-requested", "")
                .answer("running", "reverting", "");

        assertThatCode(() -> source.checkAfter(60_000)).doesNotThrowAnyException();
        // It looks again sooner than usual.
        assertThatThrownBy(() -> source.checkAfter(5_000))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("'reverting'");
    }

    @Test
    void stopsWhenAChangefeedJobIsNotListedAnyMore() throws Exception {
        final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 });

        assertThatThrownBy(() -> source.checkAfter(60_000))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Changefeed job 1101")
                .hasMessageContaining("'unknown'");
    }

    @Test
    void namesTheSnapshotModeThatScansAgainWhenTheCursorIsTooOld() throws Exception {
        // CockroachDB accepts a changefeed into a sink at a cursor that is older than the garbage collection
        // threshold, and fails its job afterwards. The job goes through the status reverting on its way.
        for (String status : new String[]{ "reverting", "failed" }) {
            final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 }).answer("running", status, TOO_OLD);

            assertThatThrownBy(() -> source.checkAfter(60_000))
                    .as(status)
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining("Changefeed job 1102")
                    .hasMessageContaining("GC threshold")
                    .hasMessageContaining("when_needed");
            assertThat(source.createdChangefeeds).isEmpty();
        }
    }

    @Test
    void scansTheTablesOfTheJobAgainWhenTheCursorIsTooOldAndTheSnapshotModeAllowsIt() throws Exception {
        final ScriptedSource source = new ScriptedSource("when_needed", new long[]{ 1_000_000 })
                .answer("running", "failed", TOO_OLD)
                .answer("running", "2000", "running", "");

        source.checkAfter(60_000);

        // A changefeed with an initial scan for the tables of the failed job, at the current cluster timestamp.
        assertThat(source.createdChangefeeds).singleElement().satisfies(statement -> assertThat(statement)
                .contains("\"orders\"", "initial_scan = 'yes'", "cursor = '" + CLUSTER_TIMESTAMP + "'")
                .doesNotContain("\"acct\""));
        // The connector knows the timestamp of the scan, by which it recognizes the rows of the scan.
        assertThat(source.scanTimestamp(ORDERS)).isEqualByComparingTo(CLUSTER_TIMESTAMP);
        assertThat(source.scanTimestamp(ACCT)).isEqualByComparingTo("1");
        // The other changefeed goes on, so the scan is no snapshot phase of the connector: a create of the
        // other table stays a create, also when it carries no timestamp to tell it by.
        verify(source.offsets, never()).markSnapshotRecord(SnapshotRecord.TRUE);
        assertThat(source.extractOperation(ACCT, MAPPER.readTree("{\"after\":{\"id\":1},\"op\":\"c\"}"))).isEqualTo(Envelope.Operation.CREATE);
        assertThat(source.extractOperation(ORDERS, MAPPER.readTree("{\"after\":{\"id\":1},\"op\":\"c\"}"))).isEqualTo(Envelope.Operation.READ);

        // From now on the new job is the one that is looked at.
        assertThatCode(() -> source.checkAfter(60_000)).doesNotThrowAnyException();
        assertThat(source.statusQueries.get(1)).contains("job_id IN (1101, 2000)");
    }

    @Test
    void keepsTheStartOfTheScanThatReplacesAFailedChangefeed() throws Exception {
        // The stored cursor stays the one that CockroachDB no longer covers until the scan is through. A
        // changefeed that has to be created again in that time starts with that scan again. A scan as of a
        // later time would not show the rows that were deleted in between.
        final ScriptedSource source = new ScriptedSource("when_needed", new long[]{ 1_000_000 }).answer("running", "failed", TOO_OLD);

        source.checkAfter(60_000);

        verify(source.offsets).setChangefeedStart("repro.reprodb.public.orders", CLUSTER_TIMESTAMP, true);
        verify(source.offsets, never()).setChangefeedStart(eq("repro.reprodb.public.acct"), anyString(), anyBoolean());
    }

    @Test
    void startsASnapshotPhaseWhenEveryTableIsScannedAgain() throws Exception {
        // One changefeed covers all tables. Its replacement scans all of them, like the scan with which
        // snapshot.mode=when_needed answers a cursor that CockroachDB rejects at the start.
        final ScriptedSource source = new ScriptedSource("when_needed", new long[]{ 1_000_000 }, true)
                .answerForOneJob("1101", "failed", TOO_OLD);

        source.checkAfter(60_000);

        assertThat(source.createdChangefeeds).singleElement().satisfies(statement -> assertThat(statement)
                .contains("\"acct\"", "\"orders\"", "initial_scan = 'yes'", "cursor = '" + CLUSTER_TIMESTAMP + "'"));
        verify(source.offsets).markSnapshotRecord(SnapshotRecord.TRUE);
    }

    @Test
    void startsASnapshotPhaseWhenAllChangefeedsAreReplacedTogether() throws Exception {
        // With more than one changefeed, the jobs can all have failed by the same look at them when the
        // stored cursor is too old. Their replacements scan every table, like the replacement of a single
        // changefeed for all tables.
        final ScriptedSource source = new ScriptedSource("when_needed", new long[]{ 1_000_000 });
        source.answers.add(new String[][]{ { "1101", "failed", TOO_OLD }, { "1102", "reverting", TOO_OLD } });

        source.checkAfter(60_000);

        assertThat(source.createdChangefeeds).hasSize(2);
        verify(source.offsets).markSnapshotRecord(SnapshotRecord.TRUE);
        // The next look is at the two new jobs.
        source.answers.add(new String[][]{ { "2000", "running", "" }, { "2001", "running", "" } });
        assertThatCode(() -> source.checkAfter(60_000)).doesNotThrowAnyException();
        assertThat(source.statusQueries.get(1)).contains("2000", "2001").doesNotContain("1101", "1102");
    }

    @Test
    void startsASnapshotPhaseWhenTheLastChangefeedIsReplacedWhileTheOthersStillScan() throws Exception {
        // One job can still be running when the connector looks at the jobs for the first time, and fail by the
        // next look. With its replacement, every table is being scanned.
        final ScriptedSource source = new ScriptedSource("when_needed", new long[]{ 1_000_000 }).answer("running", "failed", TOO_OLD);
        source.checkAfter(60_000);
        verify(source.offsets, never()).markSnapshotRecord(SnapshotRecord.TRUE);

        source.answers.add(new String[][]{ { "1101", "failed", TOO_OLD }, { "2000", "running", "" } });
        source.checkAfter(60_000);

        assertThat(source.createdChangefeeds).hasSize(2);
        verify(source.offsets).markSnapshotRecord(SnapshotRecord.TRUE);
    }

    @Test
    void startsNoSnapshotPhaseWhenTheScanOfTheOtherTablesIsThrough() throws Exception {
        // The scan of the first replacement ended with its first resolved timestamp. The job that fails later
        // has its tables scanned again, and the other tables go on with their changes.
        final ScriptedSource source = new ScriptedSource("when_needed", new long[]{ 1_000_000 }).answer("running", "failed", TOO_OLD);
        source.checkAfter(60_000);
        source.processChangefeedEvent(null, "{\"resolved\":\"" + CLUSTER_TIMESTAMP + "\"}", ORDERS, source.offsets);

        source.answers.add(new String[][]{ { "1101", "failed", TOO_OLD }, { "2000", "running", "" } });
        source.checkAfter(60_000);

        assertThat(source.createdChangefeeds).hasSize(2);
        verify(source.offsets, never()).markSnapshotRecord(SnapshotRecord.TRUE);
        assertThat(source.extractOperation(ORDERS, MAPPER.readTree("{\"after\":{\"id\":1},\"op\":\"c\"}"))).isEqualTo(Envelope.Operation.CREATE);
        assertThat(source.extractOperation(ACCT, MAPPER.readTree("{\"after\":{\"id\":1},\"op\":\"c\"}"))).isEqualTo(Envelope.Operation.READ);
    }

    @Test
    void keepsHowFarTheOtherTablesHaveComeWhenAFailedChangefeedIsReplaced() throws Exception {
        // The stored cursor is behind the changefeed that failed. A changefeed for the table that is ahead
        // starts where that table has come to, not at the cursor that CockroachDB no longer covers.
        final ScriptedSource source = new ScriptedSource("when_needed", new long[]{ 1_000_000 }).answer("running", "failed", TOO_OLD);
        source.processChangefeedEvent(null, "{\"resolved\":\"1795243610000000000.0000000000\"}", ACCT, source.offsets);

        source.checkAfter(60_000);

        verify(source.offsets).setChangefeedStart("repro.reprodb.public.acct", "1795243610000000000.0000000000", false);
        verify(source.offsets).setChangefeedStart("repro.reprodb.public.orders", CLUSTER_TIMESTAMP, true);
    }

    @Test
    void doesNotScanAgainWhenTheReplacementFailsInTheSameWay() throws Exception {
        // For example when the scan takes longer than the garbage collection allows.
        final ScriptedSource source = new ScriptedSource("when_needed", new long[]{ 1_000_000 })
                .answer("running", "failed", TOO_OLD)
                .answer("running", "2000", "failed", TOO_OLD);
        source.checkAfter(60_000);

        assertThatThrownBy(() -> source.checkAfter(60_000))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Changefeed job 2000")
                .hasMessageContaining("does not start a further scan");
        assertThat(source.createdChangefeeds).hasSize(1);
    }

    @Test
    void reportsAPausedChangefeedJobOnce() throws Exception {
        final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 })
                .answer("running", "paused", "")
                .answer("running", "paused", "")
                .answer("running", "running", "");
        final Logger sourceLogger = (Logger) LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        sourceLogger.addAppender(log);
        try {
            source.checkAfter(60_000);
            source.checkAfter(60_000);
            source.checkAfter(60_000);

            assertThat(log.list).filteredOn(event -> event.getFormattedMessage().contains("is paused"))
                    .singleElement()
                    .satisfies(event -> assertThat(event.getFormattedMessage()).contains("Changefeed job 1102", "RESUME JOB 1102"));
            assertThat(log.list).filteredOn(event -> event.getFormattedMessage().contains("Changefeed job 1102 runs again")).hasSize(1);
        }
        finally {
            sourceLogger.detachAppender(log);
        }
    }

    @Test
    void retriesWhenTheJobsCannotBeRead() throws Exception {
        final ScriptedSource source = new ScriptedSource("initial", new long[]{ 1_000_000 });
        source.queryFails = true;

        // After a second attempt on a new connection, the failure restarts the task.
        assertThatThrownBy(() -> source.checkAfter(60_000))
                .isInstanceOf(DebeziumException.class)
                .hasCauseInstanceOf(SQLException.class)
                .satisfies(failure -> assertThat(errorHandler().isRetriable(new RuntimeException("Failed to consume from Kafka topics", failure)))
                        .as("retriable").isTrue());
        assertThat(source.statusQueries).hasSize(2);
    }

    @SuppressWarnings("unchecked")
    private static EventDispatcher<CockroachDBPartition, TableId> dispatcher() {
        return mock(EventDispatcher.class);
    }

    private static String changefeedFor(String table) {
        return "CREATE CHANGEFEED FOR TABLE reprodb.public." + table + " INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (cursor = '1.0000000000', enriched_properties = 'source', envelope = 'enriched', full_table_name, "
                + "initial_scan = 'no', resolved = '3s', updated)";
    }

    private static CockroachDBConnectorConfig config(String snapshotMode) {
        return new CockroachDBConnectorConfig(Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "reprodb")
                .with("topic.prefix", "repro")
                .with("cockroachdb.changefeed.sink.type", "kafka")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with("snapshot.mode", snapshotMode)
                .build());
    }

    private static CockroachDBErrorHandler errorHandler() {
        final DefaultQueueProvider<DataChangeEvent> queueProvider = new DefaultQueueProvider<>();
        queueProvider.configure(Map.of("max.queue.size", String.valueOf(DEFAULT_MAX_QUEUE_SIZE)));
        return new CockroachDBErrorHandler(config("initial"),
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .queueProvider(queueProvider)
                        .pollInterval(Duration.ofMillis(500))
                        .pollDispatchInterval(Duration.ofMillis(0))
                        .build(),
                null);
    }

    /** A result set with the given rows, whose columns are read as strings or as longs. */
    private static ResultSet rows(String[]... rows) throws SQLException {
        final ResultSet result = mock(ResultSet.class);
        final int[] row = { -1 };
        when(result.next()).thenAnswer(invocation -> ++row[0] < rows.length);
        when(result.getString(1)).thenAnswer(invocation -> rows[row[0]][0]);
        when(result.getString(2)).thenAnswer(invocation -> rows[row[0]][1]);
        when(result.getString(3)).thenAnswer(invocation -> rows[row[0]][2]);
        when(result.getLong(1)).thenAnswer(invocation -> Long.parseLong(rows[row[0]][0]));
        return result;
    }

    private static CockroachDBConnection connectionListing(String[]... jobs) throws SQLException {
        final ResultSet rows = rows(jobs);
        final Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenReturn(rows);
        final Connection jdbc = mock(Connection.class);
        when(jdbc.createStatement()).thenReturn(statement);
        final CockroachDBConnection connection = mock(CockroachDBConnection.class);
        when(connection.connection()).thenReturn(jdbc);
        return connection;
    }
}
