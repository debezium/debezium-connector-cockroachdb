/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * Unit tests for the changefeed reuse option checks. A reused changefeed has the options it was
 * created with, plus any added later with ALTER CHANGEFEED, so it must be validated against the
 * connector configuration;
 * silently reusing a changefeed without the {@code diff} option while
 * {@code cockroachdb.changefeed.include.diff} is enabled leaves update events without a before
 * image. Also covers a failing jobs query, which must never be taken to mean that no changefeed runs.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangefeedReuseTest {

    private static final String WITH_DIFF = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
            + "WITH OPTIONS (diff, enriched_properties = 'source,schema', envelope = 'enriched', resolved = '3s')";
    private static final String WITHOUT_DIFF = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
            + "WITH OPTIONS (enriched_properties = 'source,schema', envelope = 'enriched', resolved = '3s')";

    @Test
    void detectsDiffOptionInJobDescription() {
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption(WITH_DIFF)).isTrue();
    }

    @Test
    void detectsMissingDiffOptionInJobDescription() {
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption(WITHOUT_DIFF)).isFalse();
    }

    @Test
    void diffCheckDoesNotMatchTableNamesContainingDiff() {
        String description = "CREATE CHANGEFEED FOR TABLE demodb.public.diff_audit INTO 'kafka://kafka:9092?topic_prefix=crdb.' "
                + "WITH OPTIONS (envelope = 'enriched', resolved = '3s')";
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption(description)).isFalse();
    }

    @Test
    void diffCheckHandlesNullAndMalformedDescriptions() {
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption(null)).isFalse();
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption("not a changefeed description")).isFalse();
    }

    @Test
    void detectsMvccTimestampOptionInJobDescription() {
        String with = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', mvcc_timestamp, resolved = '3s')";
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasOption(with, "mvcc_timestamp")).isTrue();
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasOption(WITH_DIFF, "mvcc_timestamp")).isFalse();
    }

    private static final TableId ACCT = new TableId("reprodb", "public", "acct");

    @Test
    void initialOnlyReusesARunningInitialScanOnlyChangefeedWithoutDiff() throws Exception {
        // In initial_only mode the connector never requests diff (CockroachDB rejects it with
        // initial_scan='only'), so a reused initial_scan='only' changefeed without diff is valid
        // even when include.diff is enabled (debezium/dbz#2966).
        String initialScanOnly = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', full_table_name, initial_scan = 'only')";
        CockroachDBStreamingChangeEventSource source = initialOnlySource();

        assertThat(source.changefeedExists(connectionListing("1001", "running", initialScanOnly), ACCT, List.of(ACCT))).isTrue();
    }

    @Test
    void initialOnlyRefusesToReuseAContinuouslyStreamingChangefeed() throws Exception {
        // A streaming changefeed never finishes, so an initial_only run that attached to it would never complete.
        String streaming = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', full_table_name, initial_scan = 'yes', resolved = '10s')";
        CockroachDBStreamingChangeEventSource source = initialOnlySource();

        assertThatThrownBy(() -> source.changefeedExists(connectionListing("1002", "running", streaming), ACCT, List.of(ACCT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1002")
                .hasMessageContaining("initial_only");
    }

    @Test
    void diffMismatchErrorOffersAlterChangefeed() throws Exception {
        // CockroachDB can add the option to the existing changefeed, which keeps its job and position
        // (debezium/dbz#2967).
        CockroachDBStreamingChangeEventSource source = sourceWith("cockroachdb.changefeed.include.diff");

        assertThatThrownBy(() -> source.changefeedExists(connectionListing("1003", "running", WITHOUT_DIFF), ACCT, List.of(ACCT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PAUSE JOB 1003")
                .hasMessageContaining("SHOW CHANGEFEED JOB 1003")
                .hasMessageContaining("'paused'")
                .hasMessageContaining("ALTER CHANGEFEED 1003 SET diff")
                .hasMessageContaining("RESUME JOB 1003")
                .hasMessageContaining("CANCEL JOB 1003");
    }

    @Test
    void mvccTimestampMismatchErrorOffersAlterChangefeed() throws Exception {
        CockroachDBStreamingChangeEventSource source = sourceWith("cockroachdb.changefeed.include.mvcc.timestamp");

        assertThatThrownBy(() -> source.changefeedExists(connectionListing("1004", "running", WITH_DIFF), ACCT, List.of(ACCT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PAUSE JOB 1004")
                .hasMessageContaining("SHOW CHANGEFEED JOB 1004")
                .hasMessageContaining("'paused'")
                .hasMessageContaining("ALTER CHANGEFEED 1004 SET mvcc_timestamp")
                .hasMessageContaining("RESUME JOB 1004")
                .hasMessageContaining("CANCEL JOB 1004");
    }

    @Test
    void namesEveryChangefeedJobOfTheConnectorThatLacksTheOption() throws Exception {
        // With cockroachdb.changefeed.max.tables.per.changefeed the connector runs several changefeeds,
        // and all of them were created before the option was enabled.
        TableId other = new TableId("reprodb", "public", "other");
        String otherWithoutDiff = "CREATE CHANGEFEED FOR TABLE reprodb.public.other INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source,schema', envelope = 'enriched', resolved = '3s')";
        CockroachDBStreamingChangeEventSource source = sourceWith("cockroachdb.changefeed.include.diff");

        assertThatThrownBy(() -> source.changefeedExists(
                connectionListingJobs(new String[]{ "1003", "running", WITHOUT_DIFF }, new String[]{ "1005", "running", otherWithoutDiff }),
                ACCT, List.of(ACCT, other)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1003")
                .hasMessageContaining("1005");
    }

    @Test
    void retriesTheJobsQueryAfterReconnecting() throws Exception {
        // A node restart or drain ends the session, so the first query fails and a new connection finds the job.
        ResultSet jobs = jobRows(new String[]{ "1006", "running", WITHOUT_DIFF });
        Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenThrow(sessionEnded()).thenReturn(jobs);
        CockroachDBConnection connection = connectionFor(statement);

        assertThat(source(Map.of()).changefeedExists(connection, ACCT, List.of(ACCT))).isTrue();
        verify(connection).close();
    }

    @Test
    void failsTheTaskWhenTheJobsQueryFailsAgainAfterReconnecting() throws Exception {
        // Creating a changefeed without knowing whether one already runs could start a second one that
        // double-produces into the same topics (debezium/dbz#3037). With the SQLException as the cause, the
        // error handler treats the failure as retriable, so the restarted task checks again.
        Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenThrow(sessionEnded());

        assertThatThrownBy(() -> source(Map.of()).changefeedExists(connectionFor(statement), ACCT, List.of(ACCT)))
                .isInstanceOf(DebeziumException.class)
                .hasCauseInstanceOf(SQLException.class);
    }

    @Test
    void failsTheTaskWhenReconnectingForTheJobsQueryFails() throws Exception {
        Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenThrow(sessionEnded());
        Connection jdbc = mock(Connection.class);
        when(jdbc.createStatement()).thenReturn(statement);
        CockroachDBConnection connection = mock(CockroachDBConnection.class);
        when(connection.connection()).thenReturn(jdbc).thenThrow(new SQLException("Connection to localhost:26257 refused.", "08001"));

        assertThatThrownBy(() -> source(Map.of()).changefeedExists(connection, ACCT, List.of(ACCT)))
                .isInstanceOf(DebeziumException.class)
                .hasCauseInstanceOf(SQLException.class);
    }

    private static final TableId ORDERS = new TableId("reprodb", "public", "orders");

    @Test
    void remembersTheCursorOfEveryRunningChangefeed() throws Exception {
        // The running changefeeds do not have to line up with the connector's groups of tables.
        CockroachDBStreamingChangeEventSource source = source(Map.of());

        assertThat(source.changefeedExists(connectionListingJobs(
                new String[]{ "1007", "running", scanning("acct", "1.0000000000") },
                new String[]{ "1008", "running", scanning("orders", "2.0000000000") }), ACCT, List.of(ACCT, ORDERS))).isTrue();

        assertThat(source.scanTimestamp(ACCT)).isEqualByComparingTo("1");
        assertThat(source.scanTimestamp(ORDERS)).isEqualByComparingTo("2");
    }

    @Test
    void remembersNoCursorWhenNoChangefeedCoversTheTable() throws Exception {
        CockroachDBStreamingChangeEventSource source = source(Map.of());

        assertThat(source.changefeedExists(connectionListing("1009", "running", scanning("acct", "1.0000000000")), ORDERS,
                List.of(ACCT, ORDERS))).isFalse();

        assertThat(source.scanTimestamp(ACCT)).isNull();
    }

    @Test
    void readsTheCursorsOfTheRunningChangefeedsOncePerStart() throws Exception {
        // Reading them again for every group of tables that has a running changefeed made the work grow
        // with the number of groups times the number of changefeeds times the number of tables.
        CockroachDBStreamingChangeEventSource source = source(Map.of());
        String acct = scanning("acct", "1.0000000000");

        assertThat(source.changefeedExists(connectionListingJobs(
                new String[]{ "1010", "running", acct },
                new String[]{ "1011", "running", scanning("orders", "2.0000000000") }), ACCT, List.of(ACCT, ORDERS))).isTrue();
        // The second listing differs from the first, which the listings of one start never do, so that the
        // test can tell whether the cursors were read again.
        assertThat(source.changefeedExists(connectionListingJobs(
                new String[]{ "1010", "running", acct },
                new String[]{ "1011", "running", scanning("orders", "3.0000000000") }), ORDERS, List.of(ACCT, ORDERS))).isTrue();

        assertThat(source.scanTimestamp(ORDERS)).isEqualByComparingTo("2");
    }

    @Test
    void doesNotTakeAChangefeedWithALongerTopicPrefixForItsOwn() throws Exception {
        // Another connector with the topic prefix "repro.v2" captures the same table. Its sink URI contains
        // "topic_prefix=repro.", the text that this connector looks for, as the start of "topic_prefix=repro.v2.".
        final String other = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.v2.' "
                + "WITH OPTIONS (cursor = '9.0000000000', enriched_properties = 'source', envelope = 'enriched', full_table_name, "
                + "initial_scan = 'yes', resolved = '3s', updated)";
        CockroachDBStreamingChangeEventSource source = source(Map.of());

        assertThat(source.changefeedExists(connectionListing("1015", "running", other), ACCT, List.of(ACCT))).isFalse();

        // Next to the connector's own changefeed, the other one must not lend its cursor either.
        assertThat(source.changefeedExists(connectionListingJobs(
                new String[]{ "1016", "running", scanning("acct", "1.0000000000") },
                new String[]{ "1015", "running", other }), ACCT, List.of(ACCT))).isTrue();
        assertThat(source.scanTimestamp(ACCT)).isEqualByComparingTo("1");
    }

    @Test
    void recognizesItsTopicPrefixAmongOtherSinkParameters() {
        assertThat(CockroachDBStreamingChangeEventSource.descriptionHasTopicPrefix(
                "CREATE CHANGEFEED FOR TABLE d.public.t INTO 'kafka://kafka:9092?topic_prefix=repro.' WITH OPTIONS (updated)", "repro.")).isTrue();
        assertThat(CockroachDBStreamingChangeEventSource.descriptionHasTopicPrefix(
                "CREATE CHANGEFEED FOR TABLE d.public.t INTO 'kafka://kafka:9092?tls_enabled=true&topic_prefix=repro.&ca_cert=redacted' WITH OPTIONS (updated)",
                "repro.")).isTrue();
        assertThat(CockroachDBStreamingChangeEventSource.descriptionHasTopicPrefix(
                "CREATE CHANGEFEED FOR TABLE d.public.t INTO 'kafka://kafka:9092?topic_prefix=repro.v2.' WITH OPTIONS (updated)", "repro.")).isFalse();
        assertThat(CockroachDBStreamingChangeEventSource.descriptionHasTopicPrefix(
                "CREATE CHANGEFEED FOR TABLE d.public.t INTO 'kafka://kafka:9092?topic_prefix=repro.v2.&tls_enabled=true' WITH OPTIONS (updated)",
                "repro.")).isFalse();
        // A prefix without a trailing separator, as cockroachdb.changefeed.sink.topic.prefix allows.
        assertThat(CockroachDBStreamingChangeEventSource.descriptionHasTopicPrefix(
                "CREATE CHANGEFEED FOR TABLE d.public.t INTO 'kafka://kafka:9092?topic_prefix=env-prod-' WITH OPTIONS (updated)", "env-prod-")).isTrue();
        assertThat(CockroachDBStreamingChangeEventSource.descriptionHasTopicPrefix(
                "CREATE CHANGEFEED FOR TABLE d.public.t INTO 'kafka://kafka:9092?topic_prefix=env-prod-2-' WITH OPTIONS (updated)", "env-prod-")).isFalse();
        assertThat(CockroachDBStreamingChangeEventSource.descriptionHasTopicPrefix(null, "repro.")).isFalse();
    }

    @Test
    void createsAChangefeedForTheTablesThatNoRunningChangefeedCovers() throws Exception {
        // The running changefeed was created before orders was added to table.include.list.
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1017", "running", scanning("acct", "1.0000000000") });

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(null), false);

        assertThat(createdChangefeeds(cluster)).singleElement().satisfies(statement -> {
            assertThat(statement).contains("\"orders\"").doesNotContain("\"acct\"");
            assertThat(statement).contains("initial_scan = 'yes'", "cursor = '1795243641446371339.0000000000'");
        });
        // Without a stored cursor there is no past to look at.
        verify(cluster, never()).executeQuery(contains("information_schema.tables"));
    }

    @Test
    void leavesTheTablesOfARunningChangefeedToIt() throws Exception {
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1018", "running", "CREATE CHANGEFEED FOR TABLE reprodb.public.acct, reprodb.public.orders INTO "
                        + "'kafka://kafka:9092?topic_prefix=repro.' WITH OPTIONS (cursor = '1.0000000000', enriched_properties = 'source', "
                        + "envelope = 'enriched', full_table_name, initial_scan = 'yes', resolved = '3s', updated)" });

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(null), false);

        assertThat(createdChangefeeds(cluster)).isEmpty();
    }

    @Test
    void checksEveryTableOfAGroupAgainstTheRunningChangefeeds() throws Exception {
        // The first table has a running changefeed, the second one a paused changefeed. Starting would add a
        // changefeed for the second table that produces the same events again once the paused one resumes.
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1019", "running", scanning("acct", "1.0000000000") },
                new String[]{ "1020", "paused", scanning("orders", "2.0000000000") });

        assertThatThrownBy(() -> source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(null), false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1020")
                .hasMessageContaining("paused");
        assertThat(createdChangefeeds(cluster)).isEmpty();
    }

    @Test
    void checksEveryTableBeforeItCreatesAChangefeed() throws Exception {
        // One changefeed for each table. The first table has none, and the second one has a paused one, which
        // stops the start. No changefeed is created before that is known.
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1025", "paused", scanning("orders", "2.0000000000") });

        assertThatThrownBy(() -> source(Map.of("cockroachdb.changefeed.max.tables.per.changefeed", "1"))
                .createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(null), false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1025");
        assertThat(createdChangefeeds(cluster)).isEmpty();
    }

    @Test
    void listsTheChangefeedJobsOncePerStart() throws Exception {
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1021", "running", scanning("acct", "1.0000000000") },
                new String[]{ "1022", "running", scanning("orders", "2.0000000000") });

        // One table per changefeed, so the tables are checked in two groups.
        source(Map.of("cockroachdb.changefeed.max.tables.per.changefeed", "1"))
                .createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(null), false);

        verify(cluster, times(1)).executeQuery(contains("SHOW CHANGEFEED JOBS"));
        assertThat(createdChangefeeds(cluster)).isEmpty();
    }

    @Test
    void scansATableThatDidNotExistAtTheStoredCursor() throws Exception {
        // orders was created, and added to table.include.list, while the connector was stopped. CockroachDB
        // rejects a changefeed that starts at the stored cursor for a table that did not exist then, so the
        // connector looks the table up as of the cursor first.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        final CockroachDBOffsetContext offsets = offsetsAt(stored);
        final Logger sourceLogger = (Logger) LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        sourceLogger.addAppender(log);
        try {
            source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

            // The scan of the new table is no scan phase of the connector: the other tables go on as before.
            assertThat(log.list).noneMatch(event -> event.getFormattedMessage().startsWith("Initial scan in progress"));
        }
        finally {
            sourceLogger.detachAppender(log);
        }

        verify(cluster).executeQuery(contains("information_schema.tables AS OF SYSTEM TIME '" + stored + "'"));
        verify(cluster).executeQuery(contains("\"orders\" AS OF SYSTEM TIME '" + stored + "'"));
        final List<String> created = createdChangefeeds(cluster);
        assertThat(created).hasSize(2);
        // The table that existed at the cursor resumes there, without a scan.
        assertThat(created.get(0)).contains("\"acct\"", "cursor = '" + stored + "'", "initial_scan = 'no'").doesNotContain("\"orders\"");
        // All rows of the new table were written after the cursor. The scan returns them.
        assertThat(created.get(1)).contains("\"orders\"", "initial_scan = 'yes'", "cursor = '1795243641446371339.0000000000'")
                .doesNotContain("\"acct\"");
    }

    @Test
    void scansATableWhoseSchemaDidNotExistAtTheStoredCursor() throws Exception {
        // For a table in a schema that was created after the cursor, the lookup by name answers that the schema
        // is unknown, not that the table does not exist.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        doThrow(new SQLException("ERROR: unknown schema \"app2\"", "3F000")).when(cluster).executeQuery(contains("\"orders\" AS OF SYSTEM TIME"));

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(stored), true);

        final List<String> created = createdChangefeeds(cluster);
        assertThat(created).hasSize(2);
        assertThat(created.get(1)).contains("\"orders\"", "initial_scan = 'yes'", "cursor = '1795243641446371339.0000000000'");
    }

    @Test
    void doesNotTakeATableThatTheUserCouldNotSeeForOneThatDidNotExist() throws Exception {
        // The catalog as of the cursor lists the tables that the user could see then. In a database without
        // CONNECT for public, that leaves out a table on which the user had no privilege yet. A lookup by name
        // tells such a table from one that did not exist: CockroachDB answers it with the missing privilege.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        doThrow(new SQLException("ERROR: user cdc does not have SELECT privilege on relation orders", "42501"))
                .when(cluster).executeQuery(contains("\"orders\" AS OF SYSTEM TIME"));

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(stored, "repro.reprodb.public.acct:0"), true);

        // orders existed at the cursor, so it is an added table: its changefeed starts at the cursor, without a scan.
        assertThat(createdChangefeeds(cluster)).hasSize(2).allSatisfy(statement -> assertThat(statement)
                .contains("cursor = '" + stored + "'", "initial_scan = 'no'"));
    }

    @Test
    void scansATableThatWasCreatedAfterTheConfiguredCursorOnlyOnceInInitialOnlyMode() throws Exception {
        // snapshot.mode=initial_only ends when the scan has ended, so no changefeed of it may go on streaming.
        final String configured = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });

        source(Map.of("snapshot.mode", "initial_only", "cockroachdb.changefeed.cursor", configured))
                .createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(configured), true);

        final List<String> created = createdChangefeeds(cluster);
        assertThat(created).hasSize(2);
        assertThat(created.get(0)).contains("\"acct\"", "initial_scan = 'only'", "cursor = '" + configured + "'");
        assertThat(created.get(1)).contains("\"orders\"", "initial_scan = 'only'", "cursor = '1795243641446371339.0000000000'")
                .doesNotContain("resolved", "updated");
    }

    @Test
    void doesNotCountTheChangesOfTheOtherTablesAsScanRowsWhileAnAddedTableIsScanned() throws Exception {
        // acct has a running changefeed without a cursor, so its scan rows cannot be told by their timestamp.
        final String stored = "1795243600000000000.0000000000";
        final String scanCursor = "1795243641446371339.0000000000";
        final Statement cluster = clusterListing(scanCursor, new String[][]{ { "public", "acct" } },
                new String[]{ "1021", "running", streaming("acct") });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0");
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());

        source.createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        // An insert into acct is a create. A scan phase of the connector would turn it into a read, and the
        // connector would not act on a signal that is inserted into the signal table.
        assertThat(source.extractOperation(ACCT, insertAt("1795243650000000000.0000000000"))).isEqualTo(Envelope.Operation.CREATE);
        assertThat(source.extractOperation(ACCT, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.CREATE);
        // The rows of the scan of orders carry the timestamp of the scan, and a row that is inserted afterwards a later one.
        assertThat(source.extractOperation(ORDERS, insertAt(scanCursor))).isEqualTo(Envelope.Operation.READ);
        assertThat(source.extractOperation(ORDERS, insertAt("1795243650000000000.0000000000"))).isEqualTo(Envelope.Operation.CREATE);
        // Without a timestamp, every create of the scanned table counts as a scan row until its first resolved timestamp.
        assertThat(source.extractOperation(ORDERS, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.READ);
        source.processChangefeedEvent(null, "{\"resolved\":\"1795243660000000000.0000000000\"}", ORDERS, offsets);
        assertThat(source.extractOperation(ORDERS, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.CREATE);
    }

    @Test
    void scansATableThatDidNotExistAtTheStoredCursorInEverySnapshotMode() throws Exception {
        final String stored = "1795243600000000000.0000000000";
        for (String snapshotMode : List.of("no_data", "when_needed", "always")) {
            final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });

            source(Map.of("snapshot.mode", snapshotMode)).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(stored), true);

            final List<String> created = createdChangefeeds(cluster);
            assertThat(created).as(snapshotMode).hasSize(2);
            // The table that existed at the cursor starts there, as the snapshot mode says. With when_needed
            // that is without a scan too: CockroachDB accepts the cursor for this table.
            assertThat(created.get(0)).as(snapshotMode).contains("\"acct\"", "cursor = '" + stored + "'",
                    "initial_scan = '" + ("always".equals(snapshotMode) ? "yes" : "no") + "'").doesNotContain("\"orders\"");
            assertThat(created.get(1)).as(snapshotMode).contains("\"orders\"", "initial_scan = 'yes'", "cursor = '1795243641446371339.0000000000'")
                    .doesNotContain("\"acct\"");
        }
    }

    @Test
    void startsAnAddedTableNowWhenItsPrivilegeIsNewerThanTheStoredCursor() throws Exception {
        // CockroachDB checks the privilege of the user as of the cursor of the changefeed. orders was added to
        // table.include.list after the privilege on it was granted, and the stored cursor is older than the grant.
        // acct was captured before: the offsets hold a position in its topic.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } });
        when(cluster.execute(anyString())).thenAnswer(invocation -> {
            final String statement = invocation.getArgument(0);
            if (statement.startsWith("CREATE CHANGEFEED") && statement.contains("\"orders\"") && statement.contains("cursor = '" + stored + "'")) {
                throw new SQLException("ERROR: user \"cdc\" requires the CHANGEFEED privilege on all target tables to be able to run an "
                        + "enterprise changefeed", "42501");
            }
            return false;
        });

        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0");
        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        final List<String> created = createdChangefeeds(cluster);
        assertThat(created).hasSize(3);
        // The table that was captured before resumes at the stored cursor, in a changefeed of its own.
        assertThat(created.get(0)).contains("\"acct\"", "cursor = '" + stored + "'").doesNotContain("\"orders\"");
        // The added table is tried at the stored cursor, which CockroachDB rejects, and starts at the current time.
        assertThat(created.get(1)).contains("\"orders\"", "cursor = '" + stored + "'").doesNotContain("\"acct\"");
        assertThat(created.get(2)).contains("\"orders\"", "initial_scan = 'no'", "cursor = '1795243641446371339.0000000000'")
                .doesNotContain("\"acct\"");
    }

    @Test
    void namesTheErrorOfCockroachDbOnceWhenAnAddedTableStartsAtTheCurrentTime() throws Exception {
        // The error can arrive with an explanation attached. The warning explains the same in its own words.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } });
        final SQLException rejection = new SQLException("ERROR: user \"cdc\" requires the CHANGEFEED privilege on all target tables", "42501");
        when(cluster.execute(anyString())).thenAnswer(invocation -> {
            final String statement = invocation.getArgument(0);
            if (statement.startsWith("CREATE CHANGEFEED") && statement.contains("cursor = '" + stored + "'")) {
                throw new SQLException(rejection.getMessage() + ". An explanation that was attached on the way.", "42501", rejection);
            }
            return false;
        });
        final Logger sourceLogger = (Logger) LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        sourceLogger.addAppender(log);
        try {
            source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsetsAt(stored, "repro.reprodb.public.acct:0"), true);

            assertThat(log.list).filteredOn(event -> event.getFormattedMessage().startsWith("CockroachDB rejected the stored cursor"))
                    .singleElement()
                    .satisfies(event -> assertThat(event.getFormattedMessage())
                            .contains("requires the CHANGEFEED privilege on all target tables")
                            .doesNotContain("An explanation that was attached"));
        }
        finally {
            sourceLogger.detachAppender(log);
        }
    }

    @Test
    void keepsTheStoredCursorWhenTheOffsetsDoNotTellWhichTablesWereCaptured() throws Exception {
        // Offsets with a cursor and without a position in a topic of this connector come from a sinkless
        // changefeed, or from before the sink topic prefix changed. They do not show that orders is new, so the
        // changes of orders since the cursor are owed, and another start time would leave them out.
        final String stored = "1795243600000000000.0000000000";
        for (String[] positions : new String[][]{ {}, { "earlier.reprodb.public.orders:0" } }) {
            final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "orders" } });
            final SQLException noPrivilege = new SQLException("ERROR: user \"cdc\" requires the CHANGEFEED privilege on all target tables", "42501");
            when(cluster.execute(anyString())).thenAnswer(invocation -> {
                if (((String) invocation.getArgument(0)).startsWith("CREATE CHANGEFEED")) {
                    throw noPrivilege;
                }
                return false;
            });

            assertThatThrownBy(() -> source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsetsAt(stored, positions), true))
                    .matches(failure -> failure == noPrivilege || failure.getCause() == noPrivilege, "is the rejection by CockroachDB");
            assertThat(createdChangefeeds(cluster)).hasSize(1);
        }
    }

    @Test
    void passesOnAMissingPrivilegeOnATableThatWasCapturedBefore() throws Exception {
        // The changes of such a table since the stored cursor are owed, so another start time is no way out.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        final SQLException noPrivilege = new SQLException("ERROR: user \"cdc\" requires the CHANGEFEED privilege on all target tables", "42501");
        when(cluster.execute(anyString())).thenAnswer(invocation -> {
            if (((String) invocation.getArgument(0)).startsWith("CREATE CHANGEFEED")) {
                throw noPrivilege;
            }
            return false;
        });

        assertThatThrownBy(() -> source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT),
                offsetsAt(stored, "repro.reprodb.public.acct:0"), true))
                .matches(failure -> failure == noPrivilege || failure.getCause() == noPrivilege, "is the rejection by CockroachDB");
        assertThat(createdChangefeeds(cluster)).hasSize(1);
    }

    @Test
    void keepsTheCursorThatTheConfigurationAsksFor() throws Exception {
        // On a first start with cockroachdb.changefeed.cursor, the changefeed is to start there. Another start
        // time would leave out the changes that the configuration asks for.
        final String configured = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "orders" } });
        final SQLException noPrivilege = new SQLException("ERROR: user \"cdc\" requires the CHANGEFEED privilege on all target tables", "42501");
        when(cluster.execute(anyString())).thenAnswer(invocation -> {
            if (((String) invocation.getArgument(0)).startsWith("CREATE CHANGEFEED")) {
                throw noPrivilege;
            }
            return false;
        });

        // The offsets hold a position of another table, so orders counts as added. They still hold the cursor
        // of the configuration, because no resolved timestamp has replaced it yet.
        assertThatThrownBy(() -> source(Map.of("cockroachdb.changefeed.cursor", configured))
                .createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsetsAt(configured, "repro.reprodb.public.acct:0"), true))
                .matches(failure -> failure == noPrivilege || failure.getCause() == noPrivilege, "is the rejection by CockroachDB");
        assertThat(createdChangefeeds(cluster)).hasSize(1);
    }

    @Test
    void passesOnAPrivilegeThatIsStillMissing() throws Exception {
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "orders" } });
        final SQLException noPrivilege = new SQLException("ERROR: user \"cdc\" requires the CHANGEFEED privilege on all target tables", "42501");
        when(cluster.execute(anyString())).thenAnswer(invocation -> {
            if (((String) invocation.getArgument(0)).startsWith("CREATE CHANGEFEED")) {
                throw noPrivilege;
            }
            return false;
        });

        assertThatThrownBy(() -> source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ORDERS),
                offsetsAt(stored, "repro.reprodb.public.acct:0"), true))
                .matches(failure -> failure == noPrivilege || failure.getCause() == noPrivilege, "is the rejection by CockroachDB");
        // At the stored cursor, and at the current time.
        assertThat(createdChangefeeds(cluster)).hasSize(2);
    }

    @Test
    void startsAllTablesAtTheStoredCursorWhenTheyExistedThen() throws Exception {
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } });

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(stored), true);

        assertThat(createdChangefeeds(cluster)).singleElement().satisfies(statement -> assertThat(statement)
                .contains("\"acct\"", "\"orders\"", "cursor = '" + stored + "'", "initial_scan = 'no'"));
    }

    @Test
    void stopsASinklessChangefeedThatCannotStartATableAtTheStoredCursor() throws Exception {
        // A sinkless changefeed starts all tables with one statement, at one cursor.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });

        assertThatThrownBy(() -> source(Map.of("cockroachdb.changefeed.sink.type", "sinkless"))
                .createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(stored), true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("[reprodb.public.orders] did not exist at the stored cursor " + stored)
                .hasMessageContaining("when_needed");

        // snapshot.mode=when_needed scans again when CockroachDB rejects the cursor, and a first start has no cursor.
        source(Map.of("cockroachdb.changefeed.sink.type", "sinkless", "snapshot.mode", "when_needed"))
                .createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(stored), true);
        source(Map.of("cockroachdb.changefeed.sink.type", "sinkless"))
                .createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(null), false);
        // And nothing stops a sinkless changefeed whose tables all existed at the cursor.
        final Statement allExisted = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } });
        source(Map.of("cockroachdb.changefeed.sink.type", "sinkless"))
                .createChangefeeds(connectionFor(allExisted), List.of(ACCT, ORDERS), offsetsAt(stored), true);
        assertThat(createdChangefeeds(cluster)).isEmpty();
    }

    @Test
    void leavesTheCursorToCockroachDbWhenTheCatalogOfThePastCannotBeRead() throws Exception {
        // For example when the cursor is older than the garbage collection threshold of the catalog.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        when(cluster.executeQuery(contains("information_schema.tables")))
                .thenThrow(new SQLException("ERROR: batch timestamp 1795243600.000000000,0 must be after replica GC threshold"));

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(stored), true);

        assertThat(createdChangefeeds(cluster)).singleElement().satisfies(statement -> assertThat(statement)
                .contains("\"acct\"", "\"orders\"", "cursor = '" + stored + "'"));
    }

    @Test
    void passesOnTheRejectionOfACursorForTablesThatExistedThen() throws Exception {
        // A cursor that is older than the garbage collection threshold is a different problem: the tables
        // existed, and their history is gone.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } });
        final SQLException tooOld = new SQLException("ERROR: batch timestamp 1795243600.000000000,0 must be after replica GC threshold 1795243630.000000000,0");
        when(cluster.execute(anyString())).thenAnswer(invocation -> {
            if (((String) invocation.getArgument(0)).startsWith("CREATE CHANGEFEED")) {
                throw tooOld;
            }
            return false;
        });

        assertThatThrownBy(() -> source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsetsAt(stored), true))
                .matches(failure -> failure == tooOld || failure.getCause() == tooOld, "is the rejection by CockroachDB");
        assertThat(createdChangefeeds(cluster)).hasSize(1);
    }

    /**
     * A statement of a cluster that lists the given changefeed jobs, answers the cluster timestamp query,
     * and returns the given tables for the catalog as of a past time.
     */
    private static Statement clusterListing(String clusterTimestamp, String[][] tablesInThePast, String[]... jobs) throws SQLException {
        final Statement statement = mock(Statement.class);
        final Connection jdbc = mock(Connection.class);
        when(jdbc.createStatement()).thenReturn(statement);
        when(jdbc.getAutoCommit()).thenReturn(true);
        when(statement.getConnection()).thenReturn(jdbc);
        when(statement.executeQuery(anyString())).thenAnswer(invocation -> {
            final String query = invocation.getArgument(0);
            if (query.contains("SHOW CHANGEFEED JOBS")) {
                return jobRows(jobs);
            }
            if (query.contains("cluster_logical_timestamp")) {
                return jobRows(new String[]{ clusterTimestamp });
            }
            if (query.contains("information_schema.tables")) {
                return jobRows(tablesInThePast);
            }
            if (query.startsWith("SELECT 1 FROM")) {
                for (String[] table : tablesInThePast) {
                    if (query.contains("\"" + table[0] + "\".\"" + table[1] + "\" AS OF SYSTEM TIME")) {
                        return jobRows();
                    }
                }
                throw new SQLException("ERROR: relation does not exist", "42P01");
            }
            return jobRows();
        });
        return statement;
    }

    private static List<String> createdChangefeeds(Statement cluster) throws SQLException {
        final ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        verify(cluster, atLeast(0)).execute(statements.capture());
        return statements.getAllValues().stream().filter(statement -> statement.startsWith("CREATE CHANGEFEED")).collect(Collectors.toList());
    }

    /** Offsets with the given cursor and with positions in the intermediate topics of the tables that were captured before. */
    private static CockroachDBOffsetContext offsetsAt(String cursor, String... topicPartitions) {
        final CockroachDBOffsetContext offsets = mock(CockroachDBOffsetContext.class);
        when(offsets.getCursor()).thenReturn(cursor);
        final Map<String, Long> positions = new HashMap<>();
        for (String topicPartition : topicPartitions) {
            positions.put(topicPartition, 5L);
        }
        when(offsets.getConsumerOffsets()).thenReturn(positions);
        return offsets;
    }

    @Test
    void namesEveryReusedChangefeedThatLacksTheUpdatedOption() throws Exception {
        // A changefeed that an earlier version of the connector created has no updated option unless
        // cockroachdb.changefeed.include.updated was enabled. The notice names the jobs and the way to add it.
        final String withoutUpdated = "CREATE CHANGEFEED FOR TABLE reprodb.public.orders INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', full_table_name, resolved = '3s')";
        final Logger sourceLogger = (Logger) LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        sourceLogger.addAppender(log);
        try {
            assertThat(source(Map.of()).changefeedExists(connectionListingJobs(
                    new String[]{ "1012", "running", WITHOUT_DIFF },
                    new String[]{ "1013", "running", withoutUpdated },
                    new String[]{ "1014", "running", scanning("other", "1.0000000000") }), ACCT, List.of(ACCT, ORDERS))).isTrue();

            assertThat(log.list).filteredOn(event -> event.getFormattedMessage().contains("without the updated option"))
                    .singleElement()
                    .satisfies(event -> assertThat(event.getFormattedMessage())
                            .contains("1012, 1013", "ALTER CHANGEFEED <job_id> SET updated")
                            .doesNotContain("1014"));
        }
        finally {
            sourceLogger.detachAppender(log);
        }
    }

    private static String scanning(String table, String cursor) {
        return "CREATE CHANGEFEED FOR TABLE reprodb.public." + table + " INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (cursor = '" + cursor + "', enriched_properties = 'source', envelope = 'enriched', full_table_name, "
                + "initial_scan = 'yes', resolved = '3s', updated)";
    }

    /** A changefeed without a cursor in its description: an operator created it, or an earlier version of the connector. */
    private static String streaming(String table) {
        return "CREATE CHANGEFEED FOR TABLE reprodb.public." + table + " INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', full_table_name, resolved = '3s', updated)";
    }

    private static JsonNode insertAt(String timestamp) throws Exception {
        return new ObjectMapper().readTree("{\"after\":{\"id\":1},\"op\":\"c\",\"source\":{\"ts_hlc\":\"" + timestamp + "\"}}");
    }

    private static JsonNode insertWithoutSourceBlock() throws Exception {
        return new ObjectMapper().readTree("{\"after\":{\"id\":1},\"op\":\"c\"}");
    }

    @SuppressWarnings("unchecked")
    private static EventDispatcher<CockroachDBPartition, TableId> dispatcher() {
        return mock(EventDispatcher.class);
    }

    private static CockroachDBStreamingChangeEventSource initialOnlySource() {
        return source(Map.of("snapshot.mode", "initial_only", "cockroachdb.changefeed.include.diff", "true"));
    }

    private static CockroachDBStreamingChangeEventSource sourceWith(String enabledOption) {
        return source(Map.of(enabledOption, "true"));
    }

    private static CockroachDBStreamingChangeEventSource source(Map<String, String> settings) {
        return source(settings, null);
    }

    private static CockroachDBStreamingChangeEventSource source(Map<String, String> settings, EventDispatcher<CockroachDBPartition, TableId> dispatcher) {
        Configuration.Builder config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "reprodb")
                .with("topic.prefix", "repro")
                .with("cockroachdb.changefeed.sink.type", "kafka")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
        settings.forEach((key, value) -> config.with(key, value));
        return new CockroachDBStreamingChangeEventSource(new CockroachDBConnectorConfig(config.build()), dispatcher, null, null, null);
    }

    private static CockroachDBConnection connectionListing(String jobId, String status, String description) throws Exception {
        return connectionListingJobs(new String[]{ jobId, status, description });
    }

    private static CockroachDBConnection connectionListingJobs(String[]... rows) throws Exception {
        ResultSet jobs = jobRows(rows);
        Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenReturn(jobs);
        return connectionFor(statement);
    }

    private static ResultSet jobRows(String[]... rows) throws SQLException {
        ResultSet jobs = mock(ResultSet.class);
        int[] row = { -1 };
        when(jobs.next()).thenAnswer(invocation -> ++row[0] < rows.length);
        when(jobs.getString(1)).thenAnswer(invocation -> rows[row[0]][0]);
        when(jobs.getString(2)).thenAnswer(invocation -> rows[row[0]][1]);
        when(jobs.getString(3)).thenAnswer(invocation -> rows[row[0]][2]);
        return jobs;
    }

    private static CockroachDBConnection connectionFor(Statement statement) throws SQLException {
        Connection jdbc = mock(Connection.class);
        when(jdbc.createStatement()).thenReturn(statement);
        CockroachDBConnection connection = mock(CockroachDBConnection.class);
        when(connection.connection()).thenReturn(jdbc);
        return connection;
    }

    private static SQLException sessionEnded() {
        return new SQLException("An I/O error occurred while sending to the backend.", "08006");
    }
}
