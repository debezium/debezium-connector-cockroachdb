/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
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
 * Unit tests for the changefeed reuse option checks. CockroachDB fixes changefeed options at
 * creation time, so a reused changefeed must be validated against the connector configuration;
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
    void warnsWhenAReusedChangefeedWasCreatedWithSplitColumnFamilies() throws Exception {
        // The option splits a table that has more than one column family: CockroachDB writes it to one topic per
        // column family, and the connector reads the topic of the table. A table with one column family is not
        // split, so the changefeed is reused. The description is the one that CockroachDB shows for such a job.
        final String split = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', full_table_name, split_column_families, updated)";
        // The text in a value is no option.
        final String label = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', full_table_name, "
                + "metrics_label = e'a, split_column_families, b', resolved = '10s', updated)";
        final Logger sourceLogger = (Logger) LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        sourceLogger.addAppender(log);
        try {
            assertThat(source(Map.of()).changefeedExists(connectionListing("1021", "running", label), ACCT, List.of(ACCT))).isTrue();
            assertThat(log.list).noneMatch(event -> event.getFormattedMessage().contains("split_column_families"));

            assertThat(source(Map.of()).changefeedExists(connectionListing("1020", "running", split), ACCT, List.of(ACCT))).isTrue();
            assertThat(log.list).filteredOn(event -> event.getFormattedMessage().contains("split_column_families"))
                    .singleElement()
                    .satisfies(event -> assertThat(event.getFormattedMessage()).contains("1020", "more than one column family"));
        }
        finally {
            sourceLogger.detachAppender(log);
        }
    }

    @Test
    void retriesTheJobsQueryAfterReconnecting() throws Exception {
        // A node restart or drain ends the session, so the first query fails and a new connection finds the job.
        ResultSet jobs = jobRows("1006", "running", WITHOUT_DIFF);
        Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenThrow(sessionEnded()).thenReturn(jobs);
        CockroachDBConnection connection = connectionFor(statement);

        assertThat(defaultSource().changefeedExists(connection, ACCT, List.of(ACCT))).isTrue();
        verify(connection).close();
    }

    @Test
    void failsTheTaskWhenTheJobsQueryFailsAgainAfterReconnecting() throws Exception {
        // Creating a changefeed without knowing whether one already runs could start a second one that
        // double-produces into the same topics (debezium/dbz#3037). With the SQLException as the cause, the
        // error handler treats the failure as retriable, so the restarted task checks again.
        Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenThrow(sessionEnded());

        assertThatThrownBy(() -> defaultSource().changefeedExists(connectionFor(statement), ACCT, List.of(ACCT)))
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

        assertThatThrownBy(() -> defaultSource().changefeedExists(connection, ACCT, List.of(ACCT)))
                .isInstanceOf(DebeziumException.class)
                .hasCauseInstanceOf(SQLException.class);
    }

    private static final TableId ORDERS = new TableId("reprodb", "public", "orders");

    @Test
    void remembersTheCursorOfEveryRunningChangefeed() throws Exception {
        // The running changefeeds do not have to line up with the connector's groups of tables.
        CockroachDBStreamingChangeEventSource source = defaultSource();

        assertThat(source.changefeedExists(connectionListingJobs(
                new String[]{ "1007", "running", scanning("acct", "1.0000000000") },
                new String[]{ "1008", "running", scanning("orders", "2.0000000000") }), ACCT, List.of(ACCT, ORDERS))).isTrue();

        assertThat(source.scanTimestamp(ACCT)).isEqualByComparingTo("1");
        assertThat(source.scanTimestamp(ORDERS)).isEqualByComparingTo("2");
    }

    @Test
    void remembersNoCursorWhenNoChangefeedCoversTheTable() throws Exception {
        CockroachDBStreamingChangeEventSource source = defaultSource();

        assertThat(source.changefeedExists(connectionListing("1009", "running", scanning("acct", "1.0000000000")), ORDERS,
                List.of(ACCT, ORDERS))).isFalse();

        assertThat(source.scanTimestamp(ACCT)).isNull();
    }

    @Test
    void readsTheCursorsOfTheRunningChangefeedsOncePerStart() throws Exception {
        // Reading them again for every group of tables that has a running changefeed made the work grow
        // with the number of groups times the number of changefeeds times the number of tables.
        CockroachDBStreamingChangeEventSource source = defaultSource();
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
    void waitsUntilACanceledChangefeedHasStopped() throws Exception {
        // A job whose cancellation was requested writes to the intermediate topics until CockroachDB carries
        // the cancellation out. A changefeed that is created in the meantime would write the same events.
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        final ResultSet canceling = jobRows(new String[]{ "1031", "cancel-requested", scanning("acct", "1.0000000000") });
        final ResultSet gone = jobRows();
        when(cluster.executeQuery(contains("SHOW CHANGEFEED JOBS"))).thenReturn(canceling, gone);
        final CockroachDBStreamingChangeEventSource source = source(Map.of());
        source.cancellationWaitIntervalMs = 0;

        source.createChangefeeds(connectionFor(cluster), List.of(ACCT), offsetsAt(null), false);

        verify(cluster, times(2)).executeQuery(contains("SHOW CHANGEFEED JOBS"));
        assertThat(createdChangefeeds(cluster)).hasSize(1);
    }

    @Test
    void doesNotStartNextToAChangefeedThatIsStillBeingCanceled() throws Exception {
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        when(cluster.executeQuery(contains("SHOW CHANGEFEED JOBS")))
                .thenAnswer(invocation -> jobRows(new String[]{ "1032", "cancel-requested", scanning("acct", "1.0000000000") }));
        final CockroachDBStreamingChangeEventSource source = source(Map.of());
        source.cancellationWaitIntervalMs = 0;

        assertThatThrownBy(() -> source.createChangefeeds(connectionFor(cluster), List.of(ACCT), offsetsAt(null), false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1032")
                .hasMessageContaining("still being canceled");
        assertThat(createdChangefeeds(cluster)).isEmpty();
    }

    @Test
    void doesNotWaitForACanceledChangefeedOfAnotherConnector() throws Exception {
        final String other = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=other.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', full_table_name, resolved = '3s', updated)";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } },
                new String[]{ "1033", "cancel-requested", other });

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT), offsetsAt(null), false);

        verify(cluster, times(1)).executeQuery(contains("SHOW CHANGEFEED JOBS"));
        assertThat(createdChangefeeds(cluster)).hasSize(1);
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
        // The offsets remember where the changefeed of the new table started, for as long as the cursor is behind that.
        verify(offsets).setChangefeedStart("repro.reprodb.public.orders", "1795243641446371339.0000000000", true);
        verify(offsets, never()).setChangefeedStart(eq("repro.reprodb.public.acct"), anyString(), anyBoolean());
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
    void remembersTheStartOfAReusedChangefeedThatStartedLaterThanTheStoredCursor() throws Exception {
        // The run that created the changefeed of orders can have ended before a record stored its start in
        // the offsets. The description of the job has the cursor and tells whether it began with a scan.
        final String stored = "1795243600000000000.0000000000";
        final String later = "1795243620000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } },
                new String[]{ "1031", "running", scanning("acct", "1795243500000000000.0000000000") },
                new String[]{ "1032", "running", scanning("orders", later) });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0");
        final CockroachDBStreamingChangeEventSource source = source(Map.of());

        source.createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        assertThat(createdChangefeeds(cluster)).isEmpty();
        verify(offsets).setChangefeedStart("repro.reprodb.public.orders", later, true);
        // The changefeed of acct started before the stored cursor, so a new one for acct can start at the cursor.
        verify(offsets, never()).setChangefeedStart(eq("repro.reprodb.public.acct"), anyString(), anyBoolean());
        // The scan of orders can still be running.
        assertThat(source.extractOperation(ORDERS, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.READ);
        assertThat(source.extractOperation(ACCT, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.CREATE);

        // A changefeed that began without a scan, and offsets without a cursor to compare with.
        final Statement withoutScan = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } },
                new String[]{ "1033", "running", scanning("orders", later).replace("initial_scan = 'yes'", "initial_scan = 'no'") });
        final CockroachDBOffsetContext resumed = offsetsAt(stored);
        source(Map.of()).createChangefeeds(connectionFor(withoutScan), List.of(ORDERS), resumed, true);
        verify(resumed).setChangefeedStart("repro.reprodb.public.orders", later, false);
        final CockroachDBOffsetContext firstStart = offsetsAt(null);
        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), firstStart, false);
        verify(firstStart, never()).setChangefeedStart(anyString(), anyString(), anyBoolean());
    }

    @Test
    void followsAChangefeedThatIsBehindTheStoredCursor() throws Exception {
        // The stored cursor can be ahead of a changefeed. An earlier version of the connector stored the
        // resolved timestamp of any changefeed, and an operator can create the changefeed of an added table
        // with an earlier cursor. A new changefeed at the stored cursor would skip what that changefeed has not
        // emitted yet, so the cursor moves back to the resolved timestamp that all changefeeds have reached.
        final String stored = "1795243650000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        final CockroachDBOffsetContext offsets = offsetsAt(stored);
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());
        source.createChangefeeds(connectionFor(cluster), List.of(ACCT), offsets, true);

        source.processChangefeedEvent(null, "{\"resolved\":\"1795243640000000000.0000000000\"}", ACCT, offsets);

        verify(offsets).setCursor("1795243640000000000.0000000000");
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
    void startsATableAgainWhereItsChangefeedStartedWhileTheCursorIsBehindThatStart() throws Exception {
        // orders was created while the connector was stopped, and its changefeed started with a scan. That
        // changefeed ended before the cursor had reached the scan, so the cursor is still older than the table.
        // A scan as of now would miss the rows that were deleted since the changefeed ended.
        final String stored = "1795243600000000000.0000000000";
        final String firstScan = "1795243620000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0", "repro.reprodb.public.orders:0");
        when(offsets.getChangefeedStart("repro.reprodb.public.orders")).thenReturn(new CockroachDBOffsetContext.ChangefeedStart(firstScan, true));

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        final List<String> created = createdChangefeeds(cluster);
        assertThat(created).hasSize(2);
        assertThat(created).anySatisfy(statement -> assertThat(statement)
                .contains("\"orders\"", "initial_scan = 'yes'", "cursor = '" + firstScan + "'").doesNotContain("\"acct\""));
        assertThat(created).anySatisfy(statement -> assertThat(statement)
                .contains("\"acct\"", "initial_scan = 'no'", "cursor = '" + stored + "'").doesNotContain("\"orders\""));
    }

    @Test
    void startsATableAgainAtTheStartOfItsChangefeedWhileTheCursorIsOlderThanItsPrivilege() throws Exception {
        // The changefeed of orders started at a later time than the cursor, because the privilege on orders is
        // newer than the cursor. It ended before the cursor had reached that time. CockroachDB still rejects
        // the cursor for orders, and the offsets hold a position in its topic, so its changes are owed.
        final String stored = "1795243600000000000.0000000000";
        final String firstStart = "1795243620000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0", "repro.reprodb.public.orders:0");
        when(offsets.getChangefeedStart("repro.reprodb.public.orders")).thenReturn(new CockroachDBOffsetContext.ChangefeedStart(firstStart, false));

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        final List<String> created = createdChangefeeds(cluster);
        assertThat(created).hasSize(2);
        assertThat(created).anySatisfy(statement -> assertThat(statement)
                .contains("\"orders\"", "initial_scan = 'no'", "cursor = '" + firstStart + "'").doesNotContain("\"acct\""));
        assertThat(created).anySatisfy(statement -> assertThat(statement)
                .contains("\"acct\"", "initial_scan = 'no'", "cursor = '" + stored + "'").doesNotContain("\"orders\""));
    }

    @Test
    void forgetsTheStartOfAChangefeedOnceTheCursorHasReachedIt() throws Exception {
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        final CockroachDBOffsetContext offsets = offsetsAt(null);
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());
        source.createChangefeeds(connectionFor(cluster), List.of(ACCT), offsets, false);

        source.processChangefeedEvent(null, "{\"resolved\":\"1795243660000000000.0000000000\"}", ACCT, offsets);

        verify(offsets).setCursor("1795243660000000000.0000000000");
        verify(offsets).removeChangefeedStartsUpTo("1795243660000000000.0000000000");
    }

    @Test
    void keepsHowFarATableHasComeWhenTheCursorMovesBackBehindIt() throws Exception {
        // The changefeed of acct had ended, so this start created one at the stored cursor. The changefeed of
        // orders is behind the stored cursor, and the cursor moves back to what that one has reached. A
        // changefeed for acct that started there would go back further than acct has to: to a time that can be
        // older than what CockroachDB still has of acct, or older than acct or the privilege on it. So the
        // offsets keep how far acct has come.
        final String stored = "1795243650000000000.0000000000";
        final String behind = "1795243640000000000.0000000000";
        final String reached = "1795243660000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1041", "running", streaming("orders") });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0", "repro.reprodb.public.orders:0");
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());
        source.createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);
        assertThat(createdChangefeeds(cluster)).singleElement().satisfies(statement -> assertThat(statement)
                .contains("\"acct\"", "cursor = '" + stored + "'", "initial_scan = 'no'"));
        verify(offsets, never()).setChangefeedStart(anyString(), anyString(), anyBoolean());

        source.processChangefeedEvent(null, "{\"resolved\":\"" + reached + "\"}", ACCT, offsets);
        source.processChangefeedEvent(null, "{\"resolved\":\"" + behind + "\"}", ORDERS, offsets);

        verify(offsets).setCursor(behind);
        verify(offsets).setChangefeedStart("repro.reprodb.public.acct", reached, false);
        // orders is the table that is behind: a changefeed for it starts at the cursor.
        verify(offsets, never()).setChangefeedStart(eq("repro.reprodb.public.orders"), anyString(), anyBoolean());
    }

    @Test
    void keepsTheStartOfAChangefeedThatHasDeliveredNothingYetWhenTheCursorMovesBack() throws Exception {
        // The topic of acct still held a resolved timestamp of the changefeed that had ended. The new
        // changefeed of acct has delivered nothing yet, so where it started is what counts for acct.
        final String stored = "1795243650000000000.0000000000";
        final String behind = "1795243640000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1042", "running", streaming("orders") });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0", "repro.reprodb.public.orders:0");
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());
        source.createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        source.processChangefeedEvent(null, "{\"resolved\":\"1795243645000000000.0000000000\"}", ACCT, offsets);
        source.processChangefeedEvent(null, "{\"resolved\":\"" + behind + "\"}", ORDERS, offsets);

        verify(offsets).setCursor(behind);
        verify(offsets).setChangefeedStart("repro.reprodb.public.acct", stored, false);
    }

    @Test
    void keepsHowFarAReusedChangefeedHasComeWhenTheCursorMovesBackBehindIt() throws Exception {
        // The changefeed of orders started before the stored cursor, so the restart that finds it running
        // stores nothing about it. That changes when the cursor moves back to a time before that start. The
        // scan with which the changefeed began is through by then: orders has delivered a resolved timestamp.
        final String stored = "1795243650000000000.0000000000";
        final String startOfOrders = "1795243645000000000.0000000000";
        final String behind = "1795243640000000000.0000000000";
        final String reached = "1795243660000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1043", "running", streaming("acct") },
                new String[]{ "1044", "running", scanning("orders", startOfOrders) });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0", "repro.reprodb.public.orders:0");
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());
        source.createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);
        assertThat(createdChangefeeds(cluster)).isEmpty();
        verify(offsets, never()).setChangefeedStart(anyString(), anyString(), anyBoolean());

        source.processChangefeedEvent(null, "{\"resolved\":\"" + reached + "\"}", ORDERS, offsets);
        source.processChangefeedEvent(null, "{\"resolved\":\"" + behind + "\"}", ACCT, offsets);

        verify(offsets).setCursor(behind);
        verify(offsets).setChangefeedStart("repro.reprodb.public.orders", reached, false);
        assertThat(source.extractOperation(ORDERS, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.CREATE);
    }

    @Test
    void keepsHowFarATableHasComeAlsoWhenTheStartOfItsChangefeedIsUnknown() throws Exception {
        // Neither description has a cursor. acct can be newer than what the changefeed of orders has reached.
        final String stored = "1795243650000000000.0000000000";
        final String behind = "1795243640000000000.0000000000";
        final String reached = "1795243660000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1045", "running", streaming("acct") },
                new String[]{ "1046", "running", streaming("orders") });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0", "repro.reprodb.public.orders:0");
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());
        source.createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        source.processChangefeedEvent(null, "{\"resolved\":\"" + reached + "\"}", ACCT, offsets);
        source.processChangefeedEvent(null, "{\"resolved\":\"" + behind + "\"}", ORDERS, offsets);

        verify(offsets).setChangefeedStart("repro.reprodb.public.acct", reached, false);
        verify(offsets, never()).setChangefeedStart(eq("repro.reprodb.public.orders"), anyString(), anyBoolean());
    }

    @Test
    void storesTheCursorAndThePointsOfTheOtherTablesTogetherWhenTheCursorMovesBack() throws Exception {
        // The same with the offsets as Kafka Connect stores them.
        final String stored = "1795243650000000000.0000000000";
        final String behind = "1795243640000000000.0000000000";
        final String reached = "1795243660000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1047", "running", streaming("acct") },
                new String[]{ "1048", "running", streaming("orders") });
        final CockroachDBOffsetContext offsets = new CockroachDBOffsetContext(connectorConfig(Map.of()));
        offsets.setCursor(stored);
        offsets.setConsumerOffset("repro.reprodb.public.acct:0", 5L);
        offsets.setConsumerOffset("repro.reprodb.public.orders:0", 5L);
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());
        source.createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        source.processChangefeedEvent(null, "{\"resolved\":\"" + reached + "\"}", ACCT, offsets);
        source.processChangefeedEvent(null, "{\"resolved\":\"" + behind + "\"}", ORDERS, offsets);

        assertThat(offsets.getOffset().get("offset.cursor")).isEqualTo(behind);
        assertThat(offsets.getOffset().get("changefeed.start.repro.reprodb.public.acct")).isEqualTo(reached);
        assertThat(offsets.getOffset()).doesNotContainKey("changefeed.start.repro.reprodb.public.orders");

        // The changefeed of orders catches up. Once the cursor has reached the point of acct, acct starts at the cursor again.
        source.processChangefeedEvent(null, "{\"resolved\":\"1795243670000000000.0000000000\"}", ORDERS, offsets);
        assertThat(offsets.getOffset().get("offset.cursor")).isEqualTo(reached);
        assertThat(offsets.getOffset()).doesNotContainKey("changefeed.start.repro.reprodb.public.acct");
    }

    @Test
    void keepsALaterPointThatTheOffsetsHoldForATable() throws Exception {
        // In an earlier run the cursor moved back, and the offsets kept how far orders had come. That is later
        // than the start of its changefeed, so its scan is through, and a changefeed that takes its place can
        // start at that point without a scan.
        final String stored = "1795243600000000000.0000000000";
        final String startOfOrders = "1795243620000000000.0000000000";
        final String reached = "1795243630000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } },
                new String[]{ "1049", "running", scanning("orders", startOfOrders) });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.orders:0");
        when(offsets.getChangefeedStart("repro.reprodb.public.orders")).thenReturn(new CockroachDBOffsetContext.ChangefeedStart(reached, false));
        final CockroachDBStreamingChangeEventSource source = source(Map.of());

        source.createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsets, true);

        verify(offsets, never()).setChangefeedStart(anyString(), anyString(), anyBoolean());
        assertThat(source.extractOperation(ORDERS, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.CREATE);
    }

    @Test
    void doesNotTakeAScanThatIsThroughForOneThatStillRuns() throws Exception {
        // The offsets hold the start of the changefeed of orders without a scan: orders had delivered a resolved
        // timestamp, so the scan with which the changefeed began was through. The description of the job still
        // says initial_scan = 'yes'. That must not bring the scan back.
        final String stored = "1795243600000000000.0000000000";
        final String startOfOrders = "1795243620000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } },
                new String[]{ "1050", "running", scanning("orders", startOfOrders) });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.orders:0");
        when(offsets.getChangefeedStart("repro.reprodb.public.orders")).thenReturn(new CockroachDBOffsetContext.ChangefeedStart(startOfOrders, false));
        final CockroachDBStreamingChangeEventSource source = source(Map.of());

        source.createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsets, true);

        verify(offsets, never()).setChangefeedStart(anyString(), anyString(), anyBoolean());
        assertThat(source.extractOperation(ORDERS, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.CREATE);
    }

    @Test
    void replacesALaterStartWithAScanThatTheOffsetsHoldForAnotherChangefeed() throws Exception {
        // A later point without a scan says how far the table has come. A later start with a scan is the start
        // of another changefeed of the table, and the one that runs now started earlier.
        final String stored = "1795243600000000000.0000000000";
        final String startOfOrders = "1795243620000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } },
                new String[]{ "1053", "running", scanning("orders", startOfOrders) });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.orders:0");
        when(offsets.getChangefeedStart("repro.reprodb.public.orders"))
                .thenReturn(new CockroachDBOffsetContext.ChangefeedStart("1795243630000000000.0000000000", true));

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsets, true);

        verify(offsets).setChangefeedStart("repro.reprodb.public.orders", startOfOrders, true);
    }

    @Test
    void dropsTheScanOfAStartOnceTheTableHasDeliveredAResolvedTimestamp() throws Exception {
        // The first resolved timestamp of a changefeed that began with a scan is the timestamp of the scan. It
        // says that the scan is through.
        final String stored = "1795243650000000000.0000000000";
        final String startOfOrders = "1795243645000000000.0000000000";
        final String behind = "1795243640000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } },
                new String[]{ "1054", "running", streaming("acct") },
                new String[]{ "1055", "running", scanning("orders", startOfOrders) });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0", "repro.reprodb.public.orders:0");
        final CockroachDBStreamingChangeEventSource source = source(Map.of(), dispatcher());
        source.createChangefeeds(connectionFor(cluster), List.of(ACCT, ORDERS), offsets, true);

        source.processChangefeedEvent(null, "{\"resolved\":\"" + startOfOrders + "\"}", ORDERS, offsets);
        source.processChangefeedEvent(null, "{\"resolved\":\"" + behind + "\"}", ACCT, offsets);

        verify(offsets).setChangefeedStart("repro.reprodb.public.orders", startOfOrders, false);
    }

    @Test
    void keepsTheScanOfAChangefeedThatStartsAgainWithOne() throws Exception {
        // With snapshot.mode=always every changefeed that a start creates scans. The offsets held the start of
        // orders without a scan, and the changefeed that starts there again begins with one.
        final String stored = "1795243600000000000.0000000000";
        final String startOfOrders = "1795243620000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" }, { "public", "orders" } });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.orders:0");
        when(offsets.getChangefeedStart("repro.reprodb.public.orders")).thenReturn(new CockroachDBOffsetContext.ChangefeedStart(startOfOrders, false));

        source(Map.of("snapshot.mode", "always")).createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsets, true);

        assertThat(createdChangefeeds(cluster)).singleElement().satisfies(statement -> assertThat(statement)
                .contains("\"orders\"", "cursor = '" + startOfOrders + "'", "initial_scan = 'yes'"));
        verify(offsets).setChangefeedStart("repro.reprodb.public.orders", startOfOrders, true);
    }

    @Test
    void keepsTheScanOfAStoredStartWhenTheChangefeedWasAltered() throws Exception {
        // ALTER CHANGEFEED takes the initial_scan option out of the description of a job and keeps the cursor.
        // It is the statement that adds an option to a reused changefeed. The description then does not tell
        // how the changefeed began, and the offsets do.
        final String stored = "1795243600000000000.0000000000";
        final String later = "1795243620000000000.0000000000";
        final String altered = scanning("orders", later).replace("initial_scan = 'yes', ", "");
        assertThat(altered).doesNotContain("initial_scan").contains("cursor = '" + later + "'");
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } },
                new String[]{ "1051", "running", altered });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.orders:0");
        when(offsets.getChangefeedStart("repro.reprodb.public.orders")).thenReturn(new CockroachDBOffsetContext.ChangefeedStart(later, true));
        final CockroachDBStreamingChangeEventSource source = source(Map.of());

        source.createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsets, true);

        verify(offsets, never()).setChangefeedStart(eq("repro.reprodb.public.orders"), anyString(), eq(false));
        // The scan can still be running.
        assertThat(source.extractOperation(ORDERS, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.READ);

        // A stored start without a scan stays one without a scan.
        final CockroachDBOffsetContext withoutScan = offsetsAt(stored, "repro.reprodb.public.orders:0");
        when(withoutScan.getChangefeedStart("repro.reprodb.public.orders")).thenReturn(new CockroachDBOffsetContext.ChangefeedStart(later, false));
        final CockroachDBStreamingChangeEventSource resumed = source(Map.of());
        resumed.createChangefeeds(connectionFor(cluster), List.of(ORDERS), withoutScan, true);
        verify(withoutScan, never()).setChangefeedStart(eq("repro.reprodb.public.orders"), anyString(), eq(true));
        assertThat(resumed.extractOperation(ORDERS, insertWithoutSourceBlock())).isEqualTo(Envelope.Operation.CREATE);
    }

    @Test
    void takesAnAlteredChangefeedWithoutAStoredStartForOneThatBeganWithAScan() throws Exception {
        // Neither the description nor the offsets tell how the changefeed began. A changefeed that takes its
        // place has to deliver what a scan that was not through yet had left, so it scans.
        final String stored = "1795243600000000000.0000000000";
        final String later = "1795243620000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } },
                new String[]{ "1052", "running", scanning("orders", later).replace("initial_scan = 'yes', ", "") });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.orders:0");

        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ORDERS), offsets, true);

        verify(offsets).setChangefeedStart("repro.reprodb.public.orders", later, true);

        // An earlier start that the offsets hold for another changefeed of the table says nothing about this one.
        final CockroachDBOffsetContext earlier = offsetsAt(stored, "repro.reprodb.public.orders:0");
        when(earlier.getChangefeedStart("repro.reprodb.public.orders"))
                .thenReturn(new CockroachDBOffsetContext.ChangefeedStart("1795243610000000000.0000000000", false));
        source(Map.of()).createChangefeeds(connectionFor(cluster), List.of(ORDERS), earlier, true);
        verify(earlier).setChangefeedStart("repro.reprodb.public.orders", later, true);
    }

    @Test
    void readsFromTheDescriptionOfAJobWhetherItsChangefeedBeganWithAScan() {
        final String scanning = scanning("orders", "1795243620000000000.0000000000");
        assertThat(CockroachDBStreamingChangeEventSource.scanAtStartOf(scanning))
                .isEqualTo(CockroachDBStreamingChangeEventSource.ScanAtStart.YES);
        assertThat(CockroachDBStreamingChangeEventSource.scanAtStartOf(scanning.replace("initial_scan = 'yes'", "initial_scan")))
                .isEqualTo(CockroachDBStreamingChangeEventSource.ScanAtStart.YES);
        // A changefeed that ends after its scan begins with one as well.
        assertThat(CockroachDBStreamingChangeEventSource.scanAtStartOf(scanning.replace("initial_scan = 'yes'", "initial_scan = 'only'")))
                .isEqualTo(CockroachDBStreamingChangeEventSource.ScanAtStart.YES);
        assertThat(CockroachDBStreamingChangeEventSource.scanAtStartOf(scanning.replace("initial_scan = 'yes'", "initial_scan = 'no'")))
                .isEqualTo(CockroachDBStreamingChangeEventSource.ScanAtStart.NO);
        assertThat(CockroachDBStreamingChangeEventSource.scanAtStartOf(scanning.replace("initial_scan = 'yes'", "no_initial_scan")))
                .isEqualTo(CockroachDBStreamingChangeEventSource.ScanAtStart.NO);
        // ALTER CHANGEFEED leaves no option about the scan in the description.
        assertThat(CockroachDBStreamingChangeEventSource.scanAtStartOf(scanning.replace("initial_scan = 'yes', ", "")))
                .isEqualTo(CockroachDBStreamingChangeEventSource.ScanAtStart.UNKNOWN);
        assertThat(CockroachDBStreamingChangeEventSource.scanAtStartOf(null)).isEqualTo(CockroachDBStreamingChangeEventSource.ScanAtStart.UNKNOWN);
    }

    @Test
    void keepsTheStartOfTheScanThatAnswersARejectedCursor() throws Exception {
        // snapshot.mode=when_needed answers a stored cursor that CockroachDB rejects with a new initial scan, as
        // of the current time. The stored cursor stays the rejected one until the scan is through. A changefeed
        // that has to be created again in that time starts with that scan again, and not with a scan as of a
        // later time, which would not show the rows that were deleted in between.
        final String stored = "1795243600000000000.0000000000";
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        when(cluster.execute(anyString())).thenAnswer(invocation -> {
            final String statement = invocation.getArgument(0);
            if (statement.startsWith("CREATE CHANGEFEED") && statement.contains("cursor = '" + stored + "'")) {
                throw new SQLException("ERROR: batch timestamp 1795243600.000000000,0 must be after replica GC threshold 1795243630.000000000,0");
            }
            return false;
        });
        final CockroachDBOffsetContext offsets = offsetsAt(stored, "repro.reprodb.public.acct:0");

        source(Map.of("snapshot.mode", "when_needed")).createChangefeeds(connectionFor(cluster), List.of(ACCT), offsets, true);

        assertThat(createdChangefeeds(cluster)).hasSize(2);
        verify(offsets).setChangefeedStart("repro.reprodb.public.acct", "1795243641446371339.0000000000", true);
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
        // The offsets remember that start: until the cursor has reached it, CockroachDB rejects the cursor for orders.
        verify(offsets).setChangefeedStart("repro.reprodb.public.orders", "1795243641446371339.0000000000", false);
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
            assertThat(defaultSource().changefeedExists(connectionListingJobs(
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

    @Test
    void namesEveryReusedChangefeedThatSendsNoSourceBlock() throws Exception {
        final String withoutSource = "CREATE CHANGEFEED FOR TABLE reprodb.public.orders INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (envelope = 'enriched', full_table_name, resolved = '3s', updated)";
        final String withSourceAndSchema = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'schema,source', envelope = 'enriched', full_table_name, resolved = '3s', updated)";
        final Logger sourceLogger = (Logger) LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        sourceLogger.addAppender(log);
        try {
            assertThat(source(Map.of()).changefeedExists(connectionListingJobs(
                    new String[]{ "1029", "running", withSourceAndSchema },
                    new String[]{ "1030", "running", withoutSource }), ACCT, List.of(ACCT, ORDERS))).isTrue();

            assertThat(log.list).filteredOn(event -> event.getFormattedMessage().contains("does not include source"))
                    .singleElement()
                    .satisfies(event -> assertThat(event.getFormattedMessage())
                            .contains("job(s) 1030,", "ALTER CHANGEFEED <job_id> SET enriched_properties = 'source'",
                                    "replaces the value of enriched_properties", "enriched_properties = 'schema,source'")
                            .doesNotContain("1029"));
        }
        finally {
            sourceLogger.detachAppender(log);
        }
    }

    @Test
    void warnsWhenTheEnrichedPropertiesLeaveOutTheSourceBlock() throws Exception {
        // The timestamp of an event is in its source block.
        final Statement cluster = clusterListing("1795243641446371339.0000000000", new String[][]{ { "public", "acct" } });
        final Logger sourceLogger = (Logger) LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        sourceLogger.addAppender(log);
        try {
            source(Map.of("cockroachdb.changefeed.enriched.properties", "schema"))
                    .createChangefeeds(connectionFor(cluster), List.of(ACCT), offsetsAt(null), false);
            source(Map.of("cockroachdb.changefeed.enriched.properties", "schema, source"))
                    .createChangefeeds(connectionFor(cluster), List.of(ACCT), offsetsAt(null), false);

            assertThat(log.list).filteredOn(event -> event.getFormattedMessage().contains("without 'source'"))
                    .singleElement()
                    .satisfies(event -> assertThat(event.getFormattedMessage()).contains("cockroachdb.changefeed.enriched.properties is 'schema'"));
        }
        finally {
            sourceLogger.detachAppender(log);
        }
    }

    @Test
    void namesEveryReusedChangefeedThatLacksTheResolvedOption() throws Exception {
        // The stored cursor is the resolved timestamp that all changefeeds have reached. A changefeed that
        // reports none keeps it where it is, so the connector names the jobs and the way to add the option.
        final String withoutResolved = "CREATE CHANGEFEED FOR TABLE reprodb.public.orders INTO 'kafka://kafka:9092?topic_prefix=repro.' "
                + "WITH OPTIONS (enriched_properties = 'source', envelope = 'enriched', full_table_name, updated)";
        final Logger sourceLogger = (Logger) LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        sourceLogger.addAppender(log);
        try {
            assertThat(source(Map.of("cockroachdb.changefeed.resolved.interval", "5s")).changefeedExists(connectionListingJobs(
                    new String[]{ "1023", "running", scanning("acct", "1.0000000000") },
                    new String[]{ "1024", "running", withoutResolved }), ACCT, List.of(ACCT, ORDERS))).isTrue();

            assertThat(log.list).filteredOn(event -> event.getFormattedMessage().contains("without the resolved option"))
                    .singleElement()
                    .satisfies(event -> assertThat(event.getFormattedMessage())
                            .contains("job(s) 1024,", "ALTER CHANGEFEED <job_id> SET resolved = '5s'")
                            .doesNotContain("1023"));
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
        return new CockroachDBStreamingChangeEventSource(new CockroachDBConnectorConfig(baseConfig()
                .with("snapshot.mode", "initial_only")
                .with("cockroachdb.changefeed.include.diff", "true")
                .build()), null, null, null, null);
    }

    private static CockroachDBStreamingChangeEventSource defaultSource() {
        return source(Map.of());
    }

    private static CockroachDBStreamingChangeEventSource source(Map<String, String> settings) {
        return source(settings, null);
    }

    private static CockroachDBStreamingChangeEventSource source(Map<String, String> settings, EventDispatcher<CockroachDBPartition, TableId> dispatcher) {
        return new CockroachDBStreamingChangeEventSource(connectorConfig(settings), dispatcher, null, null, null);
    }

    private static CockroachDBConnectorConfig connectorConfig(Map<String, String> settings) {
        Configuration.Builder config = baseConfig();
        settings.forEach((key, value) -> config.with(key, value));
        return new CockroachDBConnectorConfig(config.build());
    }

    private static Configuration.Builder baseConfig() {
        return Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "reprodb")
                .with("topic.prefix", "repro")
                .with("cockroachdb.changefeed.sink.type", "kafka")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092");
    }

    private static CockroachDBConnection connectionListing(String jobId, String status, String description) throws Exception {
        ResultSet jobs = jobRows(jobId, status, description);
        Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenReturn(jobs);
        return connectionFor(statement);
    }

    private static CockroachDBConnection connectionListingJobs(String[]... rows) throws Exception {
        ResultSet jobs = mock(ResultSet.class);
        int[] row = { -1 };
        when(jobs.next()).thenAnswer(invocation -> ++row[0] < rows.length);
        when(jobs.getString(1)).thenAnswer(invocation -> rows[row[0]][0]);
        when(jobs.getString(2)).thenAnswer(invocation -> rows[row[0]][1]);
        when(jobs.getString(3)).thenAnswer(invocation -> rows[row[0]][2]);
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

    private static ResultSet jobRows(String jobId, String status, String description) throws SQLException {
        ResultSet jobs = mock(ResultSet.class);
        when(jobs.next()).thenReturn(true, false);
        when(jobs.getString(1)).thenReturn(jobId);
        when(jobs.getString(2)).thenReturn(status);
        when(jobs.getString(3)).thenReturn(description);
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
