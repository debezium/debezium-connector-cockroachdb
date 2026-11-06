/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.relational.TableId;

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

    private static CockroachDBStreamingChangeEventSource initialOnlySource() {
        return new CockroachDBStreamingChangeEventSource(new CockroachDBConnectorConfig(baseConfig()
                .with("snapshot.mode", "initial_only")
                .with("cockroachdb.changefeed.include.diff", "true")
                .build()), null, null, null, null);
    }

    private static CockroachDBStreamingChangeEventSource defaultSource() {
        return new CockroachDBStreamingChangeEventSource(new CockroachDBConnectorConfig(baseConfig().build()), null, null, null, null);
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
