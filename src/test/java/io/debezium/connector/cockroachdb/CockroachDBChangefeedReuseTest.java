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
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.relational.TableId;

/**
 * Unit tests for the changefeed reuse option checks. A reused changefeed has the options it was
 * created with, plus any added later with ALTER CHANGEFEED, so it must be validated against the
 * connector configuration;
 * silently reusing a changefeed without the {@code diff} option while
 * {@code cockroachdb.changefeed.include.diff} is enabled leaves update events without a before
 * image.
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

    private static CockroachDBStreamingChangeEventSource sourceWith(String enabledOption) {
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "reprodb")
                .with("topic.prefix", "repro")
                .with("cockroachdb.changefeed.sink.type", "kafka")
                .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
                .with(enabledOption, "true")
                .build();
        return new CockroachDBStreamingChangeEventSource(new CockroachDBConnectorConfig(config), null, null, null, null);
    }

    private static CockroachDBConnection connectionListing(String jobId, String status, String description) throws Exception {
        return connectionListingJobs(new String[]{ jobId, status, description });
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
        Connection jdbc = mock(Connection.class);
        when(jdbc.createStatement()).thenReturn(statement);
        CockroachDBConnection connection = mock(CockroachDBConnection.class);
        when(connection.connection()).thenReturn(jdbc);
        return connection;
    }
}
