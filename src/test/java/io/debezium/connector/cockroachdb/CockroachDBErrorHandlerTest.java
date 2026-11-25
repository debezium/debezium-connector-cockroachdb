/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.pipeline.DataChangeEvent;

/**
 * Unit tests for {@link CockroachDBErrorHandler} verifying that communication
 * exceptions (SQL and IO) are classified as retriable while non-communication
 * exceptions are not.
 *
 * @author Virag Tripathi
 */
public class CockroachDBErrorHandlerTest {

    private final CockroachDBErrorHandler errorHandler = errorHandler("initial");

    private static CockroachDBErrorHandler errorHandler(String snapshotMode) {
        return new CockroachDBErrorHandler(
                new CockroachDBConnectorConfig(Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                        .with("database.hostname", "localhost")
                        .with("database.user", "root")
                        .with("database.dbname", "testdb")
                        .with("snapshot.mode", snapshotMode)
                        .build()),
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .queueProvider(createDefaultQueueProvider(DEFAULT_MAX_QUEUE_SIZE))
                        .pollInterval(Duration.ofMillis(500))
                        .pollDispatchInterval(Duration.ofMillis(0))
                        .build(),
                null);
    }

    @Test
    void sqlExceptionIsRetriable() {
        SQLException sqlException = new SQLException("connection reset", "08006");
        assertThat(errorHandler.isRetriable(sqlException)).isTrue();
    }

    @Test
    void serializationFailureIsRetriable() {
        SQLException serializationFailure = new SQLException("restart transaction", "40001");
        assertThat(errorHandler.isRetriable(serializationFailure)).isTrue();
    }

    @Test
    void ioExceptionIsRetriable() {
        IOException ioException = new IOException("connection refused");
        assertThat(errorHandler.isRetriable(ioException)).isTrue();
    }

    @Test
    void wrappedSqlExceptionIsRetriable() {
        SQLException sqlException = new SQLException("connection lost", "08003");
        RuntimeException wrapper = new RuntimeException("streaming failed", sqlException);
        assertThat(errorHandler.isRetriable(wrapper)).isTrue();
    }

    @Test
    void errorsThatNoRetryCanFixAreNotRetriable() {
        // Messages and SQLSTATEs as CockroachDB v26.4.1 returns them.
        final SQLException missingPrivilege = new SQLException("only users with the admin role or the EXTERNALIOIMPLICITACCESS system "
                + "privilege are allowed to access the specified kafka URI", "42501");
        final SQLException syntaxError = new SQLException("lexical error: trailing junk after numeric literal at or near \"10s\"", "42601");
        final SQLException invalidOptionValue = new SQLException("unknown enriched_properties: bogus, valid values are: source, schema", "22023");
        final SQLException wrongPassword = new SQLException("password authentication failed for user cdc", "28P01");
        final SQLException notSupported = new SQLException("unsupported in READ COMMITTED isolation", "0A000");
        final SQLException unknownDatabase = new SQLException("database \"nosuchdb\" does not exist", "3D000");

        assertThat(errorHandler.isRetriable(missingPrivilege)).isFalse();
        assertThat(errorHandler.isRetriable(syntaxError)).isFalse();
        assertThat(errorHandler.isRetriable(invalidOptionValue)).isFalse();
        assertThat(errorHandler.isRetriable(wrongPassword)).isFalse();
        assertThat(errorHandler.isRetriable(notSupported)).isFalse();
        assertThat(errorHandler.isRetriable(unknownDatabase)).isFalse();
        // The streaming source wraps the error.
        assertThat(errorHandler.isRetriable(new RuntimeException("Failed to stream changes from CockroachDB", missingPrivilege))).isFalse();
    }

    @Test
    void errorsWithoutASpecificSqlStateThatNoRetryCanFixAreNotRetriable() {
        // CockroachDB v25.4.21 and v26.4.1 report these with their uncategorized SQLSTATE, like an unreachable
        // broker, so only the text tells them apart.
        final SQLException unknownOption = new SQLException("ERROR: invalid option \"no_such_option\"", "XXUUU");
        final SQLException unknownSinkParameter = new SQLException("ERROR: unknown kafka sink query parameters: no_such_param", "XXUUU");
        final SQLException unknownSink = new SQLException("ERROR: unsupported sink: nosuch", "XXUUU");
        final SQLException cursorTooOld = new SQLException("ERROR: could not create changefeed: cursor 1795332885584348201.0000000000 is "
                + "older than the GC threshold 1795332894103672556: batch timestamp 1795332885.584348201,0 must be after replica GC "
                + "threshold 1795332894.103672556,0 (r79: /{Table/106-Max})\n  Hint: use a more recent cursor", "XXUUU");

        // A table that was created after the cursor: the cursor does not move while the connector cannot start.
        final SQLException tableNewerThanTheCursor = new SQLException("ERROR: failed to resolve targets in the CHANGEFEED stmt: table "
                + "\"e.public.created_later\" does not exist\n  Hint: do the targets exist at the specified cursor time "
                + "1795343265.665877422,0?", "XXUUU");

        assertThat(errorHandler.isRetriable(unknownOption)).isFalse();
        assertThat(errorHandler.isRetriable(unknownSinkParameter)).isFalse();
        assertThat(errorHandler.isRetriable(unknownSink)).isFalse();
        assertThat(errorHandler.isRetriable(cursorTooOld)).isFalse();
        assertThat(errorHandler.isRetriable(tableNewerThanTheCursor)).isFalse();
        // With snapshot.mode=when_needed the connector scans again when CockroachDB rejects the cursor, so
        // these two stay retriable.
        final CockroachDBErrorHandler whenNeeded = errorHandler("when_needed");
        assertThat(whenNeeded.isRetriable(cursorTooOld)).isTrue();
        assertThat(whenNeeded.isRetriable(tableNewerThanTheCursor)).isTrue();
        assertThat(whenNeeded.isRetriable(unknownOption)).isFalse();
        // The streaming source wraps the error.
        assertThat(errorHandler.isRetriable(new RuntimeException("Failed to stream changes from CockroachDB", unknownOption))).isFalse();
        // The driver can leave the SQLSTATE out.
        assertThat(errorHandler.isRetriable(new SQLException("ERROR: unsupported sink: nosuch"))).isFalse();
    }

    @Test
    void otherErrorsWithoutASpecificSqlStateStayRetriable() {
        // An operator can enable rangefeeds while the connector retries.
        final SQLException rangefeedsDisabled = new SQLException("ERROR: rangefeeds require the kv.rangefeed.enabled setting. See "
                + "https://www.cockroachlabs.com/docs/v25.4/create-and-configure-changefeeds.html#enable-rangefeeds", "XXUUU");
        // A table that was dropped after the connector looked for its tables. The next start finds the tables again.
        final SQLException tableGone = new SQLException("ERROR: failed to resolve targets in the CHANGEFEED stmt: table \"e.missing\" "
                + "does not exist", "XXUUU");
        final SQLException sinkDown = new SQLException("ERROR: kafka: client has run out of available brokers to talk to", "XXUUU");
        // CockroachDB adds its hint about the cursor to every failure to look the tables up, also to one that passes.
        final SQLException lookupInterrupted = new SQLException("ERROR: failed to resolve targets in the CHANGEFEED stmt: query execution "
                + "canceled\n  Hint: do the targets exist at the specified cursor time 1795343265.665877422,0?", "XXUUU");

        assertThat(errorHandler.isRetriable(rangefeedsDisabled)).isTrue();
        assertThat(errorHandler.isRetriable(tableGone)).isTrue();
        assertThat(errorHandler.isRetriable(sinkDown)).isTrue();
        assertThat(errorHandler.isRetriable(lookupInterrupted)).isTrue();
    }

    @Test
    void errorsThatARetryCanFixStayRetriable() {
        // CockroachDB reports an unreachable broker with its uncategorized SQLSTATE.
        final SQLException brokerUnreachable = new SQLException("unable to dial: dial tcp: lookup no-such-broker: no such host", "XXUUU");
        final SQLException withoutState = new SQLException("connection attempt failed");
        final SQLException serverShutdown = new SQLException("server is shutting down", "57P01");
        // CockroachDB answers a login with this SQLSTATE when it cannot load the user, for example while a
        // system range is unavailable. Unlike a wrong password (28P01), that passes.
        final SQLException userNotLoaded = new SQLException("internal error while retrieving user account", "28000");

        assertThat(errorHandler.isRetriable(brokerUnreachable)).isTrue();
        assertThat(errorHandler.isRetriable(withoutState)).isTrue();
        assertThat(errorHandler.isRetriable(serverShutdown)).isTrue();
        assertThat(errorHandler.isRetriable(userNotLoaded)).isTrue();
    }

    @Test
    void nonCommunicationExceptionNotRetriable() {
        Exception testException = new NullPointerException();
        assertThat(errorHandler.isRetriable(testException)).isFalse();
    }

    @Test
    void nullThrowableNotRetriable() {
        assertThat(errorHandler.isRetriable(null)).isFalse();
    }

    @Test
    void illegalArgumentExceptionNotRetriable() {
        IllegalArgumentException testException = new IllegalArgumentException("bad config");
        assertThat(errorHandler.isRetriable(testException)).isFalse();
    }

    private static DefaultQueueProvider<DataChangeEvent> createDefaultQueueProvider(int maxQueueSize) {
        DefaultQueueProvider<DataChangeEvent> provider = new DefaultQueueProvider<>();
        provider.configure(java.util.Map.of("max.queue.size", String.valueOf(maxQueueSize)));
        return provider;
    }
}
