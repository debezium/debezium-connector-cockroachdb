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

    private final CockroachDBErrorHandler errorHandler = new CockroachDBErrorHandler(
            new CockroachDBConnectorConfig(Configuration.create()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                    .with("database.hostname", "localhost")
                    .with("database.user", "root")
                    .with("database.dbname", "testdb")
                    .build()),
            new ChangeEventQueue.Builder<DataChangeEvent>()
                    .queueProvider(createDefaultQueueProvider(DEFAULT_MAX_QUEUE_SIZE))
                    .pollInterval(Duration.ofMillis(500))
                    .pollDispatchInterval(Duration.ofMillis(0))
                    .build(),
            null);

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
