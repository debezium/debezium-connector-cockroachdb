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
                    .queueProvider(new DefaultQueueProvider<>(DEFAULT_MAX_QUEUE_SIZE))
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
}
