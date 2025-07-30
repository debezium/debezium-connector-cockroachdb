/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Tests for CockroachDB error handling.
 *
 * @author Virag Tripathi
 */
public class CockroachDBErrorHandlerTest {

    private CockroachDBErrorHandler errorHandler;
    private CockroachDBConnectorConfig config;

    @BeforeEach
    public void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        config = new CockroachDBConnectorConfig(Configuration.from(props));
        errorHandler = new CockroachDBErrorHandler(config, null);
    }

    @Test
    public void shouldHandleTransientErrors() throws InterruptedException {
        SQLException transientError = new SQLException("serialization failure", "40001");

        boolean shouldRetry = errorHandler.handleConnectionError(transientError, 1, 3, 1000);

        assertThat(shouldRetry).isTrue();
    }

    @Test
    public void shouldHandlePermanentErrors() throws InterruptedException {
        SQLException permanentError = new SQLException("permission denied", "42501");

        boolean shouldRetry = errorHandler.handleConnectionError(permanentError, 1, 3, 1000);

        assertThat(shouldRetry).isFalse();
    }

    @Test
    public void shouldHandleMaxRetriesExceeded() throws InterruptedException {
        SQLException transientError = new SQLException("serialization failure", "40001");

        boolean shouldRetry = errorHandler.handleConnectionError(transientError, 3, 3, 1000);

        assertThat(shouldRetry).isFalse();
    }

    @Test
    public void shouldHandleNullError() throws InterruptedException {
        boolean shouldRetry = errorHandler.handleConnectionError(null, 1, 3, 1000);

        assertThat(shouldRetry).isFalse();
    }

    @Test
    public void shouldHandleUnknownErrorCodes() throws InterruptedException {
        SQLException unknownError = new SQLException("unknown error", "99999");

        boolean shouldRetry = errorHandler.handleConnectionError(unknownError, 1, 3, 1000);

        assertThat(shouldRetry).isFalse();
    }

    @Test
    public void shouldHandleConnectionTimeout() throws InterruptedException {
        SQLException timeoutError = new SQLException("connection timeout", "08006");

        boolean shouldRetry = errorHandler.handleConnectionError(timeoutError, 1, 3, 1000);

        assertThat(shouldRetry).isTrue();
    }

    @Test
    public void shouldHandleNetworkErrors() throws InterruptedException {
        SQLException networkError = new SQLException("network error", "08006");

        boolean shouldRetry = errorHandler.handleConnectionError(networkError, 1, 3, 1000);

        assertThat(shouldRetry).isTrue();
    }
}
