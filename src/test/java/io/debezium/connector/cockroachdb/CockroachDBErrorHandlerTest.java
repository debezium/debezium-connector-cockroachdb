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

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for CockroachDBErrorHandler.
 *
 * @author Virag Tripathi
 */
public class CockroachDBErrorHandlerTest {

    private CockroachDBErrorHandler errorHandler;
    private CockroachDBConnectorConfig config;

    @Before
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
    public void shouldIdentifyTransientErrors() {
        // Serialization failure (40001) - should be transient
        SQLException serializationError = new SQLException("serialization failure", "40001");
        assertThat(errorHandler.isTransientError(serializationError)).isTrue();

        // Deadlock detected (40P01) - should be transient
        SQLException deadlockError = new SQLException("deadlock detected", "40P01");
        assertThat(errorHandler.isTransientError(deadlockError)).isTrue();

        // Connection timeout - should be transient
        SQLException timeoutError = new SQLException("connection timeout", "08006");
        assertThat(errorHandler.isTransientError(timeoutError)).isTrue();
    }

    @Test
    public void shouldIdentifyNonTransientErrors() {
        // Syntax error (42601) - should not be transient
        SQLException syntaxError = new SQLException("syntax error", "42601");
        assertThat(errorHandler.isTransientError(syntaxError)).isFalse();

        // Table not found (42P01) - should not be transient
        SQLException tableNotFoundError = new SQLException("table not found", "42P01");
        assertThat(errorHandler.isTransientError(tableNotFoundError)).isFalse();

        // Permission denied (42501) - should not be transient
        SQLException permissionError = new SQLException("permission denied", "42501");
        assertThat(errorHandler.isTransientError(permissionError)).isFalse();
    }

    @Test
    public void shouldHandleErrorsWithoutSQLState() {
        // Error without SQL state - should not be transient
        SQLException genericError = new SQLException("generic error");
        assertThat(errorHandler.isTransientError(genericError)).isFalse();
    }

    @Test
    public void shouldHandleNullError() {
        assertThat(errorHandler.isTransientError(null)).isFalse();
    }

    @Test
    public void shouldHandleConnectionErrors() {
        // Only certain SQL states are considered transient
        SQLException connectionFailure = new SQLException("connection failure", "08000");
        assertThat(errorHandler.isTransientError(connectionFailure)).isTrue();

        SQLException doesNotExist = new SQLException("connection does not exist", "08003");
        assertThat(errorHandler.isTransientError(doesNotExist)).isTrue();

        SQLException failureDuringExecution = new SQLException("connection failure during execution", "08006");
        assertThat(errorHandler.isTransientError(failureDuringExecution)).isTrue();

        SQLException commLinkFailure = new SQLException("communication link failure", "08S01");
        assertThat(errorHandler.isTransientError(commLinkFailure)).isTrue();

        // Other connection-related SQL states are not considered transient
        SQLException connectionRefused = new SQLException("connection refused", "08001");
        assertThat(errorHandler.isTransientError(connectionRefused)).isFalse();
    }

    @Test
    public void shouldHandleResourceErrors() {
        // Resource temporarily unavailable is not considered transient by the handler
        SQLException resourceUnavailable = new SQLException("resource temporarily unavailable", "57014");
        assertThat(errorHandler.isTransientError(resourceUnavailable)).isFalse();
    }

    @Test
    public void shouldHandleCockroachDBSpecificErrors() {
        // CockroachDB specific retryable errors
        SQLException retryableError = new SQLException("retry: write too old", "40001");
        assertThat(errorHandler.isTransientError(retryableError)).isTrue();

        SQLException anotherRetryableError = new SQLException("retry: txn aborted", "40001");
        assertThat(errorHandler.isTransientError(anotherRetryableError)).isTrue();
    }
}