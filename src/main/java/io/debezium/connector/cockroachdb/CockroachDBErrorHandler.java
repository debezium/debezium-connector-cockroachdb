/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for the CockroachDB connector.
 *
 * <p>Extends Debezium's {@link ErrorHandler} to classify both {@link java.io.IOException}
 * and {@link java.sql.SQLException} as retriable communication exceptions. This enables
 * the framework to automatically restart the connector on transient failures such as
 * connection resets, serialization conflicts (SQL state 40001), and network errors.</p>
 *
 * <p>An SQL error that no retry can fix, such as a missing privilege or an invalid changefeed
 * option, is not retriable: the task fails with CockroachDB's message instead of running into
 * the same error after every restart.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBErrorHandler extends ErrorHandler {

    public CockroachDBErrorHandler(CockroachDBConnectorConfig connectorConfig,
                                   ChangeEventQueue<?> queue,
                                   ErrorHandler replacedErrorHandler) {
        super(CockroachDBConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    /**
     * SQLSTATE classes of errors that no retry can fix: feature not supported (0A), data exception (22,
     * which CockroachDB uses for an invalid changefeed option value), invalid catalog name (3D), and
     * syntax error or access rule violation (42, which includes a missing privilege). CockroachDB
     * reports errors that a retry can fix, such as an unreachable sink, without a specific class.
     */
    private static final Set<String> PERMANENT_SQL_STATE_CLASSES = Collect.unmodifiableSet("0A", "22", "3D", "42");

    /**
     * A wrong password. The rest of class 28 stays retriable: CockroachDB answers a login with 28000
     * also when it cannot load the user, for example while a system range is unavailable.
     */
    private static final String INVALID_PASSWORD = "28P01";

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }

    /**
     * Returns {@code true} if the exception, or any cause in its chain, is an SQL error that no retry
     * can fix.
     */
    static boolean isPermanentError(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException) {
                final String sqlState = sqlException.getSQLState();
                if (INVALID_PASSWORD.equals(sqlState)
                        || sqlState != null && sqlState.length() >= 2 && PERMANENT_SQL_STATE_CLASSES.contains(sqlState.substring(0, 2))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Overridden with package-private visibility to allow unit testing of retriability classification.
     */
    @Override
    protected boolean isRetriable(Throwable throwable) {
        // Checked first, so that the task fails instead of retrying forever.
        if (isPermanentError(throwable)) {
            return false;
        }
        return super.isRetriable(throwable);
    }
}
