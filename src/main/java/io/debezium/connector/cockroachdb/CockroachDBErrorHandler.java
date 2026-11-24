/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
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

    /**
     * Whether the connector starts again with an initial scan when CockroachDB rejects the cursor,
     * which is what {@code snapshot.mode=when_needed} asks for.
     */
    private final boolean scansAgainWhenTheCursorIsRejected;

    public CockroachDBErrorHandler(CockroachDBConnectorConfig connectorConfig,
                                   ChangeEventQueue<?> queue,
                                   ErrorHandler replacedErrorHandler) {
        super(CockroachDBConnector.class, connectorConfig, queue, replacedErrorHandler);
        this.scansAgainWhenTheCursorIsRejected = connectorConfig.getSnapshotMode() == CockroachDBConnectorConfig.SnapshotMode.WHEN_NEEDED;
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

    /**
     * Texts of errors that no retry can fix and that CockroachDB reports without a specific SQLSTATE, like
     * the errors that a retry can fix: a changefeed option that does not exist, a sink URI parameter that
     * does not exist, a sink that does not exist, and a table with more than one column family, which the
     * connector cannot capture.
     */
    private static final List<String> PERMANENT_ERROR_TEXTS = List.of(
            "invalid option \"",
            "sink query parameters",
            "unsupported sink:",
            CockroachDBStreamingChangeEventSource.MULTIPLE_COLUMN_FAMILIES_TEXT,
            CockroachDBStreamingChangeEventSource.COLUMN_FAMILY_ADDED_TEXT);

    /**
     * The text of the error for a changefeed whose cursor, or whose initial scan, is older than the garbage
     * collection threshold. CockroachDB reports it without a specific SQLSTATE as well. No retry at the
     * same cursor can fix it. With {@code snapshot.mode=when_needed} it stays retriable: the connector
     * scans again where it meets the rejection of a cursor, and a restart does the same. An initial scan
     * that ends with this error stops the task in every snapshot mode, with an error of its own.
     */
    private static final String CURSOR_TOO_OLD_TEXT = "must be after replica gc threshold";

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }

    /**
     * Returns {@code true} if the exception, or any cause in its chain, is an SQL error that no retry
     * can fix.
     */
    boolean isPermanentError(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException) {
                final String sqlState = ((SQLException) cause).getSQLState();
                if (INVALID_PASSWORD.equals(sqlState)
                        || sqlState != null && sqlState.length() >= 2 && PERMANENT_SQL_STATE_CLASSES.contains(sqlState.substring(0, 2))
                        || hasPermanentErrorText(cause.getMessage())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasPermanentErrorText(String message) {
        if (message == null) {
            return false;
        }
        final String normalized = message.toLowerCase(Locale.ROOT);
        return PERMANENT_ERROR_TEXTS.stream().anyMatch(normalized::contains)
                // A cursor that is older than one of the tables of the changefeed is reported without a specific
                // SQLSTATE as well. The cursor does not move while the connector cannot start, so no retry can
                // fix that either.
                || !scansAgainWhenTheCursorIsRejected
                        && (normalized.contains(CURSOR_TOO_OLD_TEXT) || CockroachDBStreamingChangeEventSource.isTableNewerThanCursorMessage(message));
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
