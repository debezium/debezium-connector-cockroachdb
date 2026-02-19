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
 * @author Virag Tripathi
 */
public class CockroachDBErrorHandler extends ErrorHandler {

    public CockroachDBErrorHandler(CockroachDBConnectorConfig connectorConfig,
                                   ChangeEventQueue<?> queue,
                                   ErrorHandler replacedErrorHandler) {
        super(CockroachDBConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }

    /**
     * Overridden with package-private visibility to allow unit testing of retriability classification.
     */
    @Override
    protected boolean isRetriable(Throwable throwable) {
        return super.isRetriable(throwable);
    }
}
