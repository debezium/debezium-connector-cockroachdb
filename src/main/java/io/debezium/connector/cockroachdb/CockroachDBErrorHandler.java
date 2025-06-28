/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

/**
 * Error handler for CockroachDB connector with retry logic for transient errors.
 * Handles serialization failures (40001) and other transient SQL errors.
 *
 * @author Virag Tripathi
 */
public class CockroachDBErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBErrorHandler.class);

    // CockroachDB specific error codes
    private static final String SERIALIZATION_FAILURE = "40001";
    private static final String DEADLOCK_DETECTED = "40P01";
    private static final String CONNECTION_FAILURE = "08000";
    private static final String CONNECTION_DOES_NOT_EXIST = "08003";
    private static final String CONNECTION_FAILURE_DURING_EXECUTION = "08006";
    private static final String COMMUNICATION_LINK_FAILURE = "08S01";

    private final CockroachDBConnectorConfig config;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final AtomicReference<Throwable> producerThrowable = new AtomicReference<>();

    public CockroachDBErrorHandler(CockroachDBConnectorConfig config, ChangeEventQueue<DataChangeEvent> queue) {
        this.config = config;
        this.queue = queue;
    }

    public void setProducerThrowable(Throwable producerThrowable) {
        this.producerThrowable.set(producerThrowable);
    }

    public Throwable getProducerThrowable() {
        return producerThrowable.get();
    }

    public void handle(Throwable throwable) {
        if (throwable instanceof SQLException) {
            SQLException sqlException = (SQLException) throwable;
            String sqlState = sqlException.getSQLState();

            if (isTransientError(sqlState)) {
                LOGGER.warn("Transient SQL error occurred: {} - {}. Will retry.", sqlState, sqlException.getMessage());
                // For transient errors, we don't set the producer throwable, allowing retry
                return;
            }
        }

        LOGGER.error("Error in CockroachDB connector: ", throwable);
        setProducerThrowable(throwable);
    }

    /**
     * Determines if a SQL error is transient and should be retried.
     *
     * @param sqlState the SQL state code
     * @return true if the error is transient and should be retried
     */
    private boolean isTransientError(String sqlState) {
        if (sqlState == null) {
            return false;
        }

        return SERIALIZATION_FAILURE.equals(sqlState) ||
                DEADLOCK_DETECTED.equals(sqlState) ||
                CONNECTION_FAILURE.equals(sqlState) ||
                CONNECTION_DOES_NOT_EXIST.equals(sqlState) ||
                CONNECTION_FAILURE_DURING_EXECUTION.equals(sqlState) ||
                COMMUNICATION_LINK_FAILURE.equals(sqlState);
    }

    /**
     * Determines if an exception is a transient error that should be retried.
     *
     * @param throwable the exception to check
     * @return true if the exception is transient and should be retried
     */
    public boolean isTransientError(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        if (throwable instanceof SQLException) {
            return isTransientError(((SQLException) throwable).getSQLState());
        }

        // Check for common transient network errors
        String message = throwable.getMessage();
        if (message != null) {
            message = message.toLowerCase();
            return message.contains("connection") ||
                    message.contains("timeout") ||
                    message.contains("network") ||
                    message.contains("retry") ||
                    message.contains("temporary");
        }

        return false;
    }
}