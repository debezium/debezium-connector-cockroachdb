/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.CockroachDBConnectorConfig;
import io.debezium.connector.cockroachdb.CockroachDBErrorHandler;

/**
 * Manages JDBC connections to CockroachDB with retry logic for transient errors.
 * Handles connection establishment, configuration, and error recovery.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnection implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnection.class);

    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration RETRY_DELAY = Duration.ofMillis(100);
    private static final int MAX_RETRIES = 3;

    private final CockroachDBConnectorConfig config;
    private final CockroachDBErrorHandler errorHandler;
    private Connection connection;

    public CockroachDBConnection(CockroachDBConnectorConfig config) {
        this.config = config;
        this.errorHandler = new CockroachDBErrorHandler(config, null);
    }

    /**
     * Establishes a connection to CockroachDB with retry logic.
     *
     * @throws SQLException if connection cannot be established after retries
     */
    public void connect() throws SQLException {
        String url = buildConnectionUrl();
        Properties props = buildConnectionProperties();

        int attempts = 0;
        SQLException lastException = null;

        while (attempts < MAX_RETRIES) {
            try {
                LOGGER.info("Attempting to connect to CockroachDB (attempt {}/{}): {}",
                        attempts + 1, MAX_RETRIES, url);

                connection = DriverManager.getConnection(url, props);

                // Test the connection
                try (var stmt = connection.createStatement()) {
                    stmt.execute("SELECT 1");
                }

                LOGGER.info("Successfully connected to CockroachDB");
                return;

            }
            catch (SQLException e) {
                lastException = e;
                attempts++;

                if (errorHandler.isTransientError(e) && attempts < MAX_RETRIES) {
                    LOGGER.warn("Transient error connecting to CockroachDB (attempt {}/{}): {}",
                            attempts, MAX_RETRIES, e.getMessage());

                    try {
                        Thread.sleep(RETRY_DELAY.toMillis() * attempts); // Exponential backoff
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Connection interrupted", ie);
                    }
                }
                else {
                    break;
                }
            }
        }

        LOGGER.error("Failed to connect to CockroachDB after {} attempts", MAX_RETRIES);
        throw lastException != null ? lastException : new SQLException("Failed to connect to CockroachDB");
    }

    /**
     * Builds the JDBC connection URL for CockroachDB.
     *
     * @return the connection URL
     */
    private String buildConnectionUrl() {
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://");
        url.append(config.getHostname());
        url.append(":").append(config.getPort());
        url.append("/").append(config.getDatabaseName());

        // Add SSL configuration if needed
        CockroachDBConnectorConfig.SecureConnectionMode sslMode = CockroachDBConnectorConfig.SecureConnectionMode.parse(
                config.getSslMode());
        if (sslMode != CockroachDBConnectorConfig.SecureConnectionMode.DISABLED) {
            url.append("?sslmode=").append(sslMode.getValue());
        }

        return url.toString();
    }

    /**
     * Builds connection properties for the JDBC connection.
     *
     * @return connection properties
     */
    private Properties buildConnectionProperties() {
        Properties props = new Properties();

        // Basic connection properties
        props.setProperty("user", config.getUser());
        String password = config.getPassword();
        if (password != null) {
            props.setProperty("password", password);
        }

        // Connection timeout
        props.setProperty("connectTimeout", String.valueOf(CONNECTION_TIMEOUT.toSeconds()));

        // SSL properties if configured
        CockroachDBConnectorConfig.SecureConnectionMode sslMode = CockroachDBConnectorConfig.SecureConnectionMode.parse(
                config.getSslMode());
        if (sslMode != CockroachDBConnectorConfig.SecureConnectionMode.DISABLED) {
            if (config.getSslRootCert() != null) {
                props.setProperty("sslrootcert", config.getSslRootCert());
            }
            if (config.getSslClientCert() != null) {
                props.setProperty("sslcert", config.getSslClientCert());
            }
            if (config.getSslClientKey() != null) {
                props.setProperty("sslkey", config.getSslClientKey());
            }
            if (config.getSslClientKeyPassword() != null) {
                props.setProperty("sslpassword", config.getSslClientKeyPassword());
            }
        }

        // TCP keep-alive
        if (config.isTcpKeepAlive()) {
            props.setProperty("tcpKeepAlive", "true");
        }

        return props;
    }

    /**
     * Gets the underlying JDBC connection.
     *
     * @return the JDBC connection
     */
    public Connection connection() {
        return connection;
    }

    /**
     * Checks if the connection is valid.
     *
     * @return true if the connection is valid
     */
    public boolean isValid() {
        try {
            return connection != null && !connection.isClosed() && connection.isValid(5);
        }
        catch (SQLException e) {
            LOGGER.debug("Error checking connection validity: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                LOGGER.debug("Closed CockroachDB connection");
            }
            catch (SQLException e) {
                LOGGER.warn("Error closing CockroachDB connection: {}", e.getMessage());
            }
            finally {
                connection = null;
            }
        }
    }
}