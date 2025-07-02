/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
        int maxRetries = config.getConnectionMaxRetries();

        while (attempts < maxRetries) {
            try {
                LOGGER.info("Attempting to connect to CockroachDB (attempt {}/{}): {}",
                        attempts + 1, maxRetries, url);

                connection = DriverManager.getConnection(url, props);

                // Test the connection
                try (var stmt = connection.createStatement()) {
                    stmt.execute("SELECT 1");
                }

                // Check permissions for changefeed operations
                checkChangefeedPermissions();

                LOGGER.info("Successfully connected to CockroachDB with required permissions");
                return;

            }
            catch (SQLException e) {
                lastException = e;
                attempts++;

                try {
                    long retryDelay = config.getConnectionRetryDelayMs() * attempts; // Exponential backoff
                    if (!errorHandler.handleConnectionError(e, attempts, maxRetries, retryDelay)) {
                        break; // Don't retry
                    }
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Connection interrupted", ie);
                }
            }
        }

        LOGGER.error("Failed to connect to CockroachDB after {} attempts", maxRetries);
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
        props.setProperty("connectTimeout", String.valueOf(config.getConnectionTimeoutMs() / 1000)); // Convert ms to seconds

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

    /**
     * Checks if the database user has the required permissions for changefeed operations.
     * This method performs early validation to fail fast if permissions are insufficient.
     *
     * @throws SQLException if permission checks fail
     */
    private void checkChangefeedPermissions() throws SQLException {
        LOGGER.debug("Checking changefeed permissions for user: {}", config.getUser());

        try (var stmt = connection.createStatement()) {
            // Check if rangefeed is enabled (gracefully handle permission errors)
            boolean rangefeedEnabled = false;
            try {
                stmt.execute("SHOW CLUSTER SETTING kv.rangefeed.enabled");
                var rs = stmt.getResultSet();
                if (rs != null && rs.next()) {
                    String rangefeedSetting = rs.getString(1);
                    rangefeedEnabled = "true".equalsIgnoreCase(rangefeedSetting);
                    LOGGER.debug("Rangefeed setting: {}", rangefeedSetting);
                }
            }
            catch (SQLException e) {
                if (e.getMessage().contains("VIEWCLUSTERSETTING") || e.getMessage().contains("MODIFYCLUSTERSETTING")) {
                    LOGGER.warn("Cannot check rangefeed cluster setting due to insufficient privileges. " +
                            "Assuming rangefeed is enabled. If changefeeds fail, ensure 'kv.rangefeed.enabled = true' " +
                            "and grant VIEWCLUSTERSETTING to user '{}': GRANT VIEWCLUSTERSETTING TO {}",
                            config.getUser(), config.getUser());
                    rangefeedEnabled = true; // Assume it's enabled and proceed
                }
                else {
                    throw e; // Re-throw other SQL exceptions
                }
            }

            if (!rangefeedEnabled) {
                throw new SQLException("Rangefeed is disabled. Enable rangefeed by setting 'kv.rangefeed.enabled = true' " +
                        "in your CockroachDB cluster configuration. This is required for changefeeds to work.");
            }

            // Check if user has CHANGEFEED privilege on at least one table
            String dbName = config.getDatabaseName();
            stmt.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables t " +
                    "JOIN information_schema.table_privileges tp ON t.table_name = tp.table_name " +
                    "WHERE t.table_schema = '" + config.getSchemaName() + "' AND tp.privilege_type = 'CHANGEFEED' " +
                    "AND tp.grantee = '" + config.getUser() + "' LIMIT 1)");
            var rs = stmt.getResultSet();
            if (rs != null && rs.next()) {
                boolean hasPrivilege = rs.getBoolean(1);
                if (!hasPrivilege) {
                    throw new SQLException("User '" + config.getUser() + "' lacks CHANGEFEED privilege on any table in database '" + dbName + "'. " +
                            "Grant the privilege with: GRANT CHANGEFEED ON TABLE table_name TO " + config.getUser());
                }
                LOGGER.debug("User has CHANGEFEED privilege on at least one table in database: {}", dbName);
            }

            // Check if user can create changefeeds (basic test)
            stmt.execute("SELECT 1 WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '" + config.getSchemaName() + "' LIMIT 1)");
            rs = stmt.getResultSet();
            if (rs != null && !rs.next()) {
                throw new SQLException("No accessible tables found in database '" + dbName + "'. " +
                        "Ensure the user has SELECT privilege on at least one table.");
            }

        }
        catch (SQLException e) {
            LOGGER.error("Permission check failed: {}", e.getMessage());
            throw e;
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
