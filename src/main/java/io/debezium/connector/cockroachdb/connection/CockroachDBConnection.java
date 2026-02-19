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

/**
 * Manages JDBC connections to CockroachDB with retry logic for transient errors.
 * Handles connection establishment, configuration, and error recovery.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnection implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnection.class);

    private static final String SERIALIZATION_FAILURE = "40001";
    private static final String DEADLOCK_DETECTED = "40P01";
    private static final String CONNECTION_FAILURE = "08000";
    private static final String CONNECTION_DOES_NOT_EXIST = "08003";
    private static final String CONNECTION_FAILURE_DURING_EXECUTION = "08006";
    private static final String COMMUNICATION_LINK_FAILURE = "08S01";

    private final CockroachDBConnectorConfig config;
    private Connection connection;

    public CockroachDBConnection(CockroachDBConnectorConfig config) {
        this.config = config;
    }

    /**
     * Establishes a JDBC connection to CockroachDB with linear-backoff retry logic
     * for transient errors. Validates the connection and checks changefeed permissions
     * before returning.
     *
     * @throws SQLException if the connection cannot be established after all retries
     */
    public void connect() throws SQLException {
        String url = buildConnectionUrl();
        Properties props = buildConnectionProperties();

        int attempts = 0;
        SQLException lastException = null;
        int maxRetries = config.getConnectionMaxRetries();

        while (attempts < maxRetries) {
            try {
                LOGGER.info("Attempting to connect to CockroachDB (attempt {}/{}): {}:{}",
                        attempts + 1, maxRetries, config.getHostname(), config.getPort());

                connection = DriverManager.getConnection(url, props);

                try (var stmt = connection.createStatement()) {
                    stmt.execute("SELECT 1");
                }

                if (!config.isSkipPermissionCheck()) {
                    checkChangefeedPermissions();
                }
                else {
                    LOGGER.info("Skipping changefeed permission check as configured");
                }

                LOGGER.info("Successfully connected to CockroachDB with required permissions");
                return;
            }
            catch (SQLException e) {
                lastException = e;
                attempts++;

                if (!isTransientError(e) || attempts >= maxRetries) {
                    break;
                }

                try {
                    long retryDelay = config.getConnectionRetryDelayMs() * attempts;
                    LOGGER.warn("Transient connection error (attempt {}/{}): {}. Retrying in {}ms...",
                            attempts, maxRetries, e.getMessage(), retryDelay);
                    Thread.sleep(retryDelay);
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
     * Builds the JDBC connection URL including SSL mode when configured.
     * CockroachDB uses the PostgreSQL wire protocol, so the URL scheme is {@code jdbc:postgresql://}.
     *
     * @return the fully-qualified JDBC connection URL
     */
    private String buildConnectionUrl() {
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://");
        url.append(config.getHostname());
        url.append(":").append(config.getPort());
        url.append("/").append(config.getDatabaseName());

        CockroachDBConnectorConfig.SecureConnectionMode sslMode = parseSslMode();
        if (sslMode != CockroachDBConnectorConfig.SecureConnectionMode.DISABLED) {
            url.append("?sslmode=").append(sslMode.getValue());
        }

        return url.toString();
    }

    /**
     * Parses the SSL mode from config, defaulting to PREFER if the value is unrecognized or null.
     */
    private CockroachDBConnectorConfig.SecureConnectionMode parseSslMode() {
        CockroachDBConnectorConfig.SecureConnectionMode mode = CockroachDBConnectorConfig.SecureConnectionMode.parse(config.getSslMode());
        return mode != null ? mode : CockroachDBConnectorConfig.SecureConnectionMode.PREFER;
    }

    /**
     * Builds JDBC connection properties including credentials, timeouts, SSL certificates,
     * and TCP keep-alive settings.
     *
     * @return the configured connection properties
     */
    private Properties buildConnectionProperties() {
        Properties props = new Properties();

        String user = config.getUser();
        if (user != null) {
            props.setProperty("user", user);
        }
        String password = config.getPassword();
        if (password != null) {
            props.setProperty("password", password);
        }

        // PostgreSQL JDBC driver expects connectTimeout in seconds
        props.setProperty("connectTimeout", String.valueOf(config.getConnectionTimeoutMs() / 1000));

        CockroachDBConnectorConfig.SecureConnectionMode sslMode = parseSslMode();
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

        if (config.isTcpKeepAlive()) {
            props.setProperty("tcpKeepAlive", "true");
        }

        return props;
    }

    /**
     * Returns the underlying JDBC connection, or {@code null} if not yet connected.
     *
     * @return the active JDBC connection
     */
    public Connection connection() {
        return connection;
    }

    /**
     * Checks whether the connection is open and responsive (with a 5-second timeout).
     *
     * @return {@code true} if the connection is usable
     */
    public boolean isValid() {
        try {
            return connection != null && !connection.isClosed()
                    && connection.isValid(config.getConnectionValidationTimeoutSeconds());
        }
        catch (SQLException e) {
            LOGGER.debug("Error checking connection validity: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Validates that the connected user has the permissions required for changefeed operations.
     * Checks in order: (1) {@code kv.rangefeed.enabled} cluster setting, (2) CHANGEFEED privilege
     * on at least one table, (3) existence of accessible tables in the target schema.
     *
     * <p>If the user lacks VIEWCLUSTERSETTING privilege, the rangefeed check is skipped with a
     * warning and rangefeed is assumed enabled.</p>
     *
     * @throws SQLException if any required permission is missing or unreachable
     */
    private void checkChangefeedPermissions() throws SQLException {
        LOGGER.debug("Checking changefeed permissions for user: {}", config.getUser());

        try (var stmt = connection.createStatement()) {
            boolean rangefeedEnabled = false;
            try {
                stmt.execute("SHOW CLUSTER SETTING kv.rangefeed.enabled");
                var rs = stmt.getResultSet();
                if (rs != null && rs.next()) {
                    String rangefeedSetting = rs.getString(1);
                    rangefeedEnabled = "true".equalsIgnoreCase(rangefeedSetting) || "t".equalsIgnoreCase(rangefeedSetting);
                    LOGGER.debug("Rangefeed setting: {}", rangefeedSetting);
                }
            }
            catch (SQLException e) {
                String msg = e.getMessage();
                if (msg != null && (msg.contains("VIEWCLUSTERSETTING") || msg.contains("MODIFYCLUSTERSETTING"))) {
                    LOGGER.warn("Cannot check rangefeed cluster setting due to insufficient privileges. " +
                            "Assuming rangefeed is enabled. If changefeeds fail, ensure 'kv.rangefeed.enabled = true' " +
                            "and grant VIEWCLUSTERSETTING to user '{}': GRANT VIEWCLUSTERSETTING TO {}",
                            config.getUser(), config.getUser());
                    rangefeedEnabled = true;
                }
                else {
                    throw e;
                }
            }

            if (!rangefeedEnabled) {
                throw new SQLException("Rangefeed is disabled. Enable rangefeed by setting 'kv.rangefeed.enabled = true' " +
                        "in your CockroachDB cluster configuration. This is required for changefeeds to work.");
            }

            String dbName = config.getDatabaseName();
            String schemaName = config.getSchemaName();
            if (schemaName == null || schemaName.trim().isEmpty()) {
                schemaName = "public";
                LOGGER.debug("Schema name was null or empty, defaulting to: {}", schemaName);
            }

            // Sanitize to prevent SQL injection -- identifiers allow only [a-zA-Z0-9_]
            String safeSchema = sanitizeIdentifier(schemaName);

            // SHOW GRANTS is more reliable than information_schema for CockroachDB privilege checks
            LOGGER.debug("Checking CHANGEFEED privileges for schema: {}", safeSchema);
            stmt.execute("SHOW GRANTS ON TABLE " + safeSchema + ".*");
            var rs = stmt.getResultSet();
            boolean hasChangefeedPrivilege = false;
            if (rs != null) {
                while (rs.next()) {
                    String grantee = rs.getString("grantee");
                    String privilegeType = rs.getString("privilege_type");
                    if (java.util.Objects.equals(config.getUser(), grantee) && "CHANGEFEED".equals(privilegeType)) {
                        hasChangefeedPrivilege = true;
                        break;
                    }
                }
            }

            if (!hasChangefeedPrivilege) {
                throw new SQLException(
                        "User '" + config.getUser() + "' lacks CHANGEFEED privilege on any table in database '"
                                + dbName + "' schema '" + safeSchema + "'. "
                                + "Grant the privilege with: GRANT CHANGEFEED ON TABLE "
                                + safeSchema + ".table_name TO " + config.getUser());
            }
            LOGGER.debug("User has CHANGEFEED privilege in database: {} schema: {}", dbName, safeSchema);

            // Verify at least one table exists in the target schema using a parameterized query
            try (var ps = connection.prepareStatement(
                    "SELECT 1 WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ? LIMIT 1)")) {
                ps.setString(1, schemaName);
                rs = ps.executeQuery();
                if (!rs.next()) {
                    throw new SQLException("No accessible tables found in database '" + dbName
                            + "' schema '" + safeSchema + "'. "
                            + "Ensure the user has SELECT privilege on at least one table.");
                }
            }
        }
        catch (SQLException e) {
            LOGGER.error("Permission check failed: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Sanitizes a SQL identifier by keeping only alphanumeric characters, underscores, and periods.
     * Prevents SQL injection when interpolating schema/table names into DDL statements.
     */
    private static String sanitizeIdentifier(String identifier) {
        if (identifier == null) {
            return "";
        }
        return identifier.replaceAll("[^a-zA-Z0-9_.]", "");
    }

    /**
     * Determines whether the given SQL exception represents a transient error
     * that warrants a connection retry. Matches against CockroachDB-specific SQL
     * states for serialization conflicts, deadlocks, and connection failures.
     *
     * @param e the SQL exception to evaluate
     * @return {@code true} if the error is transient and retriable
     */
    private boolean isTransientError(SQLException e) {
        String sqlState = e.getSQLState();
        if (sqlState == null) {
            return false;
        }
        return SERIALIZATION_FAILURE.equals(sqlState)
                || DEADLOCK_DETECTED.equals(sqlState)
                || CONNECTION_FAILURE.equals(sqlState)
                || CONNECTION_DOES_NOT_EXIST.equals(sqlState)
                || CONNECTION_FAILURE_DURING_EXECUTION.equals(sqlState)
                || COMMUNICATION_LINK_FAILURE.equals(sqlState);
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
