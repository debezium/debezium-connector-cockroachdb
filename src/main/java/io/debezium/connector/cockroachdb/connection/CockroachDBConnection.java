/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.CockroachDBConnectorConfig;
import io.debezium.jdbc.JdbcConnection;

/**
 * Manages JDBC connections to CockroachDB extending Debezium's {@link JdbcConnection}.
 * Preserves CockroachDB-specific retry logic for transient errors (serialization failures,
 * deadlocks, connection failures) while inheriting the full JdbcConnection API needed
 * by the incremental snapshot framework.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnection.class);

    private static final String SERIALIZATION_FAILURE = "40001";
    private static final String DEADLOCK_DETECTED = "40P01";
    private static final String CONNECTION_FAILURE = "08000";
    private static final String CONNECTION_DOES_NOT_EXIST = "08003";
    private static final String CONNECTION_FAILURE_DURING_EXECUTION = "08006";
    private static final String COMMUNICATION_LINK_FAILURE = "08S01";

    private final CockroachDBConnectorConfig connectorConfig;

    /**
     * Creates a CockroachDB-specific connection factory that handles SSL configuration,
     * CockroachDB-specific retry logic for transient errors, and connection validation.
     */
    private static ConnectionFactory createConnectionFactory(CockroachDBConnectorConfig config) {
        return jdbcConfig -> {
            String url = buildConnectionUrl(config);
            Properties props = buildConnectionProperties(config);

            int maxRetries = config.getConnectionMaxRetries();
            int attempts = 0;
            SQLException lastException = null;

            while (attempts < maxRetries) {
                try {
                    LOGGER.info("Connecting to CockroachDB (attempt {}/{}): {}:{}",
                            attempts + 1, maxRetries, config.getHostname(), config.getPort());
                    Connection conn = DriverManager.getConnection(url, props);
                    LOGGER.info("Successfully connected to CockroachDB");
                    return conn;
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
        };
    }

    public CockroachDBConnection(CockroachDBConnectorConfig config) {
        super(config.getJdbcConfig(), createConnectionFactory(config), "\"", "\"");
        this.connectorConfig = config;
    }

    @Override
    public Optional<Boolean> nullsSortLast() {
        // CockroachDB (PostgreSQL-compatible): NULLs sort last in ASC order
        return Optional.of(true);
    }

    /**
     * Validates that the connected user has the permissions required for changefeed operations.
     *
     * @throws SQLException if any required permission is missing
     */
    public void checkChangefeedPermissions() throws SQLException {
        if (connectorConfig.isSkipPermissionCheck()) {
            LOGGER.info("Skipping changefeed permission check as configured");
            return;
        }

        LOGGER.debug("Checking changefeed permissions for user: {}", connectorConfig.getUser());
        Connection conn = connection();

        try (var stmt = conn.createStatement()) {
            boolean rangefeedEnabled = false;
            try {
                stmt.execute("SHOW CLUSTER SETTING kv.rangefeed.enabled");
                var rs = stmt.getResultSet();
                if (rs != null && rs.next()) {
                    String rangefeedSetting = rs.getString(1);
                    rangefeedEnabled = "true".equalsIgnoreCase(rangefeedSetting) || "t".equalsIgnoreCase(rangefeedSetting);
                }
            }
            catch (SQLException e) {
                String msg = e.getMessage();
                if (msg != null && (msg.contains("VIEWCLUSTERSETTING") || msg.contains("MODIFYCLUSTERSETTING"))) {
                    LOGGER.warn("Cannot check rangefeed cluster setting due to insufficient privileges. " +
                            "Assuming rangefeed is enabled. If changefeeds fail, ensure 'kv.rangefeed.enabled = true' " +
                            "and grant VIEWCLUSTERSETTING to user '{}': GRANT VIEWCLUSTERSETTING TO {}",
                            connectorConfig.getUser(), connectorConfig.getUser());
                    rangefeedEnabled = true;
                }
                else {
                    throw e;
                }
            }

            if (!rangefeedEnabled) {
                throw new SQLException("Rangefeed is disabled. Enable with: SET CLUSTER SETTING kv.rangefeed.enabled = true;");
            }

            String schemaName = connectorConfig.getSchemaName();
            if (schemaName == null || schemaName.trim().isEmpty()) {
                schemaName = "public";
            }
            String safeSchema = validateIdentifier(schemaName);

            stmt.execute("SHOW GRANTS ON TABLE " + safeSchema + ".*");
            var rs = stmt.getResultSet();
            boolean hasChangefeedPrivilege = false;
            if (rs != null) {
                while (rs.next()) {
                    String grantee = rs.getString("grantee");
                    String privilegeType = rs.getString("privilege_type");
                    if (Objects.equals(connectorConfig.getUser(), grantee) && "CHANGEFEED".equals(privilegeType)) {
                        hasChangefeedPrivilege = true;
                        break;
                    }
                }
            }

            if (!hasChangefeedPrivilege) {
                throw new SQLException(
                        "User '" + connectorConfig.getUser() + "' lacks CHANGEFEED privilege on any table in schema '"
                                + safeSchema + "'. Grant with: GRANT CHANGEFEED ON TABLE " + safeSchema
                                + ".<table_name> TO " + connectorConfig.getUser() + ";");
            }

            try (var ps = conn.prepareStatement(
                    "SELECT 1 WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ? LIMIT 1)")) {
                ps.setString(1, schemaName);
                rs = ps.executeQuery();
                if (!rs.next()) {
                    throw new SQLException("No accessible tables found in schema '" + safeSchema
                            + "'. Ensure tables exist and user '" + connectorConfig.getUser() + "' has SELECT privilege.");
                }
            }
        }
    }

    private static String buildConnectionUrl(CockroachDBConnectorConfig config) {
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://");
        url.append(config.getHostname());
        url.append(":").append(config.getPort());
        url.append("/").append(config.getDatabaseName());

        CockroachDBConnectorConfig.SecureConnectionMode sslMode = parseSslMode(config);
        if (sslMode != CockroachDBConnectorConfig.SecureConnectionMode.DISABLED) {
            url.append("?sslmode=").append(sslMode.getValue());
        }
        return url.toString();
    }

    private static Properties buildConnectionProperties(CockroachDBConnectorConfig config) {
        Properties props = new Properties();

        String user = config.getUser();
        if (user != null) {
            props.setProperty("user", user);
        }
        String password = config.getPassword();
        if (password != null) {
            props.setProperty("password", password);
        }

        props.setProperty("connectTimeout", String.valueOf(config.getConnectionTimeoutMs() / 1000));

        CockroachDBConnectorConfig.SecureConnectionMode sslMode = parseSslMode(config);
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

    private static CockroachDBConnectorConfig.SecureConnectionMode parseSslMode(CockroachDBConnectorConfig config) {
        CockroachDBConnectorConfig.SecureConnectionMode mode = CockroachDBConnectorConfig.SecureConnectionMode.parse(config.getSslMode());
        return mode != null ? mode : CockroachDBConnectorConfig.SecureConnectionMode.PREFER;
    }

    private static String validateIdentifier(String identifier) throws SQLException {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new SQLException("Schema/table identifier must not be null or empty");
        }
        if (identifier.contains("\"") || identifier.contains("\0")) {
            throw new SQLException("Schema/table identifier '" + identifier
                    + "' contains disallowed characters (double-quote or null byte).");
        }
        if (!identifier.matches("[a-zA-Z0-9_]+")) {
            return "\"" + identifier + "\"";
        }
        return identifier;
    }

    private static boolean isTransientError(SQLException e) {
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
}
