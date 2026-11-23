/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Threads;

/**
 * The main connector class used to instantiate configuration and execution classes
 * Uses CockroachDB's native changefeed (enriched envelope) as the source of truth.
 * <p>
 * * @author Virag Tripathi
 */
public class CockroachDBConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnector.class);

    /**
     * CockroachDB accepts a connection to a database that does not exist, and a query on the catalog of
     * that database fails. The query shows a wrong database name when the configuration is validated.
     */
    private static final String CONNECTION_TEST_QUERY = "SELECT 1 FROM information_schema.schemata LIMIT 1";

    private Map<String, String> config;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        if (props == null) {
            throw new IllegalArgumentException("Configuration properties cannot be null");
        }
        this.config = props;
        LOGGER.info("Starting CockroachDB connector");
    }

    @Override
    public Class<? extends SourceTask> taskClass() {
        return CockroachDBConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (config == null) {
            throw new IllegalStateException("Connector has not been started");
        }
        LOGGER.debug("Generating {} task config(s)", maxTasks);
        return List.of(config);
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping CockroachDBConnector.");
    }

    @Override
    public ConfigDef config() {
        return CockroachDBConnectorConfig.configDef();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        final Map<String, ConfigValue> results = config.validate(CockroachDBConnectorConfig.ALL_FIELDS);
        // Kafka Connect's check of the property types, which is all that ran before the connector validated its
        // fields. Unlike the field validators it loads the classes that class-typed properties name.
        for (ConfigValue typed : config().validate(config.asMap())) {
            final ConfigValue result = results.get(typed.name());
            if (result != null && result.errorMessages().isEmpty()) {
                typed.errorMessages().forEach(result::addErrorMessage);
            }
        }
        return results;
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final CockroachDBConnectorConfig connectorConfig;
        try {
            connectorConfig = new CockroachDBConnectorConfig(config);
        }
        catch (RuntimeException e) {
            hostnameValue.addErrorMessage("Error during connection validation: " + e.getMessage());
            return;
        }
        final Duration timeout = connectorConfig.getConnectionValidationTimeout();
        // The thread that connects only hands over its result. After a timeout it can still be running, and
        // the validation result must not change once this method has returned it.
        final AtomicReference<String> failure = new AtomicReference<>();
        try {
            Threads.runWithTimeout(CockroachDBConnector.class, () -> failure.set(testConnection(connectorConfig, timeout)), null,
                    timeout, connectorConfig.getLogicalName(), "connection-validation");
            if (failure.get() != null) {
                hostnameValue.addErrorMessage("Unable to connect: " + failure.get());
            }
        }
        catch (TimeoutException e) {
            hostnameValue.addErrorMessage("Connection validation timed out after " + timeout.toMillis() + " ms");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            hostnameValue.addErrorMessage("Connection validation was interrupted");
        }
        catch (Exception e) {
            hostnameValue.addErrorMessage("Error during connection validation: " + e.getMessage());
        }
    }

    /**
     * Connects to CockroachDB once and runs a query on the catalog of the configured database. The connection
     * is not tried again as it is for a running task, and the statements of {@code database.initial.statements}
     * are not run: a validation answers at once and changes nothing in the database.
     *
     * @return the message of the failure, or {@code null} if the connection works
     */
    private String testConnection(CockroachDBConnectorConfig connectorConfig, Duration timeout) {
        final Connection connection;
        try {
            connection = CockroachDBConnection.connectOnce(connectorConfig, timeout);
        }
        catch (SQLException e) {
            return failedConnection(connectorConfig, e);
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute(CONNECTION_TEST_QUERY);
            LOGGER.info("Successfully tested connection for {}:{}/{} with user '{}'", connectorConfig.getHostname(),
                    connectorConfig.getPort(), connectorConfig.getDatabaseName(), connectorConfig.getUser());
            return null;
        }
        catch (SQLException e) {
            return failedConnection(connectorConfig, e);
        }
        finally {
            try {
                connection.close();
            }
            catch (SQLException e) {
                LOGGER.error("Unexpected error shutting down the database connection", e);
            }
        }
    }

    private String failedConnection(CockroachDBConnectorConfig connectorConfig, SQLException e) {
        LOGGER.error("Failed testing connection for {}:{}/{} with user '{}'", connectorConfig.getHostname(),
                connectorConfig.getPort(), connectorConfig.getDatabaseName(), connectorConfig.getUser(), e);
        return e.getMessage() != null ? e.getMessage() : e.toString();
    }
}
