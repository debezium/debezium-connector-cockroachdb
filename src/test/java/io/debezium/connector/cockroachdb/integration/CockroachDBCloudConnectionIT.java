/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.CockroachDBConnectorConfig;
import io.debezium.connector.cockroachdb.CockroachDBSchema;
import io.debezium.connector.cockroachdb.CockroachDBTaskContext;
import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Integration test for CockroachDB Cloud connectivity with SSL.
 *
 * <p>Requires the {@code CRDB_CLOUD_URL} environment variable to be set to a
 * CockroachDB Cloud connection URI (e.g.
 * {@code postgresql://user:pass@host:26257/defaultdb?sslmode=verify-full}).</p>
 *
 * @author Virag Tripathi
 */
@EnabledIfEnvironmentVariable(named = "CRDB_CLOUD_URL", matches = ".+")
public class CockroachDBCloudConnectionIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBCloudConnectionIT.class);

    private Connection connection;
    private String host;
    private int port;
    private String user;
    private String password;
    private String database;
    private String sslMode;

    @BeforeEach
    public void setUp() throws Exception {
        URI uri = new URI(System.getenv("CRDB_CLOUD_URL"));
        String userInfo = uri.getUserInfo();
        user = userInfo.contains(":") ? userInfo.split(":")[0] : userInfo;
        password = userInfo.contains(":") ? userInfo.split(":")[1] : "";
        host = uri.getHost();
        port = uri.getPort();
        database = uri.getPath().substring(1);
        sslMode = "prefer";
        String query = uri.getQuery();
        if (query != null) {
            for (String param : query.split("&")) {
                if (param.startsWith("sslmode=")) {
                    sslMode = param.substring("sslmode=".length());
                }
            }
        }

        String jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + database + "?sslmode=" + sslMode;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        LOGGER.info("Connecting to CockroachDB Cloud at {}:{}/{} with sslmode={}", host, port, database, sslMode);
        connection = DriverManager.getConnection(jdbcUrl, props);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS cloud_test_table");
            }
            catch (Exception e) {
                LOGGER.warn("Cleanup failed: {}", e.getMessage());
            }
            connection.close();
        }
    }

    @Test
    public void shouldConnectWithSSLVerifyFull() throws Exception {
        assertThat(connection.isValid(5)).isTrue();

        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT version()");
            assertThat(rs.next()).isTrue();
            String version = rs.getString(1);
            LOGGER.info("Connected to CockroachDB Cloud: {}", version);
            assertThat(version).containsIgnoringCase("cockroach");
        }
    }

    @Test
    public void shouldQueryClusterSettings() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SHOW CLUSTER SETTING kv.rangefeed.enabled");
            assertThat(rs.next()).isTrue();
            String value = rs.getString(1);
            LOGGER.info("kv.rangefeed.enabled = {}", value);
            assertThat(value).isIn("true", "false", "t", "f");
        }
    }

    @Test
    public void shouldCreateTableAndQueryInformationSchema() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS cloud_test_table ("
                    + "id INT PRIMARY KEY, "
                    + "name STRING NOT NULL, "
                    + "email STRING, "
                    + "amount DECIMAL(10,2), "
                    + "created_at TIMESTAMPTZ DEFAULT now()"
                    + ")");

            ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, data_type, is_nullable "
                            + "FROM information_schema.columns "
                            + "WHERE table_name = 'cloud_test_table' "
                            + "ORDER BY ordinal_position");

            int columnCount = 0;
            while (rs.next()) {
                columnCount++;
                LOGGER.info("Column: {} type={} nullable={}",
                        rs.getString("column_name"),
                        rs.getString("data_type"),
                        rs.getString("is_nullable"));
            }
            assertThat(columnCount).isEqualTo(5);
        }
    }

    @Test
    public void shouldDiscoverPrimaryKeys() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS cloud_test_table ("
                    + "id INT PRIMARY KEY, "
                    + "name STRING NOT NULL"
                    + ")");

            ResultSet rs = stmt.executeQuery(
                    "SELECT kcu.column_name "
                            + "FROM information_schema.key_column_usage kcu "
                            + "JOIN information_schema.table_constraints tc "
                            + "  ON kcu.constraint_name = tc.constraint_name "
                            + "  AND kcu.table_schema = tc.table_schema "
                            + "  AND kcu.table_name = tc.table_name "
                            + "WHERE kcu.table_name = 'cloud_test_table' "
                            + "AND tc.constraint_type = 'PRIMARY KEY' "
                            + "ORDER BY kcu.ordinal_position");

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("column_name")).isEqualTo("id");
            LOGGER.info("Primary key discovered: id");
        }
    }

    @Test
    public void shouldConnectViaCockroachDBConnection() throws Exception {
        Configuration config = buildConnectorConfiguration();
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        try (CockroachDBConnection crdbConnection = new CockroachDBConnection(connectorConfig)) {
            crdbConnection.connect();
            assertThat(crdbConnection.isValid()).isTrue();
            LOGGER.info("CockroachDBConnection connected successfully with SSL mode: {}", sslMode);

            try (Statement stmt = crdbConnection.connection().createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT current_database()");
                assertThat(rs.next()).isTrue();
                LOGGER.info("Current database: {}", rs.getString(1));
            }
        }
    }

    @Test
    public void shouldInitializeSchemaFromCloud() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS cloud_test_table ("
                    + "id INT PRIMARY KEY, "
                    + "name STRING NOT NULL, "
                    + "email STRING"
                    + ")");
        }

        Configuration config = buildConnectorConfiguration();
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        CockroachDBTaskContext taskContext = new CockroachDBTaskContext(config, connectorConfig);

        @SuppressWarnings("unchecked")
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(
                CommonConnectorConfig.TOPIC_NAMING_STRATEGY);

        CockroachDBSchema schema = new CockroachDBSchema(taskContext, topicNamingStrategy);
        schema.initialize(connectorConfig);

        assertThat(schema.getDiscoveredTables()).isNotEmpty();

        boolean foundTestTable = schema.getDiscoveredTables().stream()
                .anyMatch(t -> "cloud_test_table".equals(t.table()));
        assertThat(foundTestTable).isTrue();

        LOGGER.info("Schema discovered {} tables from CockroachDB Cloud", schema.getDiscoveredTables().size());
        for (TableId tableId : schema.getDiscoveredTables()) {
            LOGGER.info("  Table: {} (columns={})", tableId,
                    schema.tableFor(tableId) != null ? schema.tableFor(tableId).columns().size() : "unknown");
        }

        schema.close();
    }

    private Configuration buildConnectorConfiguration() {
        return Configuration.create()
                .with("database.hostname", host)
                .with("database.port", port)
                .with("database.user", user)
                .with("database.password", password)
                .with("database.dbname", database)
                .with("database.sslmode", sslMode)
                .with("database.server.name", "cloud-test")
                .with("topic.prefix", "cloud-test")
                .with("cockroachdb.skip.permission.check", true)
                .with("cockroachdb.schema.name", "public")
                .build();
    }
}
