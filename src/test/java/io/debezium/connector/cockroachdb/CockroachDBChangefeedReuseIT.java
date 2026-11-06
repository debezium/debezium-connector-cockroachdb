/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Verifies against CockroachDB that a restart does not create a second changefeed when the check for
 * an existing one fails (debezium/dbz#3037). It runs the package-private changefeed creation path
 * directly, so it lives in this package rather than in the integration package.
 */
@Testcontainers
public class CockroachDBChangefeedReuseIT {

    private static final String COCKROACHDB_VERSION = System.getProperty("cockroachdb.version", "v25.4.18");
    private static final String TOPIC_PREFIX = "reuse";
    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");

    @Container
    private static final CockroachContainer cockroachdb = new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:" + COCKROACHDB_VERSION))
            .withNetwork(NETWORK)
            .withNetworkAliases("cockroachdb");

    @Test
    public void shouldReuseTheRunningChangefeedWhenTheSessionEndsBeforeTheCheck() throws Exception {
        try (Connection admin = DriverManager.getConnection(
                cockroachdb.getJdbcUrl().replace("/postgres", "/defaultdb"), cockroachdb.getUsername(), cockroachdb.getPassword());
                Statement stmt = admin.createStatement()) {
            stmt.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
            stmt.execute("CREATE DATABASE IF NOT EXISTS reusedb");
            stmt.execute("CREATE TABLE IF NOT EXISTS reusedb.public.orders (id INT PRIMARY KEY, name STRING)");
            stmt.execute("UPSERT INTO reusedb.public.orders VALUES (1, 'first')");

            CockroachDBConnectorConfig config = kafkaConfig();
            List<TableId> tables = List.of(new TableId("reusedb", "public", "orders"));

            // The first start creates the changefeed
            try (CockroachDBConnection first = new CockroachDBConnection(config)) {
                first.connect();
                newSource(config).createChangefeeds(first, tables, new CockroachDBOffsetContext(config), false);
            }
            assertThat(runningJobs(stmt)).as("running changefeeds after the first start").hasSize(1);

            // A restart whose session ends before the check, the way it does when a node restarts or drains
            LogInterceptor logs = new LogInterceptor(CockroachDBStreamingChangeEventSource.class);
            try (CockroachDBConnection restarted = new CockroachDBConnection(config)) {
                restarted.connect();
                assertThat(cancelOtherSessions(stmt)).as("connector sessions ended").isPositive();
                newSource(config).createChangefeeds(restarted, tables, new CockroachDBOffsetContext(config), true);
            }

            assertThat(runningJobs(stmt)).as("the restart reuses the running changefeed instead of creating another").hasSize(1);
            assertThat(logs.containsWarnMessage("reconnecting and retrying")).as("the failed check was retried").isTrue();
            assertThat(logs.containsMessage("Changefeed already running for table(s)")).isTrue();
        }
    }

    private static CockroachDBConnectorConfig kafkaConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("database.hostname", cockroachdb.getHost());
        config.put("database.port", String.valueOf(cockroachdb.getMappedPort(26257)));
        config.put("database.user", cockroachdb.getUsername());
        config.put("database.password", cockroachdb.getPassword());
        config.put("database.dbname", "reusedb");
        config.put("database.sslmode", "disable");
        config.put("topic.prefix", TOPIC_PREFIX);
        config.put("table.include.list", "public.orders");
        config.put("cockroachdb.changefeed.sink.type", "kafka");
        config.put("cockroachdb.changefeed.sink.uri", "kafka://kafka:19092");
        return new CockroachDBConnectorConfig(Configuration.from(config));
    }

    private static CockroachDBStreamingChangeEventSource newSource(CockroachDBConnectorConfig config) {
        return new CockroachDBStreamingChangeEventSource(config, null, null, Clock.system(), null);
    }

    private static List<String> runningJobs(Statement stmt) throws Exception {
        List<String> jobs = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery("SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' "
                + "AND description LIKE '%topic_prefix=" + TOPIC_PREFIX + "%'")) {
            while (rs.next()) {
                jobs.add(rs.getString(1));
            }
        }
        return jobs;
    }

    /**
     * Ends every client session except the caller's, the way a restart of the gateway node does.
     */
    private static int cancelOtherSessions(Statement stmt) throws Exception {
        return stmt.executeUpdate("CANCEL SESSIONS (SELECT session_id FROM [SHOW CLUSTER SESSIONS] "
                + "WHERE session_id != (SELECT session_id FROM [SHOW session_id]) AND user_name = 'root' "
                + "AND application_name NOT LIKE '$ internal%')");
    }
}
