/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import io.debezium.config.Configuration;

/**
 * Basic tests for the CockroachDB connector.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTest {

    @Test
    public void shouldHaveValidConfiguration() {
        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        assertThat(configDef).isNotNull();
        assertThat(configDef.names()).contains("database.hostname");
        assertThat(configDef.names()).contains("database.port");
        assertThat(configDef.names()).contains("database.user");
        assertThat(configDef.names()).contains("database.password");
        assertThat(configDef.names()).contains("database.dbname");
    }

    @Test
    public void shouldCreateValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        assertThat(connectorConfig.getDatabaseName()).isEqualTo("defaultdb");
        assertThat(connectorConfig.getLogicalName()).isEqualTo("test");
    }

    @Test
    public void shouldHaveCorrectVersion() {
        CockroachDBConnector connector = new CockroachDBConnector();
        String version = connector.version();

        assertThat(version).isNotNull();
        assertThat(version).isNotEmpty();
    }

    @Test
    public void shouldHaveCorrectTaskClass() {
        CockroachDBConnector connector = new CockroachDBConnector();

        assertThat(connector.taskClass()).isEqualTo(CockroachDBConnectorTask.class);
    }

    @Test
    public void shouldGenerateTaskConfigs() {
        CockroachDBConnector connector = new CockroachDBConnector();

        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");

        connector.start(props);

        var taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).containsKey("database.hostname");
        assertThat(taskConfigs.get(0).get("database.hostname")).isEqualTo("localhost");
    }
}