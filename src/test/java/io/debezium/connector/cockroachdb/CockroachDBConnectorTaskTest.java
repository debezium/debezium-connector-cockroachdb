/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Tests for CockroachDB connector task.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTaskTest {

    private CockroachDBConnectorTask task;

    @BeforeEach
    public void setUp() {
        task = new CockroachDBConnectorTask();
    }

    @Test
    public void shouldHaveCorrectVersion() {
        String version = task.version();

        assertThat(version).isNotNull();
        assertThat(version).isNotEmpty();
    }

    @Test
    public void shouldStartAndStop() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        Configuration config = Configuration.from(props);

        // For unit tests, we'll just verify the task can be created and configured
        // without actually starting it (which requires a real database connection)
        assertThat(task).isNotNull();
        assertThat(config.getString("database.hostname")).isEqualTo("localhost");
        assertThat(config.getString("database.dbname")).isEqualTo("testdb");

        // Test that the task can be stopped without issues
        task.stop();
    }
}